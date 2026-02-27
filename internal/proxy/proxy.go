package proxy

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"GoNavi-Wails/internal/connection"
	"GoNavi-Wails/internal/logger"

	xproxy "golang.org/x/net/proxy"
)

const (
	defaultDialTimeout = 8 * time.Second
)

type LocalForwarder struct {
	LocalAddr  string
	RemoteAddr string
	ProxyAddr  string
	ProxyType  string

	cfg       connection.ProxyConfig
	listener  net.Listener
	closeChan chan struct{}
	closeOnce sync.Once

	closed   bool
	closedMu sync.RWMutex
}

var (
	forwarderMu     sync.RWMutex
	localForwarders = make(map[string]*LocalForwarder)
)

func NormalizeConfig(config connection.ProxyConfig) (connection.ProxyConfig, error) {
	result := connection.ProxyConfig{
		Type:     strings.ToLower(strings.TrimSpace(config.Type)),
		Host:     strings.TrimSpace(config.Host),
		Port:     config.Port,
		User:     strings.TrimSpace(config.User),
		Password: config.Password,
	}

	switch result.Type {
	case "socks5", "socks5h", "http":
	default:
		return result, fmt.Errorf("不支持的代理类型：%s", config.Type)
	}
	if result.Type == "socks5h" {
		result.Type = "socks5"
	}
	if result.Host == "" {
		return result, fmt.Errorf("代理主机为空")
	}
	if result.Port <= 0 || result.Port > 65535 {
		return result, fmt.Errorf("代理端口无效：%d", result.Port)
	}
	return result, nil
}

func GetOrCreateLocalForwarder(proxyConfig connection.ProxyConfig, remoteHost string, remotePort int) (*LocalForwarder, error) {
	cfg, err := NormalizeConfig(proxyConfig)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(remoteHost) == "" || remotePort <= 0 {
		return nil, fmt.Errorf("无效的远端地址：%s:%d", remoteHost, remotePort)
	}

	key := forwarderCacheKey(cfg, remoteHost, remotePort)
	forwarderMu.RLock()
	forwarder, exists := localForwarders[key]
	forwarderMu.RUnlock()
	if exists && forwarder != nil && !forwarder.IsClosed() {
		return forwarder, nil
	}

	if exists {
		forwarderMu.Lock()
		delete(localForwarders, key)
		forwarderMu.Unlock()
	}

	next, err := NewLocalForwarder(cfg, remoteHost, remotePort)
	if err != nil {
		return nil, err
	}

	forwarderMu.Lock()
	localForwarders[key] = next
	forwarderMu.Unlock()
	return next, nil
}

func forwarderCacheKey(cfg connection.ProxyConfig, remoteHost string, remotePort int) string {
	trimmedHost := strings.TrimSpace(remoteHost)
	credential := cfg.User + "\x00" + cfg.Password
	credentialHash := sha256.Sum256([]byte(credential))
	// 仅保留短指纹用于区分不同认证信息，避免在 key 日志中泄露明文口令。
	fingerprint := hex.EncodeToString(credentialHash[:8])
	return fmt.Sprintf("%s://%s:%d@%s:%d#%s", cfg.Type, cfg.Host, cfg.Port, trimmedHost, remotePort, fingerprint)
}

func NewLocalForwarder(proxyConfig connection.ProxyConfig, remoteHost string, remotePort int) (*LocalForwarder, error) {
	cfg, err := NormalizeConfig(proxyConfig)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("创建本地代理监听失败：%w", err)
	}

	localAddr := listener.Addr().String()
	remoteAddr := net.JoinHostPort(strings.TrimSpace(remoteHost), fmt.Sprintf("%d", remotePort))
	proxyAddr := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))
	forwarder := &LocalForwarder{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
		ProxyAddr:  proxyAddr,
		ProxyType:  cfg.Type,
		cfg:        cfg,
		listener:   listener,
		closeChan:  make(chan struct{}),
	}

	go forwarder.forward()
	logger.Infof("已创建代理端口转发：本地 %s -> 远端 %s（代理 %s://%s）", localAddr, remoteAddr, cfg.Type, proxyAddr)
	return forwarder, nil
}

func (f *LocalForwarder) forward() {
	for {
		localConn, err := f.listener.Accept()
		if err != nil {
			select {
			case <-f.closeChan:
				return
			default:
				logger.Warnf("接受本地代理连接失败：%v", err)
				return
			}
		}
		go f.handleConnection(localConn)
	}
}

func (f *LocalForwarder) handleConnection(localConn net.Conn) {
	defer localConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	remoteConn, err := dialThroughProxy(ctx, f.cfg, "tcp", f.RemoteAddr)
	cancel()
	if err != nil {
		logger.Warnf("通过代理连接远端失败：远端=%s 代理=%s://%s 错误=%v", f.RemoteAddr, f.ProxyType, f.ProxyAddr, err)
		return
	}
	defer remoteConn.Close()

	errc := make(chan error, 2)
	var closeOnce sync.Once
	closeBoth := func() {
		_ = localConn.Close()
		_ = remoteConn.Close()
	}
	go func() {
		_, copyErr := io.Copy(remoteConn, localConn)
		closeOnce.Do(closeBoth)
		errc <- copyErr
	}()
	go func() {
		_, copyErr := io.Copy(localConn, remoteConn)
		closeOnce.Do(closeBoth)
		errc <- copyErr
	}()
	<-errc
	<-errc
}

func (f *LocalForwarder) Close() error {
	var err error
	f.closeOnce.Do(func() {
		f.closedMu.Lock()
		f.closed = true
		f.closedMu.Unlock()
		close(f.closeChan)
		err = f.listener.Close()
		if err != nil {
			logger.Warnf("关闭代理端口转发失败：%v", err)
		}
	})
	return err
}

func (f *LocalForwarder) IsClosed() bool {
	f.closedMu.RLock()
	defer f.closedMu.RUnlock()
	return f.closed
}

func CloseAllForwarders() {
	forwarderMu.Lock()
	defer forwarderMu.Unlock()

	for key, forwarder := range localForwarders {
		if forwarder == nil {
			continue
		}
		_ = forwarder.Close()
		logger.Infof("已关闭代理端口转发：%s", key)
	}
	localForwarders = make(map[string]*LocalForwarder)
}

func DialContext(ctx context.Context, proxyConfig connection.ProxyConfig, network, address string) (net.Conn, error) {
	cfg, err := NormalizeConfig(proxyConfig)
	if err != nil {
		return nil, err
	}
	return dialThroughProxy(ctx, cfg, network, address)
}

func dialThroughProxy(ctx context.Context, cfg connection.ProxyConfig, network, address string) (net.Conn, error) {
	switch cfg.Type {
	case "socks5":
		return dialSOCKS5(ctx, cfg, network, address)
	case "http":
		return dialHTTPConnect(ctx, cfg, address)
	default:
		return nil, fmt.Errorf("不支持的代理类型：%s", cfg.Type)
	}
}

func dialSOCKS5(ctx context.Context, cfg connection.ProxyConfig, network, address string) (net.Conn, error) {
	proxyAddr := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))
	var auth *xproxy.Auth
	if cfg.User != "" || cfg.Password != "" {
		auth = &xproxy.Auth{
			User:     cfg.User,
			Password: cfg.Password,
		}
	}
	dialer, err := xproxy.SOCKS5("tcp", proxyAddr, auth, &net.Dialer{Timeout: defaultDialTimeout})
	if err != nil {
		return nil, fmt.Errorf("创建 SOCKS5 代理拨号器失败：%w", err)
	}

	type result struct {
		conn net.Conn
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		conn, dialErr := dialer.Dial(network, address)
		ch <- result{conn: conn, err: dialErr}
	}()

	select {
	case <-ctx.Done():
		go func() {
			r := <-ch
			if r.conn != nil {
				_ = r.conn.Close()
			}
		}()
		return nil, ctx.Err()
	case r := <-ch:
		if r.err != nil {
			return nil, fmt.Errorf("SOCKS5 代理连接失败：%w", r.err)
		}
		return r.conn, nil
	}
}

func dialHTTPConnect(ctx context.Context, cfg connection.ProxyConfig, address string) (net.Conn, error) {
	proxyAddr := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))
	dialer := &net.Dialer{Timeout: defaultDialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("连接 HTTP 代理失败：%w", err)
	}

	connectReq := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: address},
		Host:   address,
		Header: make(http.Header),
	}
	if cfg.User != "" || cfg.Password != "" {
		raw := cfg.User + ":" + cfg.Password
		connectReq.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(raw)))
	}
	if err := connectReq.Write(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("发送 HTTP CONNECT 请求失败：%w", err)
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, connectReq)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("读取 HTTP CONNECT 响应失败：%w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_ = conn.Close()
		return nil, fmt.Errorf("HTTP 代理 CONNECT 失败：%s", strings.TrimSpace(resp.Status))
	}

	if reader.Buffered() == 0 {
		return conn, nil
	}
	return &bufferedConn{Conn: conn, reader: reader}, nil
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	if c.reader == nil {
		return c.Conn.Read(p)
	}
	if c.reader.Buffered() == 0 {
		c.reader = nil
		return c.Conn.Read(p)
	}
	return c.reader.Read(p)
}
