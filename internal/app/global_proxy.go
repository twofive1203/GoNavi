package app

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"GoNavi-Wails/internal/connection"
	"GoNavi-Wails/internal/logger"
	proxytunnel "GoNavi-Wails/internal/proxy"
)

type globalProxySnapshot struct {
	Enabled bool                   `json:"enabled"`
	Proxy   connection.ProxyConfig `json:"proxy"`
}

var globalProxyRuntime = struct {
	mu      sync.RWMutex
	enabled bool
	proxy   connection.ProxyConfig
}{}

type localProxyTLSFallbackTransport struct {
	primary       *http.Transport
	fallback      *http.Transport
	proxyEndpoint string
}

func currentGlobalProxyConfig() globalProxySnapshot {
	globalProxyRuntime.mu.RLock()
	defer globalProxyRuntime.mu.RUnlock()
	if !globalProxyRuntime.enabled {
		return globalProxySnapshot{
			Enabled: false,
			Proxy:   connection.ProxyConfig{},
		}
	}
	return globalProxySnapshot{
		Enabled: true,
		Proxy:   globalProxyRuntime.proxy,
	}
}

func setGlobalProxyConfig(enabled bool, proxyConfig connection.ProxyConfig) (globalProxySnapshot, error) {
	if !enabled {
		globalProxyRuntime.mu.Lock()
		globalProxyRuntime.enabled = false
		globalProxyRuntime.proxy = connection.ProxyConfig{}
		globalProxyRuntime.mu.Unlock()
		return currentGlobalProxyConfig(), nil
	}

	normalizedProxy, err := proxytunnel.NormalizeConfig(proxyConfig)
	if err != nil {
		return globalProxySnapshot{}, err
	}

	globalProxyRuntime.mu.Lock()
	globalProxyRuntime.enabled = true
	globalProxyRuntime.proxy = normalizedProxy
	globalProxyRuntime.mu.Unlock()
	return currentGlobalProxyConfig(), nil
}

func (a *App) ConfigureGlobalProxy(enabled bool, proxyConfig connection.ProxyConfig) connection.QueryResult {
	before := currentGlobalProxyConfig()
	snapshot, err := setGlobalProxyConfig(enabled, proxyConfig)
	if err != nil {
		return connection.QueryResult{Success: false, Message: err.Error()}
	}

	// 前端可能在同一配置下重复触发同步（例如严格模式或状态回放），
	// 这里做幂等日志，避免重复刷屏。
	if !globalProxySnapshotEqual(before, snapshot) {
		if snapshot.Enabled {
			authState := ""
			if strings.TrimSpace(snapshot.Proxy.User) != "" {
				authState = "（认证：已配置）"
			}
			logger.Infof(
				"全局代理已启用：%s://%s:%d%s",
				strings.ToLower(strings.TrimSpace(snapshot.Proxy.Type)),
				strings.TrimSpace(snapshot.Proxy.Host),
				snapshot.Proxy.Port,
				authState,
			)
		} else {
			logger.Infof("全局代理已关闭")
		}
	}

	return connection.QueryResult{
		Success: true,
		Message: "全局代理配置已生效",
		Data:    snapshot,
	}
}

func globalProxySnapshotEqual(a, b globalProxySnapshot) bool {
	if a.Enabled != b.Enabled {
		return false
	}
	if !a.Enabled {
		return true
	}
	return proxyConfigEqual(a.Proxy, b.Proxy)
}

func proxyConfigEqual(a, b connection.ProxyConfig) bool {
	return strings.EqualFold(strings.TrimSpace(a.Type), strings.TrimSpace(b.Type)) &&
		strings.TrimSpace(a.Host) == strings.TrimSpace(b.Host) &&
		a.Port == b.Port &&
		strings.TrimSpace(a.User) == strings.TrimSpace(b.User) &&
		a.Password == b.Password
}

func (a *App) GetGlobalProxyConfig() connection.QueryResult {
	return connection.QueryResult{
		Success: true,
		Message: "OK",
		Data:    currentGlobalProxyConfig(),
	}
}

func applyGlobalProxyToConnection(config connection.ConnectionConfig) connection.ConnectionConfig {
	effective := config
	if effective.UseProxy || effective.UseHTTPTunnel {
		return effective
	}
	if isFileDatabaseType(effective.Type) {
		effective.Proxy = connection.ProxyConfig{}
		return effective
	}

	snapshot := currentGlobalProxyConfig()
	if !snapshot.Enabled {
		effective.Proxy = connection.ProxyConfig{}
		return effective
	}

	effective.UseProxy = true
	effective.Proxy = snapshot.Proxy
	return effective
}

func isFileDatabaseType(driverType string) bool {
	switch strings.ToLower(strings.TrimSpace(driverType)) {
	case "sqlite", "duckdb":
		return true
	default:
		return false
	}
}

func newHTTPClientWithGlobalProxy(timeout time.Duration) *http.Client {
	client := &http.Client{
		Timeout: timeout,
	}
	if transport := buildHTTPTransportWithGlobalProxy(); transport != nil {
		client.Transport = transport
	}
	return client
}

func buildHTTPTransportWithGlobalProxy() http.RoundTripper {
	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok || baseTransport == nil {
		return nil
	}

	transport := baseTransport.Clone()
	snapshot := currentGlobalProxyConfig()
	if !snapshot.Enabled {
		transport.Proxy = http.ProxyFromEnvironment
		return transport
	}

	proxyURL, err := buildProxyURLFromConfig(snapshot.Proxy)
	if err != nil {
		logger.Warnf("全局代理配置无效，回退系统代理：%v", err)
		transport.Proxy = http.ProxyFromEnvironment
		return transport
	}

	transport.Proxy = http.ProxyURL(proxyURL)
	if !isLoopbackProxyHost(snapshot.Proxy.Host) {
		return transport
	}

	fallbackTransport := transport.Clone()
	fallbackTransport.TLSClientConfig = cloneTLSConfigWithInsecureSkipVerify(fallbackTransport.TLSClientConfig)
	return &localProxyTLSFallbackTransport{
		primary:       transport,
		fallback:      fallbackTransport,
		proxyEndpoint: proxyURL.Redacted(),
	}
}

func (t *localProxyTLSFallbackTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.primary.RoundTrip(req)
	if err == nil {
		return resp, nil
	}
	if !isTLSFallbackCandidate(req.Method, err) {
		return nil, err
	}

	retryReq, cloneErr := cloneRequestForRetry(req)
	if cloneErr != nil {
		return nil, err
	}
	logger.Warnf("检测到本地代理 TLS 证书不受信任，启用兼容回退：代理=%s 目标=%s 错误=%v", t.proxyEndpoint, req.URL.String(), err)
	return t.fallback.RoundTrip(retryReq)
}

func isTLSFallbackCandidate(method string, err error) bool {
	if !isIdempotentRequestMethod(method) {
		return false
	}
	return isUnknownAuthorityError(err)
}

func isIdempotentRequestMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodGet, http.MethodHead:
		return true
	default:
		return false
	}
}

func cloneRequestForRetry(req *http.Request) (*http.Request, error) {
	cloned := req.Clone(req.Context())
	if req.Body == nil || req.Body == http.NoBody {
		return cloned, nil
	}
	if req.GetBody == nil {
		return nil, fmt.Errorf("request body not replayable")
	}
	body, err := req.GetBody()
	if err != nil {
		return nil, err
	}
	cloned.Body = body
	return cloned, nil
}

func isUnknownAuthorityError(err error) bool {
	var unknownErr x509.UnknownAuthorityError
	if errors.As(err, &unknownErr) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "x509: certificate signed by unknown authority")
}

func cloneTLSConfigWithInsecureSkipVerify(base *tls.Config) *tls.Config {
	if base == nil {
		return &tls.Config{InsecureSkipVerify: true}
	}
	cloned := base.Clone()
	cloned.InsecureSkipVerify = true
	return cloned
}

func isLoopbackProxyHost(host string) bool {
	trimmed := strings.TrimSpace(host)
	if trimmed == "" {
		return false
	}
	if strings.EqualFold(trimmed, "localhost") {
		return true
	}
	ip := net.ParseIP(trimmed)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}

func buildProxyURLFromConfig(proxyConfig connection.ProxyConfig) (*url.URL, error) {
	normalizedProxy, err := proxytunnel.NormalizeConfig(proxyConfig)
	if err != nil {
		return nil, err
	}

	proxyType := strings.ToLower(strings.TrimSpace(normalizedProxy.Type))
	if proxyType != "http" && proxyType != "socks5" {
		return nil, fmt.Errorf("不支持的代理类型：%s", normalizedProxy.Type)
	}
	if strings.TrimSpace(normalizedProxy.Host) == "" {
		return nil, fmt.Errorf("代理地址不能为空")
	}
	if normalizedProxy.Port <= 0 || normalizedProxy.Port > 65535 {
		return nil, fmt.Errorf("代理端口无效：%d", normalizedProxy.Port)
	}

	proxyURL := &url.URL{
		Scheme: proxyType,
		Host:   net.JoinHostPort(strings.TrimSpace(normalizedProxy.Host), strconv.Itoa(normalizedProxy.Port)),
	}
	if strings.TrimSpace(normalizedProxy.User) != "" {
		proxyURL.User = url.UserPassword(strings.TrimSpace(normalizedProxy.User), normalizedProxy.Password)
	}
	return proxyURL, nil
}
