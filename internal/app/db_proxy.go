package app

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"GoNavi-Wails/internal/connection"
	proxytunnel "GoNavi-Wails/internal/proxy"
)

func resolveDialConfigWithProxy(raw connection.ConnectionConfig) (connection.ConnectionConfig, error) {
	config := raw
	if config.UseHTTPTunnel {
		if config.UseProxy {
			return connection.ConnectionConfig{}, fmt.Errorf("HTTP 隧道与普通代理不能同时启用")
		}
		tunnelHost := strings.TrimSpace(config.HTTPTunnel.Host)
		if tunnelHost == "" {
			return connection.ConnectionConfig{}, fmt.Errorf("HTTP 隧道主机不能为空")
		}
		tunnelPort := config.HTTPTunnel.Port
		if tunnelPort <= 0 {
			tunnelPort = 8080
		}
		if tunnelPort > 65535 {
			return connection.ConnectionConfig{}, fmt.Errorf("HTTP 隧道端口无效：%d", config.HTTPTunnel.Port)
		}

		config.UseProxy = true
		config.Proxy = connection.ProxyConfig{
			Type:     "http",
			Host:     tunnelHost,
			Port:     tunnelPort,
			User:     strings.TrimSpace(config.HTTPTunnel.User),
			Password: config.HTTPTunnel.Password,
		}
	}
	if !config.UseProxy {
		config.Proxy = connection.ProxyConfig{}
		config.UseHTTPTunnel = false
		config.HTTPTunnel = connection.HTTPTunnelConfig{}
		return config, nil
	}

	normalizedProxy, err := proxytunnel.NormalizeConfig(config.Proxy)
	if err != nil {
		return connection.ConnectionConfig{}, err
	}
	config.Proxy = normalizedProxy
	config.UseHTTPTunnel = false
	config.HTTPTunnel = connection.HTTPTunnelConfig{}

	if config.UseSSH {
		sshPort := config.SSH.Port
		if sshPort <= 0 {
			sshPort = 22
		}
		forwardedSSH, err := buildProxyForwardAddress(normalizedProxy, strings.TrimSpace(config.SSH.Host), sshPort)
		if err != nil {
			return connection.ConnectionConfig{}, fmt.Errorf("代理连接 SSH 网关失败：%w", err)
		}
		config.SSH.Host = forwardedSSH.host
		config.SSH.Port = forwardedSSH.port
		config.UseProxy = false
		config.Proxy = connection.ProxyConfig{}
		return config, nil
	}

	normalizedType := strings.ToLower(strings.TrimSpace(config.Type))
	if normalizedType == "sqlite" || normalizedType == "duckdb" || normalizedType == "custom" {
		// 文件型/自定义 DSN 类型不走标准 host:port，不在此层改写。
		return config, nil
	}
	if normalizedType == "mongodb" {
		// MongoDB 统一由驱动侧 Dialer 处理代理，保留原始目标地址，避免将连接目标改写为本地转发地址。
		return config, nil
	}

	targetPort := config.Port
	if targetPort <= 0 {
		targetPort = defaultPortByType(normalizedType)
	}
	forwardedPrimary, err := buildProxyForwardAddress(normalizedProxy, strings.TrimSpace(config.Host), targetPort)
	if err != nil {
		return connection.ConnectionConfig{}, err
	}
	config.Host = forwardedPrimary.host
	config.Port = forwardedPrimary.port

	if len(config.Hosts) > 0 {
		rewritten := make([]string, 0, len(config.Hosts))
		seen := make(map[string]struct{}, len(config.Hosts))
		for _, rawEntry := range config.Hosts {
			targetHost, targetPort, ok := parseAddressWithDefaultPort(rawEntry, defaultPortByType(normalizedType))
			if !ok {
				continue
			}
			forwarded, forwardErr := buildProxyForwardAddress(normalizedProxy, targetHost, targetPort)
			if forwardErr != nil {
				return connection.ConnectionConfig{}, forwardErr
			}
			rewrittenAddress := formatHostPort(forwarded.host, forwarded.port)
			if _, exists := seen[rewrittenAddress]; exists {
				continue
			}
			seen[rewrittenAddress] = struct{}{}
			rewritten = append(rewritten, rewrittenAddress)
		}
		config.Hosts = rewritten
	}

	config.UseProxy = false
	config.Proxy = connection.ProxyConfig{}
	return config, nil
}

type hostPort struct {
	host string
	port int
}

func buildProxyForwardAddress(proxyConfig connection.ProxyConfig, targetHost string, targetPort int) (hostPort, error) {
	host := strings.TrimSpace(targetHost)
	if host == "" {
		host = "localhost"
	}
	port := targetPort
	if port <= 0 {
		return hostPort{}, fmt.Errorf("目标端口无效：%d", targetPort)
	}

	forwarder, err := proxytunnel.GetOrCreateLocalForwarder(proxyConfig, host, port)
	if err != nil {
		return hostPort{}, err
	}
	localHost, localPort, splitOK := parseAddressWithDefaultPort(forwarder.LocalAddr, 0)
	if !splitOK || localPort <= 0 {
		return hostPort{}, fmt.Errorf("解析代理本地转发地址失败：%s", forwarder.LocalAddr)
	}
	return hostPort{host: localHost, port: localPort}, nil
}

func parseAddressWithDefaultPort(raw string, defaultPort int) (string, int, bool) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return "", 0, false
	}

	if strings.HasPrefix(text, "[") {
		if host, portText, err := net.SplitHostPort(text); err == nil {
			if port, convErr := strconv.Atoi(portText); convErr == nil && port > 0 && port <= 65535 {
				return strings.TrimSpace(host), port, true
			}
			return "", 0, false
		}
		trimmed := strings.Trim(strings.TrimPrefix(text, "["), "]")
		if trimmed != "" && defaultPort > 0 {
			return trimmed, defaultPort, true
		}
		return "", 0, false
	}

	if strings.Count(text, ":") == 0 {
		if defaultPort <= 0 {
			return "", 0, false
		}
		return text, defaultPort, true
	}

	if strings.Count(text, ":") == 1 {
		host, portText, err := net.SplitHostPort(text)
		if err == nil {
			port, convErr := strconv.Atoi(portText)
			if convErr == nil && port > 0 && port <= 65535 {
				return strings.TrimSpace(host), port, true
			}
			return "", 0, false
		}
		if defaultPort > 0 {
			return strings.TrimSpace(text), defaultPort, true
		}
		return "", 0, false
	}

	// IPv6 地址未带端口，使用默认端口。
	if defaultPort > 0 {
		return text, defaultPort, true
	}
	return "", 0, false
}

func formatHostPort(host string, port int) string {
	h := strings.TrimSpace(host)
	if strings.Contains(h, ":") && !strings.HasPrefix(h, "[") {
		return fmt.Sprintf("[%s]:%d", h, port)
	}
	return fmt.Sprintf("%s:%d", h, port)
}

func defaultPortByType(driverType string) int {
	switch strings.ToLower(strings.TrimSpace(driverType)) {
	case "mysql", "mariadb":
		return 3306
	case "diros":
		return 9030
	case "sphinx":
		return 9306
	case "postgres", "vastbase":
		return 5432
	case "redis":
		return 6379
	case "tdengine":
		return 6041
	case "oracle":
		return 1521
	case "dameng":
		return 5236
	case "kingbase":
		return 54321
	case "sqlserver":
		return 1433
	case "mongodb":
		return 27017
	case "clickhouse":
		return 9000
	case "highgo":
		return 5866
	default:
		return 0
	}
}
