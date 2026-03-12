package app

import (
	"reflect"
	"testing"

	"GoNavi-Wails/internal/connection"
)

func TestResolveDialConfigWithProxy_MongoKeepsTargetAddress(t *testing.T) {
	hosts := []string{"10.20.30.40:27017", "10.20.30.41:27017"}
	raw := connection.ConnectionConfig{
		Type:     "mongodb",
		Host:     "10.20.30.40",
		Port:     27017,
		UseProxy: true,
		Proxy: connection.ProxyConfig{
			Type: "socks5",
			Host: "127.0.0.1",
			Port: 1080,
		},
		Hosts: hosts,
	}

	got, err := resolveDialConfigWithProxy(raw)
	if err != nil {
		t.Fatalf("resolveDialConfigWithProxy returned error: %v", err)
	}
	if got.Host != raw.Host || got.Port != raw.Port {
		t.Fatalf("mongo target address should be kept, got=%s:%d want=%s:%d", got.Host, got.Port, raw.Host, raw.Port)
	}
	if !got.UseProxy {
		t.Fatalf("mongo should keep UseProxy=true for driver-level dialer")
	}
	if !reflect.DeepEqual(got.Hosts, hosts) {
		t.Fatalf("mongo hosts should be kept, got=%v want=%v", got.Hosts, hosts)
	}
}

func TestResolveDialConfigWithProxy_MongoSRVKeepsTargetAddress(t *testing.T) {
	raw := connection.ConnectionConfig{
		Type:     "mongodb",
		Host:     "cluster0.example.com",
		Port:     27017,
		MongoSRV: true,
		UseProxy: true,
		Proxy: connection.ProxyConfig{
			Type: "http",
			Host: "127.0.0.1",
			Port: 7890,
		},
	}

	got, err := resolveDialConfigWithProxy(raw)
	if err != nil {
		t.Fatalf("resolveDialConfigWithProxy returned error: %v", err)
	}
	if got.Host != raw.Host || got.Port != raw.Port {
		t.Fatalf("mongo SRV target address should be kept, got=%s:%d want=%s:%d", got.Host, got.Port, raw.Host, raw.Port)
	}
	if !got.UseProxy {
		t.Fatalf("mongo SRV should keep UseProxy=true for driver-level dialer")
	}
}
