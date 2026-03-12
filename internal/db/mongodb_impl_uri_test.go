//go:build gonavi_full_drivers || gonavi_mongodb_driver

package db

import (
	"testing"

	"GoNavi-Wails/internal/connection"
)

func TestApplyMongoURI_ExplicitHostDoesNotAdoptURIHosts(t *testing.T) {
	config := connection.ConnectionConfig{
		Host: "10.10.10.10",
		Port: 27017,
		URI:  "mongodb://localhost:27017/admin",
	}

	got := applyMongoURI(config)
	if got.Host != "10.10.10.10" {
		t.Fatalf("expected host to remain explicit, got %q", got.Host)
	}
	if len(got.Hosts) != 0 {
		t.Fatalf("expected hosts to remain empty when explicit host exists, got %v", got.Hosts)
	}
}

func TestApplyMongoURI_ExplicitHostsDoesNotAdoptURIHosts(t *testing.T) {
	config := connection.ConnectionConfig{
		Host:  "10.10.10.10",
		Port:  27017,
		Hosts: []string{"10.10.10.10:27017", "10.10.10.11:27017"},
		URI:   "mongodb://localhost:27017,localhost:27018/admin?replicaSet=rs0",
	}

	got := applyMongoURI(config)
	if len(got.Hosts) != 2 || got.Hosts[0] != "10.10.10.10:27017" {
		t.Fatalf("expected explicit hosts to stay untouched, got %v", got.Hosts)
	}
}
