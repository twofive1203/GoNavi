//go:build gonavi_mongodb_driver_v1

package db

import (
	"testing"

	"GoNavi-Wails/internal/connection"
)

func TestApplyMongoURIV1_ExplicitHostDoesNotAdoptURIHosts(t *testing.T) {
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
