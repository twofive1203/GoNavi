package db

import (
	"testing"

	"GoNavi-Wails/internal/connection"
)

func TestMySQLDSN_UseSSH_ShouldFailWhenSSHInvalid(t *testing.T) {
	m := &MySQLDB{}
	_, err := m.getDSN(connection.ConnectionConfig{
		Host:   "127.0.0.1",
		Port:   3306,
		User:   "root",
		UseSSH: true,
		SSH: connection.SSHConfig{
			Host:     "127.0.0.1",
			Port:     0, // invalid port, should fail immediately
			User:     "bad",
			Password: "bad",
		},
	})
	if err == nil {
		t.Fatalf("expected error when UseSSH=true and SSH config invalid")
	}
}
