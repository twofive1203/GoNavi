//go:build gonavi_full_drivers || gonavi_kingbase_driver

package db

import "testing"

func TestNormalizeKingbaseIdentifier(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain", in: "ldf_server", want: "ldf_server"},
		{name: "quoted", in: `"ldf_server"`, want: "ldf_server"},
		{name: "double quoted", in: `""ldf_server""`, want: "ldf_server"},
		{name: "quad quoted", in: `""""ldf_server""""`, want: "ldf_server"},
		{name: "escaped quoted", in: `\"ldf_server\"`, want: "ldf_server"},
		{name: "backtick quoted", in: "`ldf_server`", want: "ldf_server"},
		{name: "bracket quoted", in: "[ldf_server]", want: "ldf_server"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeKingbaseIdentifier(tt.in); got != tt.want {
				t.Fatalf("normalizeKingbaseIdentifier(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestQuoteKingbaseIdent(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain", in: "ldf_server", want: `"ldf_server"`},
		{name: "double quoted", in: `""ldf_server""`, want: `"ldf_server"`},
		{name: "escaped quoted", in: `\"ldf_server\"`, want: `"ldf_server"`},
		{name: "with embedded quote", in: `ab"cd`, want: `"ab""cd"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quoteKingbaseIdent(tt.in); got != tt.want {
				t.Fatalf("quoteKingbaseIdent(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestSplitKingbaseQualifiedTable(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		wantSchema string
		wantTable  string
	}{
		{name: "plain qualified", in: "ldf_server.t_user", wantSchema: "ldf_server", wantTable: "t_user"},
		{name: "double quoted qualified", in: `""ldf_server"".""t_user""`, wantSchema: "ldf_server", wantTable: "t_user"},
		{name: "escaped qualified", in: `\"ldf_server\".\"t_user\"`, wantSchema: "ldf_server", wantTable: "t_user"},
		{name: "bracket qualified", in: "[ldf_server].[t_user]", wantSchema: "ldf_server", wantTable: "t_user"},
		{name: "table only", in: `""t_user""`, wantSchema: "", wantTable: "t_user"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSchema, gotTable := splitKingbaseQualifiedTable(tt.in)
			if gotSchema != tt.wantSchema || gotTable != tt.wantTable {
				t.Fatalf("splitKingbaseQualifiedTable(%q) = (%q, %q), want (%q, %q)", tt.in, gotSchema, gotTable, tt.wantSchema, tt.wantTable)
			}
		})
	}
}
