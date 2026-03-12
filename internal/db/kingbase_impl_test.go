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
		{name: "double escaped quoted", in: `\\\"ldf_server\\\"`, want: "ldf_server"},
		{name: "backtick quoted", in: "`ldf_server`", want: "ldf_server"},
		{name: "bracket quoted", in: "[ldf_server]", want: "ldf_server"},
		{name: "embedded double quotes", in: `ldf""server`, want: "ldfserver"},
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
		// 纯小写+下划线：不加引号
		{name: "plain lowercase", in: "ldf_server", want: "ldf_server"},
		{name: "plain lowercase 2", in: "bcs_barcode", want: "bcs_barcode"},
		{name: "double quoted input", in: `""ldf_server""`, want: "ldf_server"},
		{name: "escaped quoted input", in: `\"ldf_server\"`, want: "ldf_server"},
		// 含大写字母：加引号
		{name: "uppercase", in: "LDF_Server", want: `"LDF_Server"`},
		{name: "mixed case", in: "myTable", want: `"myTable"`},
		// SQL 保留字：加引号
		{name: "reserved word order", in: "order", want: `"order"`},
		{name: "reserved word user", in: "user", want: `"user"`},
		{name: "reserved word table", in: "table", want: `"table"`},
		{name: "reserved word select", in: "select", want: `"select"`},
		// 含特殊字符：加引号
		{name: "with hyphen", in: "my-table", want: `"my-table"`},
		{name: "with space", in: "my table", want: `"my table"`},
		{name: "with embedded quote", in: `ab"cd`, want: `"ab""cd"`},
		// 空值
		{name: "empty", in: "", want: `""`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quoteKingbaseIdent(tt.in); got != tt.want {
				t.Fatalf("quoteKingbaseIdent(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestKingbaseIdentNeedsQuote(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "plain lowercase", in: "ldf_server", want: false},
		{name: "starts with underscore", in: "_col", want: false},
		{name: "with digits", in: "col123", want: false},
		{name: "uppercase", in: "MyTable", want: true},
		{name: "reserved word", in: "order", want: true},
		{name: "with hyphen", in: "my-col", want: true},
		{name: "starts with digit", in: "123col", want: true},
		{name: "empty", in: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := kingbaseIdentNeedsQuote(tt.in); got != tt.want {
				t.Fatalf("kingbaseIdentNeedsQuote(%q) = %v, want %v", tt.in, got, tt.want)
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
		{name: "double escaped qualified", in: `\\\"ldf_server\\\".\\\"t_user\\\"`, wantSchema: "ldf_server", wantTable: "t_user"},
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
