package db

import "testing"

func TestNormalizeKingbaseIdentCommon(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain", in: "ldf_server", want: "ldf_server"},
		{name: "quoted", in: `"ldf_server"`, want: "ldf_server"},
		{name: "escaped quoted", in: `\"ldf_server\"`, want: "ldf_server"},
		{name: "double escaped quoted", in: `\\\"ldf_server\\\"`, want: "ldf_server"},
		{name: "double quoted", in: `""ldf_server""`, want: "ldf_server"},
		{name: "backtick quoted", in: "`ldf_server`", want: "ldf_server"},
		{name: "bracket quoted", in: "[ldf_server]", want: "ldf_server"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeKingbaseIdentCommon(tt.in); got != tt.want {
				t.Fatalf("normalizeKingbaseIdentCommon(%q)=%q,want=%q", tt.in, got, tt.want)
			}
		})
	}
}

func TestSplitKingbaseQualifiedNameCommon(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		wantSchema string
		wantTable  string
	}{
		{name: "plain", in: "ldf_server.andon_events", wantSchema: "ldf_server", wantTable: "andon_events"},
		{name: "quoted", in: `"ldf_server"."andon_events"`, wantSchema: "ldf_server", wantTable: "andon_events"},
		{name: "escaped quoted", in: `\"ldf_server\".\"andon_events\"`, wantSchema: "ldf_server", wantTable: "andon_events"},
		{name: "double escaped quoted", in: `\\\"ldf_server\\\".\\\"andon_events\\\"`, wantSchema: "ldf_server", wantTable: "andon_events"},
		{name: "space around dot", in: ` "ldf_server" . "andon_events" `, wantSchema: "ldf_server", wantTable: "andon_events"},
		{name: "table only", in: "andon_events", wantSchema: "", wantTable: "andon_events"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSchema, gotTable := splitKingbaseQualifiedNameCommon(tt.in)
			if gotSchema != tt.wantSchema || gotTable != tt.wantTable {
				t.Fatalf("splitKingbaseQualifiedNameCommon(%q)=(%q,%q),want=(%q,%q)", tt.in, gotSchema, gotTable, tt.wantSchema, tt.wantTable)
			}
		})
	}
}
