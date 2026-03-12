package db

import (
	"testing"

	"GoNavi-Wails/internal/connection"
)

func TestNormalizeKingbaseAgentTableName(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain", in: "ldf_server.andon_events", want: "ldf_server.andon_events"},
		{name: "quoted", in: `"ldf_server"."andon_events"`, want: "ldf_server.andon_events"},
		{name: "double quoted", in: `""ldf_server"".""andon_events""`, want: "ldf_server.andon_events"},
		{name: "escaped", in: `\"ldf_server\".\"andon_events\"`, want: "ldf_server.andon_events"},
		{name: "double escaped", in: `\\\"ldf_server\\\".\\\"andon_events\\\"`, want: "ldf_server.andon_events"},
		{name: "space around dot", in: ` "ldf_server" . "andon_events" `, want: "ldf_server.andon_events"},
		{name: "table only", in: `bcs_barcode`, want: "bcs_barcode"},
		{name: "table only quoted", in: `"bcs_barcode"`, want: "bcs_barcode"},
		{name: "table only double quoted", in: `""bcs_barcode""`, want: "bcs_barcode"},
		{name: "table only double escaped", in: `\\\"bcs_barcode\\\"`, want: "bcs_barcode"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeKingbaseAgentTableName(tt.in); got != tt.want {
				t.Fatalf("normalizeKingbaseAgentTableName(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestNormalizeKingbaseAgentChangeSetByColumns(t *testing.T) {
	columns := []string{"andon_events_id", "event_name", "event_code"}
	input := connection.ChangeSet{
		Inserts: []map[string]interface{}{
			{"event name": "物料1", "event_code": "EV-0001", "andon_events_id": 1},
		},
		Updates: []connection.UpdateRow{
			{Keys: map[string]interface{}{"andon_events_id": 1}, Values: map[string]interface{}{"event name": "物料2"}},
		},
		Deletes: []map[string]interface{}{
			{"andon_events_id": 1},
		},
	}

	out, err := normalizeKingbaseAgentChangeSetByColumns(input, columns)
	if err != nil {
		t.Fatalf("normalizeKingbaseAgentChangeSetByColumns error: %v", err)
	}

	if _, ok := out.Inserts[0]["event_name"]; !ok {
		t.Fatalf("expected insert to map \"event name\" -> \"event_name\"")
	}
	if _, ok := out.Inserts[0]["event name"]; ok {
		t.Fatalf("unexpected insert key \"event name\" after normalization")
	}
	if _, ok := out.Updates[0].Values["event_name"]; !ok {
		t.Fatalf("expected update values to map \"event name\" -> \"event_name\"")
	}
	if _, ok := out.Updates[0].Values["event name"]; ok {
		t.Fatalf("unexpected update value key \"event name\" after normalization")
	}
}
