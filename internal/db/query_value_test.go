package db

import (
	"encoding/json"
	"testing"
)

type duckMapLike map[any]any

func TestNormalizeQueryValueWithDBType_BitBytes(t *testing.T) {
	v := normalizeQueryValueWithDBType([]byte{0x00}, "BIT")
	if v != int64(0) {
		t.Fatalf("BIT 0x00 期望为 0，实际=%v(%T)", v, v)
	}

	v = normalizeQueryValueWithDBType([]byte{0x01}, "bit")
	if v != int64(1) {
		t.Fatalf("BIT 0x01 期望为 1，实际=%v(%T)", v, v)
	}

	v = normalizeQueryValueWithDBType([]byte{0x01, 0x02}, "BIT VARYING")
	if v != int64(258) {
		t.Fatalf("BIT 0x0102 期望为 258，实际=%v(%T)", v, v)
	}
}

func TestNormalizeQueryValueWithDBType_BitLargeAsString(t *testing.T) {
	v := normalizeQueryValueWithDBType([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, "BIT")
	if s, ok := v.(string); !ok || s != "18446744073709551615" {
		t.Fatalf("BIT 0xffffffffffffffff 期望为 string(18446744073709551615)，实际=%v(%T)", v, v)
	}
}

func TestNormalizeQueryValueWithDBType_ByteFallbacks(t *testing.T) {
	v := normalizeQueryValueWithDBType([]byte("abc"), "")
	if v != "abc" {
		t.Fatalf("文本 []byte 期望返回 string，实际=%v(%T)", v, v)
	}

	v = normalizeQueryValueWithDBType([]byte{0x00}, "")
	if v != int64(0) {
		t.Fatalf("未知类型 0x00 期望返回 0，实际=%v(%T)", v, v)
	}

	v = normalizeQueryValueWithDBType([]byte{0xff}, "")
	if v != "0xff" {
		t.Fatalf("未知类型 0xff 期望返回 0xff，实际=%v(%T)", v, v)
	}
}

func TestNormalizeQueryValueWithDBType_MapAnyAnyForJSON(t *testing.T) {
	input := duckMapLike{
		"id":    int64(1),
		1:       "one",
		true:    []interface{}{duckMapLike{2: "two"}},
		"bytes": []byte("ok"),
	}

	v := normalizeQueryValueWithDBType(input, "")
	root, ok := v.(map[string]interface{})
	if !ok {
		t.Fatalf("期望转换为 map[string]interface{}，实际=%T", v)
	}

	if root["id"] != int64(1) {
		t.Fatalf("id 字段异常，实际=%v(%T)", root["id"], root["id"])
	}
	if root["1"] != "one" {
		t.Fatalf("数字 key 未被字符串化，实际=%v(%T)", root["1"], root["1"])
	}
	if root["bytes"] != "ok" {
		t.Fatalf("嵌套 []byte 未被转换，实际=%v(%T)", root["bytes"], root["bytes"])
	}

	arr, ok := root["true"].([]interface{})
	if !ok || len(arr) != 1 {
		t.Fatalf("bool key 下的数组结构异常，实际=%v(%T)", root["true"], root["true"])
	}
	nested, ok := arr[0].(map[string]interface{})
	if !ok {
		t.Fatalf("嵌套 map 未被转换，实际=%v(%T)", arr[0], arr[0])
	}
	if nested["2"] != "two" {
		t.Fatalf("嵌套 map 数字 key 未转换，实际=%v(%T)", nested["2"], nested["2"])
	}
}

func TestNormalizeQueryValueWithDBType_UnsafeIntegersAsString(t *testing.T) {
	cases := []struct {
		name  string
		input interface{}
		want  string
	}{
		{name: "int64 overflow", input: int64(9007199254740992), want: "9007199254740992"},
		{name: "int64 underflow", input: int64(-9007199254740992), want: "-9007199254740992"},
		{name: "uint64 overflow", input: uint64(9007199254740992), want: "9007199254740992"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeQueryValueWithDBType(tc.input, "")
			if got != tc.want {
				t.Fatalf("期望=%q，实际=%v(%T)", tc.want, got, got)
			}
		})
	}
}

func TestNormalizeQueryValueWithDBType_SafeIntegersKeepType(t *testing.T) {
	got := normalizeQueryValueWithDBType(int64(9007199254740991), "")
	if _, ok := got.(int64); !ok {
		t.Fatalf("安全范围 int64 应保持数字类型，实际=%v(%T)", got, got)
	}

	got = normalizeQueryValueWithDBType(uint64(9007199254740991), "")
	if _, ok := got.(uint64); !ok {
		t.Fatalf("安全范围 uint64 应保持数字类型，实际=%v(%T)", got, got)
	}
}

func TestNormalizeQueryValueWithDBType_JSONNumber(t *testing.T) {
	cases := []struct {
		name      string
		input     json.Number
		wantType  string
		wantValue string
	}{
		{name: "safe integer", input: json.Number("9007199254740991"), wantType: "int64", wantValue: "9007199254740991"},
		{name: "unsafe integer", input: json.Number("9007199254740992"), wantType: "string", wantValue: "9007199254740992"},
		{name: "unsafe negative integer", input: json.Number("-9007199254740992"), wantType: "string", wantValue: "-9007199254740992"},
		{name: "decimal", input: json.Number("12.5"), wantType: "float64", wantValue: "12.5"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeQueryValueWithDBType(tc.input, "")
			switch tc.wantType {
			case "int64":
				v, ok := got.(int64)
				if !ok {
					t.Fatalf("期望 int64，实际=%T", got)
				}
				if v != 9007199254740991 {
					t.Fatalf("期望值=%s，实际=%d", tc.wantValue, v)
				}
			case "string":
				v, ok := got.(string)
				if !ok {
					t.Fatalf("期望 string，实际=%T", got)
				}
				if v != tc.wantValue {
					t.Fatalf("期望值=%s，实际=%s", tc.wantValue, v)
				}
			case "float64":
				v, ok := got.(float64)
				if !ok {
					t.Fatalf("期望 float64，实际=%T", got)
				}
				if v != 12.5 {
					t.Fatalf("期望值=%s，实际=%v", tc.wantValue, v)
				}
			default:
				t.Fatalf("未知断言类型：%s", tc.wantType)
			}
		})
	}
}
