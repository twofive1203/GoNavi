package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"GoNavi-Wails/internal/connection"
)

type duckMapLike map[any]any

func TestWriteResponse_NormalizesMapAnyAny(t *testing.T) {
	resp := agentResponse{
		ID:      1,
		Success: true,
		Data: []map[string]interface{}{
			{
				"id":   int64(7),
				"meta": duckMapLike{"k": "v", 2: "two"},
			},
		},
	}

	var out bytes.Buffer
	writer := bufio.NewWriter(&out)
	if err := writeResponse(writer, resp); err != nil {
		t.Fatalf("writeResponse 返回错误: %v", err)
	}

	var decoded struct {
		Data []map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &decoded); err != nil {
		t.Fatalf("解码响应失败: %v", err)
	}

	if len(decoded.Data) != 1 {
		t.Fatalf("期望 1 行数据，实际 %d", len(decoded.Data))
	}
	meta, ok := decoded.Data[0]["meta"].(map[string]interface{})
	if !ok {
		t.Fatalf("meta 字段类型异常: %T", decoded.Data[0]["meta"])
	}
	if meta["k"] != "v" {
		t.Fatalf("字符串 key 转换异常: %v", meta["k"])
	}
	if meta["2"] != "two" {
		t.Fatalf("数字 key 未字符串化: %v", meta["2"])
	}
}

func TestNormalizeAgentResponseData_KeepByteSlice(t *testing.T) {
	raw := []byte{0x61, 0x62, 0x63}
	normalized := normalizeAgentResponseData(raw)
	out, ok := normalized.([]byte)
	if !ok {
		t.Fatalf("期望 []byte，实际 %T", normalized)
	}
	if !bytes.Equal(out, raw) {
		t.Fatalf("[]byte 内容被意外改写: %v", out)
	}
}

type fakeAgentTimeoutDB struct {
	queryCalled        bool
	queryContextCalled bool
	execCalled         bool
	execContextCalled  bool
	deadlineSet        bool
}

func (f *fakeAgentTimeoutDB) Connect(config connection.ConnectionConfig) error { return nil }
func (f *fakeAgentTimeoutDB) Close() error                                     { return nil }
func (f *fakeAgentTimeoutDB) Ping() error                                      { return nil }
func (f *fakeAgentTimeoutDB) Query(query string) ([]map[string]interface{}, []string, error) {
	f.queryCalled = true
	return nil, nil, errors.New("query should not be called")
}
func (f *fakeAgentTimeoutDB) QueryContext(ctx context.Context, query string) ([]map[string]interface{}, []string, error) {
	f.queryContextCalled = true
	if _, ok := ctx.Deadline(); ok {
		f.deadlineSet = true
	}
	return []map[string]interface{}{{"ok": 1}}, []string{"ok"}, nil
}
func (f *fakeAgentTimeoutDB) Exec(query string) (int64, error) {
	f.execCalled = true
	return 0, errors.New("exec should not be called")
}
func (f *fakeAgentTimeoutDB) ExecContext(ctx context.Context, query string) (int64, error) {
	f.execContextCalled = true
	if _, ok := ctx.Deadline(); ok {
		f.deadlineSet = true
	}
	return 3, nil
}
func (f *fakeAgentTimeoutDB) GetDatabases() ([]string, error) { return nil, nil }
func (f *fakeAgentTimeoutDB) GetTables(dbName string) ([]string, error) {
	return nil, nil
}
func (f *fakeAgentTimeoutDB) GetCreateStatement(dbName, tableName string) (string, error) {
	return "", nil
}
func (f *fakeAgentTimeoutDB) GetColumns(dbName, tableName string) ([]connection.ColumnDefinition, error) {
	return nil, nil
}
func (f *fakeAgentTimeoutDB) GetAllColumns(dbName string) ([]connection.ColumnDefinitionWithTable, error) {
	return nil, nil
}
func (f *fakeAgentTimeoutDB) GetIndexes(dbName, tableName string) ([]connection.IndexDefinition, error) {
	return nil, nil
}
func (f *fakeAgentTimeoutDB) GetForeignKeys(dbName, tableName string) ([]connection.ForeignKeyDefinition, error) {
	return nil, nil
}
func (f *fakeAgentTimeoutDB) GetTriggers(dbName, tableName string) ([]connection.TriggerDefinition, error) {
	return nil, nil
}

func TestQueryWithOptionalTimeout_UsesQueryContext(t *testing.T) {
	fake := &fakeAgentTimeoutDB{}
	data, fields, err := queryWithOptionalTimeout(fake, "SELECT 1", int64((2 * time.Second).Milliseconds()))
	if err != nil {
		t.Fatalf("queryWithOptionalTimeout 返回错误: %v", err)
	}
	if !fake.queryContextCalled || fake.queryCalled {
		t.Fatalf("query 调用路径异常，QueryContext=%v Query=%v", fake.queryContextCalled, fake.queryCalled)
	}
	if !fake.deadlineSet {
		t.Fatal("queryWithOptionalTimeout 未设置 deadline")
	}
	if len(data) != 1 || len(fields) != 1 || fields[0] != "ok" {
		t.Fatalf("queryWithOptionalTimeout 返回数据异常: data=%v fields=%v", data, fields)
	}
}

func TestExecWithOptionalTimeout_UsesExecContext(t *testing.T) {
	fake := &fakeAgentTimeoutDB{}
	affected, err := execWithOptionalTimeout(fake, "DELETE FROM t", int64((2 * time.Second).Milliseconds()))
	if err != nil {
		t.Fatalf("execWithOptionalTimeout 返回错误: %v", err)
	}
	if !fake.execContextCalled || fake.execCalled {
		t.Fatalf("exec 调用路径异常，ExecContext=%v Exec=%v", fake.execContextCalled, fake.execCalled)
	}
	if !fake.deadlineSet {
		t.Fatal("execWithOptionalTimeout 未设置 deadline")
	}
	if affected != 3 {
		t.Fatalf("受影响行数异常，want=3 got=%d", affected)
	}
}

func TestQueryWithOptionalTimeout_ClickHouseLegacyModeUsesQueryContext(t *testing.T) {
	old := agentDriverType
	agentDriverType = "clickhouse"
	defer func() { agentDriverType = old }()

	fake := &fakeAgentTimeoutDB{}
	_, _, err := queryWithOptionalTimeout(fake, "SELECT 1", 0)
	if err != nil {
		t.Fatalf("queryWithOptionalTimeout 返回错误: %v", err)
	}
	if !fake.queryContextCalled || fake.queryCalled {
		t.Fatalf("clickhouse legacy query 调用路径异常，QueryContext=%v Query=%v", fake.queryContextCalled, fake.queryCalled)
	}
}
