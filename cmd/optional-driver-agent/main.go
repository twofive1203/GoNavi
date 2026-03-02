package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"GoNavi-Wails/internal/connection"
	"GoNavi-Wails/internal/db"
)

type agentRequest struct {
	ID        int64                        `json:"id"`
	Method    string                       `json:"method"`
	Config    *connection.ConnectionConfig `json:"config,omitempty"`
	Query     string                       `json:"query,omitempty"`
	TimeoutMs int64                        `json:"timeoutMs,omitempty"`
	DBName    string                       `json:"dbName,omitempty"`
	TableName string                       `json:"tableName,omitempty"`
	Changes   *connection.ChangeSet        `json:"changes,omitempty"`
}

type agentResponse struct {
	ID           int64       `json:"id"`
	Success      bool        `json:"success"`
	Error        string      `json:"error,omitempty"`
	Data         interface{} `json:"data,omitempty"`
	Fields       []string    `json:"fields,omitempty"`
	RowsAffected int64       `json:"rowsAffected,omitempty"`
}

const (
	agentMethodConnect       = "connect"
	agentMethodClose         = "close"
	agentMethodPing          = "ping"
	agentMethodQuery         = "query"
	agentMethodExec          = "exec"
	agentMethodGetDatabases  = "getDatabases"
	agentMethodGetTables     = "getTables"
	agentMethodGetCreateStmt = "getCreateStatement"
	agentMethodGetColumns    = "getColumns"
	agentMethodGetAllColumns = "getAllColumns"
	agentMethodGetIndexes    = "getIndexes"
	agentMethodGetForeignKey = "getForeignKeys"
	agentMethodGetTriggers   = "getTriggers"
	agentMethodApplyChanges  = "applyChanges"
)

const legacyClickHouseDefaultTimeout = 2 * time.Hour

var (
	agentDriverType      string
	agentDatabaseFactory func() db.Database
)

func main() {
	if agentDatabaseFactory == nil || strings.TrimSpace(agentDriverType) == "" {
		fmt.Fprintf(os.Stderr, "未配置驱动代理 provider，请使用 gonavi_<driver>_driver 标签构建\n")
		os.Exit(2)
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 16<<10), 8<<20)
	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()

	var inst db.Database
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var req agentRequest
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			_ = writeResponse(writer, agentResponse{
				ID:      req.ID,
				Success: false,
				Error:   fmt.Sprintf("解析请求失败：%v", err),
			})
			continue
		}

		resp := handleRequest(&inst, req)
		if err := writeResponse(writer, resp); err != nil {
			fmt.Fprintf(os.Stderr, "写入响应失败：%v\n", err)
			break
		}
	}

	if inst != nil {
		_ = inst.Close()
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "读取请求失败：%v\n", err)
	}
}

func handleRequest(inst *db.Database, req agentRequest) agentResponse {
	resp := agentResponse{ID: req.ID, Success: true}
	method := strings.TrimSpace(req.Method)

	switch method {
	case agentMethodConnect:
		if req.Config == nil {
			return fail(resp, "连接配置为空")
		}
		if *inst != nil {
			_ = (*inst).Close()
		}
		next := agentDatabaseFactory()
		if next == nil {
			return fail(resp, "驱动代理初始化失败")
		}
		if err := next.Connect(*req.Config); err != nil {
			return fail(resp, err.Error())
		}
		*inst = next
		return resp
	case agentMethodClose:
		if *inst != nil {
			if err := (*inst).Close(); err != nil {
				return fail(resp, err.Error())
			}
			*inst = nil
		}
		return resp
	}

	if *inst == nil {
		return fail(resp, "connection not open")
	}

	switch method {
	case agentMethodPing:
		if err := (*inst).Ping(); err != nil {
			return fail(resp, err.Error())
		}
	case agentMethodQuery:
		data, fields, err := queryWithOptionalTimeout(*inst, req.Query, req.TimeoutMs)
		if err != nil {
			return fail(resp, err.Error())
		}
		resp.Data = data
		resp.Fields = fields
	case agentMethodExec:
		affected, err := execWithOptionalTimeout(*inst, req.Query, req.TimeoutMs)
		if err != nil {
			return fail(resp, err.Error())
		}
		resp.RowsAffected = affected
	case agentMethodGetDatabases:
		data, err := (*inst).GetDatabases()
		if err != nil {
			return fail(resp, err.Error())
		}
		resp.Data = data
	case agentMethodGetTables:
		data, err := (*inst).GetTables(req.DBName)
		if err != nil {
			return fail(resp, err.Error())
		}
		resp.Data = data
	case agentMethodGetCreateStmt:
		data, err := (*inst).GetCreateStatement(req.DBName, req.TableName)
		if err != nil {
			return fail(resp, err.Error())
		}
		resp.Data = data
	case agentMethodGetColumns:
		data, err := (*inst).GetColumns(req.DBName, req.TableName)
		if err != nil {
			return fail(resp, err.Error())
		}
		resp.Data = data
	case agentMethodGetAllColumns:
		data, err := (*inst).GetAllColumns(req.DBName)
		if err != nil {
			return fail(resp, err.Error())
		}
		resp.Data = data
	case agentMethodGetIndexes:
		data, err := (*inst).GetIndexes(req.DBName, req.TableName)
		if err != nil {
			return fail(resp, err.Error())
		}
		resp.Data = data
	case agentMethodGetForeignKey:
		data, err := (*inst).GetForeignKeys(req.DBName, req.TableName)
		if err != nil {
			return fail(resp, err.Error())
		}
		resp.Data = data
	case agentMethodGetTriggers:
		data, err := (*inst).GetTriggers(req.DBName, req.TableName)
		if err != nil {
			return fail(resp, err.Error())
		}
		resp.Data = data
	case agentMethodApplyChanges:
		if req.Changes == nil {
			return fail(resp, "变更集为空")
		}
		applier, ok := (*inst).(interface {
			ApplyChanges(tableName string, changes connection.ChangeSet) error
		})
		if !ok {
			return fail(resp, "当前驱动不支持 ApplyChanges")
		}
		if err := applier.ApplyChanges(req.TableName, *req.Changes); err != nil {
			return fail(resp, err.Error())
		}
	default:
		return fail(resp, "不支持的方法")
	}

	return resp
}

func writeResponse(writer *bufio.Writer, resp agentResponse) error {
	// 对响应数据做统一 JSON 安全归一化：
	// 将 map[any]any（如 duckdb.Map）递归转换为 map[string]any，避免序列化失败导致代理进程退出。
	safeResp := resp
	safeResp.Data = normalizeAgentResponseData(resp.Data)
	payload, err := json.Marshal(safeResp)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	if _, err := writer.Write(payload); err != nil {
		return err
	}
	return writer.Flush()
}

func fail(resp agentResponse, errText string) agentResponse {
	resp.Success = false
	resp.Error = strings.TrimSpace(errText)
	return resp
}

func normalizeAgentResponseData(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		return normalizeAgentResponseData(rv.Elem().Interface())
	case reflect.Map:
		if rv.IsNil() {
			return nil
		}
		out := make(map[string]interface{}, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			out[fmt.Sprint(iter.Key().Interface())] = normalizeAgentResponseData(iter.Value().Interface())
		}
		return out
	case reflect.Slice:
		if rv.IsNil() {
			return nil
		}
		// 保持 []byte 原样，避免改变现有二进制列的 JSON 编码行为（base64）。
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return v
		}
		size := rv.Len()
		items := make([]interface{}, size)
		for i := 0; i < size; i++ {
			items[i] = normalizeAgentResponseData(rv.Index(i).Interface())
		}
		return items
	case reflect.Array:
		size := rv.Len()
		items := make([]interface{}, size)
		for i := 0; i < size; i++ {
			items[i] = normalizeAgentResponseData(rv.Index(i).Interface())
		}
		return items
	default:
		return v
	}
}

func queryWithOptionalTimeout(inst db.Database, query string, timeoutMs int64) ([]map[string]interface{}, []string, error) {
	effectiveTimeoutMs := timeoutMs
	if effectiveTimeoutMs <= 0 && strings.EqualFold(strings.TrimSpace(agentDriverType), "clickhouse") {
		effectiveTimeoutMs = int64(legacyClickHouseDefaultTimeout / time.Millisecond)
	}
	if effectiveTimeoutMs <= 0 {
		return inst.Query(query)
	}
	if q, ok := inst.(interface {
		QueryContext(context.Context, string) ([]map[string]interface{}, []string, error)
	}); ok {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(effectiveTimeoutMs)*time.Millisecond)
		defer cancel()
		return q.QueryContext(ctx, query)
	}
	return inst.Query(query)
}

func execWithOptionalTimeout(inst db.Database, query string, timeoutMs int64) (int64, error) {
	effectiveTimeoutMs := timeoutMs
	if effectiveTimeoutMs <= 0 && strings.EqualFold(strings.TrimSpace(agentDriverType), "clickhouse") {
		effectiveTimeoutMs = int64(legacyClickHouseDefaultTimeout / time.Millisecond)
	}
	if effectiveTimeoutMs <= 0 {
		return inst.Exec(query)
	}
	if e, ok := inst.(interface {
		ExecContext(context.Context, string) (int64, error)
	}); ok {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(effectiveTimeoutMs)*time.Millisecond)
		defer cancel()
		return e.ExecContext(ctx, query)
	}
	return inst.Exec(query)
}
