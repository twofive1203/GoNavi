package db

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"GoNavi-Wails/internal/connection"
	"GoNavi-Wails/internal/logger"
)

const (
	optionalAgentMethodConnect          = "connect"
	optionalAgentMethodClose            = "close"
	optionalAgentMethodPing             = "ping"
	optionalAgentMethodQuery            = "query"
	optionalAgentMethodExec             = "exec"
	optionalAgentMethodGetDatabases     = "getDatabases"
	optionalAgentMethodGetTables        = "getTables"
	optionalAgentMethodGetCreateStmt    = "getCreateStatement"
	optionalAgentMethodGetColumns       = "getColumns"
	optionalAgentMethodGetAllColumns    = "getAllColumns"
	optionalAgentMethodGetIndexes       = "getIndexes"
	optionalAgentMethodGetForeignKeys   = "getForeignKeys"
	optionalAgentMethodGetTriggers      = "getTriggers"
	optionalAgentMethodApplyChanges     = "applyChanges"
	optionalAgentDefaultScannerMaxBytes = 8 << 20
)

type optionalAgentRequest struct {
	ID        int64                        `json:"id"`
	Method    string                       `json:"method"`
	Config    *connection.ConnectionConfig `json:"config,omitempty"`
	Query     string                       `json:"query,omitempty"`
	TimeoutMs int64                        `json:"timeoutMs,omitempty"`
	DBName    string                       `json:"dbName,omitempty"`
	TableName string                       `json:"tableName,omitempty"`
	Changes   *connection.ChangeSet        `json:"changes,omitempty"`
}

type optionalAgentResponse struct {
	ID           int64           `json:"id"`
	Success      bool            `json:"success"`
	Error        string          `json:"error,omitempty"`
	Data         json.RawMessage `json:"data,omitempty"`
	Fields       []string        `json:"fields,omitempty"`
	RowsAffected int64           `json:"rowsAffected,omitempty"`
}

type optionalDriverAgentClient struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	reader   *bufio.Reader
	nextID   int64
	mu       sync.Mutex
	stderrMu sync.Mutex
	stderr   strings.Builder
	driver   string
}

func newOptionalDriverAgentClient(driverType string, executablePath string) (*optionalDriverAgentClient, error) {
	pathText := strings.TrimSpace(executablePath)
	if pathText == "" {
		return nil, fmt.Errorf("%s 驱动代理路径为空", driverDisplayName(driverType))
	}
	info, err := os.Stat(pathText)
	if err != nil {
		return nil, fmt.Errorf("%s 驱动代理不存在：%s", driverDisplayName(driverType), pathText)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s 驱动代理路径是目录：%s", driverDisplayName(driverType), pathText)
	}

	cmd := exec.Command(pathText)
	configureAgentProcess(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("创建 %s 驱动代理 stdin 失败：%w", driverDisplayName(driverType), err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("创建 %s 驱动代理 stdout 失败：%w", driverDisplayName(driverType), err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("创建 %s 驱动代理 stderr 失败：%w", driverDisplayName(driverType), err)
	}
	if err := cmd.Start(); err != nil {
		if isWindowsExecutableMachineMismatch(err) {
			return nil, fmt.Errorf("启动 %s 驱动代理失败：%w（检测到驱动代理与当前系统架构不兼容，请在驱动管理中重新安装启用）", driverDisplayName(driverType), err)
		}
		return nil, fmt.Errorf("启动 %s 驱动代理失败：%w", driverDisplayName(driverType), err)
	}

	client := &optionalDriverAgentClient{
		cmd:    cmd,
		stdin:  stdin,
		reader: bufio.NewReader(stdout),
		driver: normalizeRuntimeDriverType(driverType),
	}
	go client.captureStderr(stderr)
	return client, nil
}

func isWindowsExecutableMachineMismatch(err error) bool {
	if err == nil || runtime.GOOS != "windows" {
		return false
	}
	var errno syscall.Errno
	if errors.As(err, &errno) && errno == syscall.Errno(216) {
		return true
	}
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	if text == "" {
		return false
	}
	if strings.Contains(text, "not compatible with the version of windows") {
		return true
	}
	if strings.Contains(text, "win32") && strings.Contains(text, "compatible") {
		return true
	}
	if strings.Contains(text, "不是有效的win32应用程序") || strings.Contains(text, "无法在win32模式下运行") {
		return true
	}
	return false
}

func (c *optionalDriverAgentClient) captureStderr(stderr io.Reader) {
	scanner := bufio.NewScanner(stderr)
	buffer := make([]byte, 0, 8<<10)
	scanner.Buffer(buffer, optionalAgentDefaultScannerMaxBytes)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		logger.Warnf("%s 驱动代理 stderr: %s", driverDisplayName(c.driver), line)
		c.stderrMu.Lock()
		if c.stderr.Len() > 0 {
			c.stderr.WriteString(" | ")
		}
		c.stderr.WriteString(line)
		c.stderrMu.Unlock()
	}
}

func (c *optionalDriverAgentClient) stderrText() string {
	c.stderrMu.Lock()
	defer c.stderrMu.Unlock()
	return strings.TrimSpace(c.stderr.String())
}

func (c *optionalDriverAgentClient) call(req optionalAgentRequest, out interface{}, fields *[]string, rowsAffected *int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nextID++
	req.ID = c.nextID

	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	if _, err := c.stdin.Write(payload); err != nil {
		stderrText := c.stderrText()
		if stderrText == "" {
			return fmt.Errorf("调用 %s 驱动代理失败：%w", driverDisplayName(c.driver), err)
		}
		return fmt.Errorf("调用 %s 驱动代理失败：%w（stderr: %s）", driverDisplayName(c.driver), err, stderrText)
	}

	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		stderrText := c.stderrText()
		if stderrText == "" {
			return fmt.Errorf("读取 %s 驱动代理响应失败：%w", driverDisplayName(c.driver), err)
		}
		return fmt.Errorf("读取 %s 驱动代理响应失败：%w（stderr: %s）", driverDisplayName(c.driver), err, stderrText)
	}

	var resp optionalAgentResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		return fmt.Errorf("解析 %s 驱动代理响应失败：%w", driverDisplayName(c.driver), err)
	}
	if !resp.Success {
		errText := strings.TrimSpace(resp.Error)
		if errText == "" {
			errText = fmt.Sprintf("%s 驱动代理返回失败", driverDisplayName(c.driver))
		}
		return errors.New(errText)
	}

	if fields != nil {
		*fields = resp.Fields
	}
	if rowsAffected != nil {
		*rowsAffected = resp.RowsAffected
	}
	if out != nil && len(resp.Data) > 0 {
		if err := decodeJSONWithUseNumber(resp.Data, out); err != nil {
			return fmt.Errorf("解析 %s 驱动代理数据失败：%w", driverDisplayName(c.driver), err)
		}
	}
	return nil
}

func (c *optionalDriverAgentClient) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var closeErr error
	if c.stdin != nil {
		_ = c.stdin.Close()
	}
	if c.cmd != nil && c.cmd.Process != nil {
		if err := c.cmd.Process.Kill(); err != nil {
			closeErr = err
		}
	}
	if c.cmd != nil {
		_ = c.cmd.Wait()
	}
	return closeErr
}

type OptionalDriverAgentDB struct {
	driverType string
	client     *optionalDriverAgentClient
}

func newOptionalDriverAgentDatabase(driverType string) databaseFactory {
	normalized := normalizeRuntimeDriverType(driverType)
	return func() Database {
		return &OptionalDriverAgentDB{driverType: normalized}
	}
}

func (d *OptionalDriverAgentDB) Connect(config connection.ConnectionConfig) error {
	if d.client != nil {
		_ = d.client.close()
		d.client = nil
	}

	executablePath, err := ResolveOptionalDriverAgentExecutablePath("", d.driverType)
	if err != nil {
		return err
	}
	logger.Infof("%s 驱动代理路径：%s", driverDisplayName(d.driverType), executablePath)
	client, err := newOptionalDriverAgentClient(d.driverType, executablePath)
	if err != nil {
		return err
	}
	if err := client.call(optionalAgentRequest{
		Method: optionalAgentMethodConnect,
		Config: &config,
	}, nil, nil, nil); err != nil {
		_ = client.close()
		return err
	}
	d.client = client
	d.ensureKingbaseSearchPath(config)
	return nil
}

func (d *OptionalDriverAgentDB) Close() error {
	if d.client == nil {
		return nil
	}
	_ = d.client.call(optionalAgentRequest{Method: optionalAgentMethodClose}, nil, nil, nil)
	err := d.client.close()
	d.client = nil
	return err
}

func (d *OptionalDriverAgentDB) Ping() error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	return client.call(optionalAgentRequest{Method: optionalAgentMethodPing}, nil, nil, nil)
}

func (d *OptionalDriverAgentDB) QueryContext(ctx context.Context, query string) ([]map[string]interface{}, []string, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	client, err := d.requireClient()
	if err != nil {
		return nil, nil, err
	}
	var data []map[string]interface{}
	var fields []string
	if err := client.call(optionalAgentRequest{
		Method:    optionalAgentMethodQuery,
		Query:     query,
		TimeoutMs: timeoutMsFromContext(ctx),
	}, &data, &fields, nil); err != nil {
		return nil, nil, err
	}
	return data, fields, nil
}

func (d *OptionalDriverAgentDB) Query(query string) ([]map[string]interface{}, []string, error) {
	client, err := d.requireClient()
	if err != nil {
		return nil, nil, err
	}
	var data []map[string]interface{}
	var fields []string
	if err := client.call(optionalAgentRequest{
		Method: optionalAgentMethodQuery,
		Query:  query,
	}, &data, &fields, nil); err != nil {
		return nil, nil, err
	}
	return data, fields, nil
}

func (d *OptionalDriverAgentDB) ExecContext(ctx context.Context, query string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	client, err := d.requireClient()
	if err != nil {
		return 0, err
	}
	var affected int64
	if err := client.call(optionalAgentRequest{
		Method:    optionalAgentMethodExec,
		Query:     query,
		TimeoutMs: timeoutMsFromContext(ctx),
	}, nil, nil, &affected); err != nil {
		return 0, err
	}
	return affected, nil
}

func (d *OptionalDriverAgentDB) Exec(query string) (int64, error) {
	client, err := d.requireClient()
	if err != nil {
		return 0, err
	}
	var affected int64
	if err := client.call(optionalAgentRequest{
		Method: optionalAgentMethodExec,
		Query:  query,
	}, nil, nil, &affected); err != nil {
		return 0, err
	}
	return affected, nil
}

func (d *OptionalDriverAgentDB) GetDatabases() ([]string, error) {
	client, err := d.requireClient()
	if err != nil {
		return nil, err
	}
	var dbs []string
	if err := client.call(optionalAgentRequest{
		Method: optionalAgentMethodGetDatabases,
	}, &dbs, nil, nil); err != nil {
		return nil, err
	}
	return dbs, nil
}

func (d *OptionalDriverAgentDB) GetTables(dbName string) ([]string, error) {
	client, err := d.requireClient()
	if err != nil {
		return nil, err
	}
	var tables []string
	if err := client.call(optionalAgentRequest{
		Method: optionalAgentMethodGetTables,
		DBName: dbName,
	}, &tables, nil, nil); err != nil {
		return nil, err
	}
	return tables, nil
}

func (d *OptionalDriverAgentDB) GetCreateStatement(dbName, tableName string) (string, error) {
	client, err := d.requireClient()
	if err != nil {
		return "", err
	}
	var sqlText string
	if err := client.call(optionalAgentRequest{
		Method:    optionalAgentMethodGetCreateStmt,
		DBName:    dbName,
		TableName: tableName,
	}, &sqlText, nil, nil); err != nil {
		return "", err
	}
	return sqlText, nil
}

func (d *OptionalDriverAgentDB) GetColumns(dbName, tableName string) ([]connection.ColumnDefinition, error) {
	client, err := d.requireClient()
	if err != nil {
		return nil, err
	}
	var columns []connection.ColumnDefinition
	if err := client.call(optionalAgentRequest{
		Method:    optionalAgentMethodGetColumns,
		DBName:    dbName,
		TableName: tableName,
	}, &columns, nil, nil); err != nil {
		return nil, err
	}
	return columns, nil
}

func (d *OptionalDriverAgentDB) GetAllColumns(dbName string) ([]connection.ColumnDefinitionWithTable, error) {
	client, err := d.requireClient()
	if err != nil {
		return nil, err
	}
	var columns []connection.ColumnDefinitionWithTable
	if err := client.call(optionalAgentRequest{
		Method: optionalAgentMethodGetAllColumns,
		DBName: dbName,
	}, &columns, nil, nil); err != nil {
		return nil, err
	}
	return columns, nil
}

func (d *OptionalDriverAgentDB) GetIndexes(dbName, tableName string) ([]connection.IndexDefinition, error) {
	client, err := d.requireClient()
	if err != nil {
		return nil, err
	}
	var indexes []connection.IndexDefinition
	if err := client.call(optionalAgentRequest{
		Method:    optionalAgentMethodGetIndexes,
		DBName:    dbName,
		TableName: tableName,
	}, &indexes, nil, nil); err != nil {
		return nil, err
	}
	return indexes, nil
}

func (d *OptionalDriverAgentDB) GetForeignKeys(dbName, tableName string) ([]connection.ForeignKeyDefinition, error) {
	client, err := d.requireClient()
	if err != nil {
		return nil, err
	}
	var keys []connection.ForeignKeyDefinition
	if err := client.call(optionalAgentRequest{
		Method:    optionalAgentMethodGetForeignKeys,
		DBName:    dbName,
		TableName: tableName,
	}, &keys, nil, nil); err != nil {
		return nil, err
	}
	return keys, nil
}

func (d *OptionalDriverAgentDB) GetTriggers(dbName, tableName string) ([]connection.TriggerDefinition, error) {
	client, err := d.requireClient()
	if err != nil {
		return nil, err
	}
	var triggers []connection.TriggerDefinition
	if err := client.call(optionalAgentRequest{
		Method:    optionalAgentMethodGetTriggers,
		DBName:    dbName,
		TableName: tableName,
	}, &triggers, nil, nil); err != nil {
		return nil, err
	}
	return triggers, nil
}

func (d *OptionalDriverAgentDB) ApplyChanges(tableName string, changes connection.ChangeSet) error {
	client, err := d.requireClient()
	if err != nil {
		return err
	}
	if strings.EqualFold(d.driverType, "kingbase") {
		if normalized := normalizeKingbaseAgentTableName(tableName); normalized != "" {
			tableName = normalized
		}
		if normalized, normErr := d.normalizeKingbaseAgentChangeSet(tableName, changes); normErr == nil {
			changes = normalized
		} else {
			logger.Warnf("Kingbase ApplyChanges 字段名规范化失败：%v", normErr)
		}
	}
	return client.call(optionalAgentRequest{
		Method:    optionalAgentMethodApplyChanges,
		TableName: tableName,
		Changes:   &changes,
	}, nil, nil, nil)
}

func (d *OptionalDriverAgentDB) requireClient() (*optionalDriverAgentClient, error) {
	if d.client == nil {
		return nil, fmt.Errorf("connection not open")
	}
	return d.client, nil
}

func (d *OptionalDriverAgentDB) ensureKingbaseSearchPath(config connection.ConnectionConfig) {
	if !strings.EqualFold(d.driverType, "kingbase") {
		return
	}
	client, err := d.requireClient()
	if err != nil || client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	schemas, err := d.listKingbaseSchemas(ctx)
	if err != nil || len(schemas) == 0 {
		if err != nil {
			logger.Warnf("人大金仓驱动代理探测 schema 失败：%v", err)
		}
		return
	}

	searchPath := buildKingbaseSearchPathFromSchemas(schemas)
	if strings.TrimSpace(searchPath) == "" {
		return
	}

	if _, err := d.ExecContext(ctx, fmt.Sprintf("SET search_path TO %s", searchPath)); err != nil {
		logger.Warnf("人大金仓驱动代理设置 search_path 失败：%v", err)
		return
	}
	logger.Infof("人大金仓驱动代理已设置默认 search_path：%s", searchPath)
}

func (d *OptionalDriverAgentDB) listKingbaseSchemas(ctx context.Context) ([]string, error) {
	query := `SELECT nspname FROM pg_namespace
		WHERE nspname NOT IN ('pg_catalog', 'information_schema')
		  AND nspname NOT LIKE 'pg_%'
		ORDER BY nspname`
	rows, _, err := d.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	schemas := make([]string, 0, len(rows))
	for _, row := range rows {
		for key, val := range row {
			if strings.EqualFold(key, "nspname") || strings.EqualFold(key, "schema") {
				name := strings.TrimSpace(fmt.Sprintf("%v", val))
				if name != "" {
					schemas = append(schemas, name)
				}
				break
			}
		}
		if len(row) == 1 {
			for _, val := range row {
				name := strings.TrimSpace(fmt.Sprintf("%v", val))
				if name != "" {
					schemas = append(schemas, name)
				}
				break
			}
		}
	}
	return schemas, nil
}

func buildKingbaseSearchPathFromSchemas(schemas []string) string {
	if len(schemas) == 0 {
		return ""
	}
	seen := make(map[string]struct{}, len(schemas)+1)
	parts := make([]string, 0, len(schemas)+1)
	for _, name := range schemas {
		trimmed := normalizeKingbaseAgentIdent(name)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		parts = append(parts, quoteKingbaseAgentIdent(trimmed))
	}
	if _, ok := seen["public"]; !ok {
		parts = append(parts, "public")
	}
	return strings.Join(parts, ", ")
}

func quoteKingbaseAgentIdent(name string) string {
	n := normalizeKingbaseAgentIdent(name)
	if n == "" {
		return "\"\""
	}
	n = strings.ReplaceAll(n, `"`, `""`)
	return `"` + n + `"`
}

func normalizeKingbaseAgentTableName(raw string) string {
	schema, table := splitKingbaseQualifiedNameCommon(raw)
	if table == "" {
		return ""
	}
	if schema == "" {
		return table
	}
	return schema + "." + table
}

func normalizeKingbaseAgentIdent(raw string) string {
	return normalizeKingbaseIdentCommon(raw)
}

type kingbaseAgentColumnIndex struct {
	exact   map[string]string
	compact map[string]string
}

func buildKingbaseAgentColumnIndex(columns []string) kingbaseAgentColumnIndex {
	exact := make(map[string]string, len(columns))
	compact := make(map[string]string, len(columns))
	compactSeen := make(map[string]string, len(columns))
	compactDup := make(map[string]struct{}, len(columns))

	for _, col := range columns {
		name := normalizeKingbaseAgentIdent(col)
		if name == "" {
			continue
		}
		lower := strings.ToLower(name)
		if _, ok := exact[lower]; !ok {
			exact[lower] = name
		}
		key := normalizeKingbaseAgentCompactKey(name)
		if key == "" {
			continue
		}
		if prev, ok := compactSeen[key]; ok && !strings.EqualFold(prev, name) {
			compactDup[key] = struct{}{}
			continue
		}
		compactSeen[key] = name
	}

	if len(compactDup) > 0 {
		for key := range compactDup {
			delete(compactSeen, key)
		}
	}
	for key, value := range compactSeen {
		compact[key] = value
	}
	return kingbaseAgentColumnIndex{exact: exact, compact: compact}
}

func normalizeKingbaseAgentCompactKey(raw string) string {
	name := normalizeKingbaseAgentIdent(raw)
	if name == "" {
		return ""
	}
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.Join(strings.Fields(name), "")
	name = strings.ReplaceAll(name, "_", "")
	return name
}

func resolveKingbaseAgentColumnName(name string, index kingbaseAgentColumnIndex) string {
	cleaned := normalizeKingbaseAgentIdent(name)
	if cleaned == "" {
		return name
	}
	lower := strings.ToLower(cleaned)
	if actual, ok := index.exact[lower]; ok {
		return actual
	}
	compact := normalizeKingbaseAgentCompactKey(cleaned)
	if actual, ok := index.compact[compact]; ok {
		return actual
	}
	return cleaned
}

func normalizeKingbaseAgentChangeSetByColumns(changes connection.ChangeSet, columns []string) (connection.ChangeSet, error) {
	index := buildKingbaseAgentColumnIndex(columns)
	if len(index.exact) == 0 && len(index.compact) == 0 {
		return changes, nil
	}

	mapRow := func(row map[string]interface{}) (map[string]interface{}, error) {
		if row == nil {
			return row, nil
		}
		out := make(map[string]interface{}, len(row))
		for key, value := range row {
			nextKey := resolveKingbaseAgentColumnName(key, index)
			if existing, ok := out[nextKey]; ok && !reflect.DeepEqual(existing, value) {
				return nil, fmt.Errorf("duplicate mapped column %q", nextKey)
			}
			out[nextKey] = value
		}
		return out, nil
	}

	next := connection.ChangeSet{
		Inserts: make([]map[string]interface{}, 0, len(changes.Inserts)),
		Updates: make([]connection.UpdateRow, 0, len(changes.Updates)),
		Deletes: make([]map[string]interface{}, 0, len(changes.Deletes)),
	}

	for _, row := range changes.Inserts {
		mapped, err := mapRow(row)
		if err != nil {
			return changes, err
		}
		next.Inserts = append(next.Inserts, mapped)
	}

	for _, upd := range changes.Updates {
		keys, err := mapRow(upd.Keys)
		if err != nil {
			return changes, err
		}
		values, err := mapRow(upd.Values)
		if err != nil {
			return changes, err
		}
		next.Updates = append(next.Updates, connection.UpdateRow{
			Keys:   keys,
			Values: values,
		})
	}

	for _, row := range changes.Deletes {
		mapped, err := mapRow(row)
		if err != nil {
			return changes, err
		}
		next.Deletes = append(next.Deletes, mapped)
	}

	return next, nil
}

func (d *OptionalDriverAgentDB) normalizeKingbaseAgentChangeSet(tableName string, changes connection.ChangeSet) (connection.ChangeSet, error) {
	columns, err := d.GetColumns("", tableName)
	if err != nil {
		return changes, err
	}
	if len(columns) == 0 {
		return changes, nil
	}
	names := make([]string, 0, len(columns))
	for _, col := range columns {
		name := strings.TrimSpace(col.Name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	return normalizeKingbaseAgentChangeSetByColumns(changes, names)
}

func timeoutMsFromContext(ctx context.Context) int64 {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	remaining := time.Until(deadline).Milliseconds()
	if remaining <= 0 {
		return 1
	}
	return remaining
}
