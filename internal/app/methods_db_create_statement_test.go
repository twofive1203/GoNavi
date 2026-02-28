package app

import (
	"errors"
	"strings"
	"testing"

	"GoNavi-Wails/internal/connection"
)

type fakeCreateStatementDB struct {
	createSQL  string
	createErr  error
	columns    []connection.ColumnDefinition
	columnsErr error

	createSchema string
	createTable  string
	colsSchema   string
	colsTable    string
}

func (f *fakeCreateStatementDB) Connect(config connection.ConnectionConfig) error { return nil }
func (f *fakeCreateStatementDB) Close() error                                     { return nil }
func (f *fakeCreateStatementDB) Ping() error                                      { return nil }
func (f *fakeCreateStatementDB) Query(query string) ([]map[string]interface{}, []string, error) {
	return nil, nil, nil
}
func (f *fakeCreateStatementDB) Exec(query string) (int64, error)          { return 0, nil }
func (f *fakeCreateStatementDB) GetDatabases() ([]string, error)           { return nil, nil }
func (f *fakeCreateStatementDB) GetTables(dbName string) ([]string, error) { return nil, nil }
func (f *fakeCreateStatementDB) GetCreateStatement(dbName, tableName string) (string, error) {
	f.createSchema = dbName
	f.createTable = tableName
	return f.createSQL, f.createErr
}
func (f *fakeCreateStatementDB) GetColumns(dbName, tableName string) ([]connection.ColumnDefinition, error) {
	f.colsSchema = dbName
	f.colsTable = tableName
	return f.columns, f.columnsErr
}
func (f *fakeCreateStatementDB) GetAllColumns(dbName string) ([]connection.ColumnDefinitionWithTable, error) {
	return nil, nil
}
func (f *fakeCreateStatementDB) GetIndexes(dbName, tableName string) ([]connection.IndexDefinition, error) {
	return nil, nil
}
func (f *fakeCreateStatementDB) GetForeignKeys(dbName, tableName string) ([]connection.ForeignKeyDefinition, error) {
	return nil, nil
}
func (f *fakeCreateStatementDB) GetTriggers(dbName, tableName string) ([]connection.TriggerDefinition, error) {
	return nil, nil
}

func TestResolveDDLDBType_CustomDriverAlias(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		driver string
		want   string
	}{
		{name: "postgresql alias", driver: "postgresql", want: "postgres"},
		{name: "pgx alias", driver: "pgx", want: "postgres"},
		{name: "kingbase8 alias", driver: "kingbase8", want: "kingbase"},
		{name: "kingbase contains alias", driver: "kingbasees", want: "kingbase"},
		{name: "dm alias", driver: "dm8", want: "dameng"},
		{name: "sqlite alias", driver: "sqlite3", want: "sqlite"},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := connection.ConnectionConfig{Type: "custom", Driver: tc.driver}
			if got := resolveDDLDBType(cfg); got != tc.want {
				t.Fatalf("resolveDDLDBType() mismatch, want=%q got=%q", tc.want, got)
			}
		})
	}
}

func TestResolveCreateStatementWithFallback_CustomKingbaseUsesPublicSchema(t *testing.T) {
	t.Parallel()

	dbInst := &fakeCreateStatementDB{
		createSQL: "SHOW CREATE TABLE not directly supported in Kingbase/Postgres via SQL",
		columns: []connection.ColumnDefinition{
			{Name: "id", Type: "bigint", Nullable: "NO", Key: "PRI"},
		},
	}

	ddl, err := resolveCreateStatementWithFallback(dbInst, connection.ConnectionConfig{
		Type:   "custom",
		Driver: "kingbase8",
	}, "demo_db", "orders")
	if err != nil {
		t.Fatalf("resolveCreateStatementWithFallback() unexpected error: %v", err)
	}
	if dbInst.createSchema != "public" || dbInst.colsSchema != "public" {
		t.Fatalf("expected fallback schema public, got create=%q columns=%q", dbInst.createSchema, dbInst.colsSchema)
	}
	if !strings.Contains(ddl, `CREATE TABLE "public"."orders"`) {
		t.Fatalf("expected fallback DDL with public schema, got: %s", ddl)
	}
}

func TestResolveCreateStatementWithFallback_KeepQualifiedSchema(t *testing.T) {
	t.Parallel()

	dbInst := &fakeCreateStatementDB{
		createSQL: "-- SHOW CREATE TABLE not fully supported for PostgreSQL in this MVP.",
		columns: []connection.ColumnDefinition{
			{Name: "id", Type: "integer", Nullable: "NO", Key: "PRI"},
		},
	}

	ddl, err := resolveCreateStatementWithFallback(dbInst, connection.ConnectionConfig{
		Type:   "custom",
		Driver: "postgresql",
	}, "demo_db", "sales.orders")
	if err != nil {
		t.Fatalf("resolveCreateStatementWithFallback() unexpected error: %v", err)
	}
	if dbInst.createSchema != "sales" || dbInst.colsSchema != "sales" {
		t.Fatalf("expected schema sales, got create=%q columns=%q", dbInst.createSchema, dbInst.colsSchema)
	}
	if !strings.Contains(ddl, `CREATE TABLE "sales"."orders"`) {
		t.Fatalf("expected fallback DDL with sales schema, got: %s", ddl)
	}
}

func TestResolveCreateStatementWithFallback_NoFallbackForMySQL(t *testing.T) {
	t.Parallel()

	dbInst := &fakeCreateStatementDB{
		createSQL:  "SHOW CREATE TABLE not directly supported in Kingbase/Postgres via SQL",
		columnsErr: errors.New("should not be called"),
	}

	ddl, err := resolveCreateStatementWithFallback(dbInst, connection.ConnectionConfig{
		Type: "mysql",
	}, "demo_db", "orders")
	if err != nil {
		t.Fatalf("resolveCreateStatementWithFallback() unexpected error: %v", err)
	}
	if ddl != dbInst.createSQL {
		t.Fatalf("expected original ddl for mysql, got: %s", ddl)
	}
	if dbInst.colsTable != "" {
		t.Fatalf("mysql path should not call GetColumns, got table=%q", dbInst.colsTable)
	}
}

func TestResolveCreateStatementWithFallback_FallbackWhenCreateStatementError(t *testing.T) {
	t.Parallel()

	dbInst := &fakeCreateStatementDB{
		createErr: errors.New("statement unsupported"),
		columns: []connection.ColumnDefinition{
			{Name: "id", Type: "bigint", Nullable: "NO", Key: "PRI"},
		},
	}

	ddl, err := resolveCreateStatementWithFallback(dbInst, connection.ConnectionConfig{
		Type: "postgres",
	}, "demo_db", "orders")
	if err != nil {
		t.Fatalf("resolveCreateStatementWithFallback() unexpected error: %v", err)
	}
	if !strings.Contains(ddl, `CREATE TABLE "public"."orders"`) {
		t.Fatalf("expected fallback DDL for postgres error path, got: %s", ddl)
	}
}
