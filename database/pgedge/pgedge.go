package pgedge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"

	// "regexp"
	"strconv"
	"time"

	// "github.com/cenkalti/backoff/v4"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"github.com/hashicorp/go-multierror"

	// "github.com/jackc/pgconn"
	// "github.com/jackc/pgerrcode"
	"github.com/lib/pq"
	"go.uber.org/atomic"
)

var (
	DefaultMaxRetryInterval      = time.Second * 15
	DefaultMaxRetryElapsedTime   = time.Second * 30
	DefaultMaxRetries            = 10
	DefaultMigrationsTable       = "migrations"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB
	multiStmtDelimiter           = []byte(";")
	// DefaultLockTable           = "migrations_locks"
)

var (
	ErrNilConfig          = errors.New("no config")
	ErrNoDatabaseName     = errors.New("no database name")
	ErrNoSchema           = errors.New("no schema")
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
)

func init() {
	db := PgEdge{}
	database.Register("pgedge", &db)
}

type Config struct {
	MigrationsTable       string
	MigrationsTableQuoted bool
	MultiStatementEnabled bool
	DatabaseName          string
	SchemaName            string
	migrationsSchemaName  string
	migrationsTableName   string
	StatementTimeout      time.Duration
	MultiStatementMaxSize int
}

type PgEdge struct {
	conn     *sql.Conn
	db       *sql.DB
	isLocked atomic.Bool

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithConnection(ctx context.Context, conn *sql.Conn, config *Config) (*PgEdge, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.PingContext(ctx); err != nil {
		return nil, err
	}

	if config.DatabaseName == "" {
		query := `SELECT CURRENT_DATABASE()`
		var databaseName string
		if err := conn.QueryRowContext(ctx, query).Scan(&databaseName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if len(databaseName) == 0 {
			return nil, ErrNoDatabaseName
		}

		config.DatabaseName = databaseName
	}

	if config.SchemaName == "" {
		query := `SELECT CURRENT_SCHEMA()`
		var schemaName sql.NullString
		if err := conn.QueryRowContext(ctx, query).Scan(&schemaName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if !schemaName.Valid {
			return nil, ErrNoSchema
		}

		config.SchemaName = schemaName.String
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	config.migrationsSchemaName = config.SchemaName
	config.migrationsTableName = config.MigrationsTable
	if config.MigrationsTableQuoted {
		re := regexp.MustCompile(`"(.*?)"`)
		result := re.FindAllStringSubmatch(config.MigrationsTable, -1)
		config.migrationsTableName = result[len(result)-1][1]
		if len(result) == 2 {
			config.migrationsSchemaName = result[0][1]
		} else if len(result) > 2 {
			return nil, fmt.Errorf("\"%s\" MigrationsTable contains too many dot characters", config.MigrationsTable)
		}
	}

	px := &PgEdge{
		conn:   conn,
		config: config,
	}

	if err := px.ensureVersionTable(); err != nil {
		return nil, err
	}

	return px, nil
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	ctx := context.Background()

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	conn, err := instance.Conn(ctx)
	if err != nil {
		return nil, err
	}

	px, err := WithConnection(ctx, conn, config)
	if err != nil {
		return nil, err
	}
	px.db = instance
	return px, nil
}

func (p *PgEdge) Open(dbURL string) (database.Driver, error) {
	purl, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}

	// As PgEdge uses the postgres protocol, and 'postgres' is already a registered database, we need to replace the
	// connect prefix, with the actual protocol, so that the library can differentiate between the implementations
	re := regexp.MustCompile("^(pgedge)")
	connectString := re.ReplaceAllString(migrate.FilterCustomQuery(purl).String(), "postgres")

	db, err := sql.Open("postgres", connectString)
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	migrationsTableQuoted := false
	if s := purl.Query().Get("x-migrations-table-quoted"); len(s) > 0 {
		migrationsTableQuoted, err = strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse option x-migrations-table-quoted: %w", err)
		}
	}
	if (len(migrationsTable) > 0) && (migrationsTableQuoted) && ((migrationsTable[0] != '"') || (migrationsTable[len(migrationsTable)-1] != '"')) {
		return nil, fmt.Errorf("x-migrations-table must be quoted (for instance '\"migrate\".\"schema_migrations\"') when x-migrations-table-quoted is enabled, current value is: %s", migrationsTable)
	}

	statementTimeoutString := purl.Query().Get("x-statement-timeout")
	statementTimeout := 0
	if statementTimeoutString != "" {
		statementTimeout, err = strconv.Atoi(statementTimeoutString)
		if err != nil {
			return nil, err
		}
	}

	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if s := purl.Query().Get("x-multi-statement-max-size"); len(s) > 0 {
		multiStatementMaxSize, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		if multiStatementMaxSize <= 0 {
			multiStatementMaxSize = DefaultMultiStatementMaxSize
		}
	}

	multiStatementEnabled := false
	if s := purl.Query().Get("x-multi-statement"); len(s) > 0 {
		multiStatementEnabled, err = strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse option x-multi-statement: %w", err)
		}
	}

	px, err := WithInstance(db, &Config{
		DatabaseName:          purl.Path,
		MigrationsTable:       migrationsTable,
		MigrationsTableQuoted: migrationsTableQuoted,
		StatementTimeout:      time.Duration(statementTimeout) * time.Millisecond,
		MultiStatementEnabled: multiStatementEnabled,
		MultiStatementMaxSize: multiStatementMaxSize,
	})

	if err != nil {
		return nil, err
	}

	return px, nil
}

func (p *PgEdge) Close() error {
	connErr := p.conn.Close()
	var dbErr error
	if p.db != nil {
		dbErr = p.db.Close()
	}

	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

// Locking is done manually with a separate lock table. Implementing advisory locks in PgEdge is being discussed
func (p *PgEdge) Lock() error {
	return database.CasRestoreOnErr(&p.isLocked, false, true, database.ErrLocked, func() error {
		aid, err := database.GenerateAdvisoryLockId(p.config.DatabaseName, p.config.migrationsSchemaName, p.config.migrationsTableName)
		if err != nil {
			return err
		}

		// This will wait indefinitely until the lock can be acquired.
		query := `SELECT pg_advisory_lock($1)`
		if _, err := p.conn.ExecContext(context.Background(), query, aid); err != nil {
			return &database.Error{OrigErr: err, Err: "try lock failed", Query: []byte(query)}
		}

		return nil
	})
}

// Locking is done manually with a separate lock table. Implementing advisory locks in PgEdge is being discussed
func (p *PgEdge) Unlock() error {
	return database.CasRestoreOnErr(&p.isLocked, true, false, database.ErrNotLocked, func() error {
		aid, err := database.GenerateAdvisoryLockId(p.config.DatabaseName, p.config.migrationsSchemaName, p.config.migrationsTableName)
		if err != nil {
			return err
		}

		query := `SELECT pg_advisory_unlock($1)`
		if _, err := p.conn.ExecContext(context.Background(), query, aid); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		return nil
	})
}

func (p *PgEdge) Run(migration io.Reader) error {
	fmt.Println("Run() is called")
	if p.config.MultiStatementEnabled {
		var err error
		if e := multistmt.Parse(migration, multiStmtDelimiter, p.config.MultiStatementMaxSize, func(m []byte) bool {
			if err = p.runStatement(m); err != nil {
				return false
			}
			return true
		}); e != nil {
			return e
		}
		return err
	}
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	return p.runStatement(migr)
}

func (p *PgEdge) runStatement(statement []byte) error {
	ctx := context.Background()
	fmt.Println("runStatement() is called")
	if p.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.config.StatementTimeout)
		defer cancel()
	}

	query := string(statement)
	if strings.TrimSpace(query) == "" {
		return nil
	}

	query = filterQuery(query)
	fmt.Println(query, "finalquery")
	if _, err := p.conn.ExecContext(ctx, query); err != nil {
		if pgErr, ok := err.(*pq.Error); ok {
			var line uint
			var col uint
			var lineColOK bool
			if pgErr.Position != "" {
				if pos, err := strconv.ParseUint(pgErr.Position, 10, 64); err == nil {
					line, col, lineColOK = computeLineFromPos(query, int(pos))
				}
			}
			message := fmt.Sprintf("migration failed: %s", pgErr.Message)
			if lineColOK {
				message = fmt.Sprintf("%s (column %d)", message, col)
			}
			if pgErr.Detail != "" {
				message = fmt.Sprintf("%s, %s", message, pgErr.Detail)
			}
			return database.Error{OrigErr: err, Err: message, Query: statement, Line: line}
		}
		return database.Error{OrigErr: err, Err: "migration failed", Query: statement}
	}
	return nil
}

func computeLineFromPos(s string, pos int) (line uint, col uint, ok bool) {
	// replace crlf with lf
	s = strings.Replace(s, "\r\n", "\n", -1)
	// pg docs: pos uses index 1 for the first character, and positions are measured in characters not bytes
	runes := []rune(s)
	if pos > len(runes) {
		return 0, 0, false
	}
	sel := runes[:pos]
	line = uint(runesCount(sel, newLine) + 1)
	col = uint(pos - 1 - runesLastIndex(sel, newLine))
	return line, col, true
}

const newLine = '\n'

func runesCount(input []rune, target rune) int {
	var count int
	for _, r := range input {
		if r == target {
			count++
		}
	}
	return count
}

func runesLastIndex(input []rune, target rune) int {
	for i := len(input) - 1; i >= 0; i-- {
		if input[i] == target {
			return i
		}
	}
	return -1
}

func filterQuery(query string) string {
	// Remove single-line comments
	re := regexp.MustCompile(`(?m)--.*$`)
	script := re.ReplaceAllString(query, "")

	// Remove multi-line comments
	re = regexp.MustCompile(`(?s)/\*.*?\*/`)
	script = re.ReplaceAllString(script, "")

	// Remove empty lines
	re = regexp.MustCompile(`(?m)^\s*$[\r\n]*|[\r\n]+\s+\z`)
	script = re.ReplaceAllString(script, "")

	// Wrap all the ddl statements
	script = fmt.Sprintf("SELECT spock.replicate_ddl('%s');", script)

	return script
}

func (p *PgEdge) SetVersion(version int, dirty bool) error {
	tx, err := p.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := `TRUNCATE ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName)
	if _, err := tx.Exec(query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query = `INSERT INTO ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName) + ` (version, dirty) VALUES ($1, $2)`
		if _, err := tx.Exec(query, version, dirty); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = multierror.Append(err, errRollback)
			}
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

func (p *PgEdge) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName) + ` LIMIT 1`
	err = p.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		if e, ok := err.(*pq.Error); ok {
			if e.Code.Name() == "undefined_table" {
				return database.NilVersion, false, nil
			}
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func (c *PgEdge) Drop() (err error) {
	fmt.Println("Drop() is called")
	query := `SELECT table_schema, table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema','spock') AND table_schema NOT LIKE 'pg_toast%';`
	rows, err := c.db.Query(query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := rows.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	// Store schema and table names
	tables := make([]struct {
		Schema string
		Table  string
	}, 0)

	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return err
		}
		tables = append(tables, struct {
			Schema string
			Table  string
		}{Schema: schema, Table: table})
	}
	if err := rows.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Drop each table
	for _, t := range tables {
		query = `select spock.replicate_ddl('DROP TABLE IF EXISTS ` + t.Schema + `.` + t.Table + `;')` //CASCADE
		fmt.Println(query, "query", t.Table, "schema", t.Schema)                                       // log the schema name here
		if _, err := c.db.Exec(query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database
func (p *PgEdge) ensureVersionTable() (err error) {
	fmt.Println("ensureVersionTable() is called")
	if err = p.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := p.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	// This block checks whether the `MigrationsTable` already exists. This is useful because it allows read only postgres
	// users to also check the current version of the schema. Previously, even if `MigrationsTable` existed, the
	// `CREATE TABLE IF NOT EXISTS...` query would fail because the user does not have the CREATE permission.
	// Taken from https://github.com/mattes/migrate/blob/master/database/postgres/postgres.go#L258
	query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`
	row := p.conn.QueryRowContext(context.Background(), query, p.config.migrationsSchemaName, p.config.migrationsTableName)

	var count int
	err = row.Scan(&count)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if count == 1 {
		return nil
	}

	query = `CREATE TABLE IF NOT EXISTS ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName) + ` (version bigint not null primary key, dirty boolean not null)`
	if _, err = p.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}
