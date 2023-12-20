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
	// "github.com/golang-migrate/migrate/v4"
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
	conns    []*sql.Conn
	dbs      []*sql.DB
	isLocked []atomic.Bool

	// Open and WithInstance need to guarantee that config is never nil
	config []*Config
}

func prepend[T any](slice []T, elems ...T) []T {
	return append(elems, slice...)
}

func WithInstance(instances []*sql.DB, config []*Config) (database.Driver, error) {
	fmt.Println("WithInstance() is called")
	ctx := context.Background()
	conns := make([]*sql.Conn, 0, len(instances))
	isLocked := make([]atomic.Bool, len(instances))

	for i, instance := range instances {
		if err := instance.Ping(); err != nil {
			return nil, err
		}

		conn, err := instance.Conn(ctx)
		if err != nil {
			return nil, err
		}

		if config == nil {
			return nil, ErrNilConfig
		}

		if err := conn.PingContext(ctx); err != nil {
			return nil, err
		}

		if config[i].DatabaseName == "" {
			query := `SELECT CURRENT_DATABASE()`
			var databaseName string
			if err := conn.QueryRowContext(ctx, query).Scan(&databaseName); err != nil {
				return nil, &database.Error{OrigErr: err, Query: []byte(query)}
			}

			if len(databaseName) == 0 {
				return nil, ErrNoDatabaseName
			}

			config[i].DatabaseName = databaseName
		}

		if config[i].SchemaName == "" {
			query := `SELECT CURRENT_SCHEMA()`
			var schemaName sql.NullString
			if err := conn.QueryRowContext(ctx, query).Scan(&schemaName); err != nil {
				return nil, &database.Error{OrigErr: err, Query: []byte(query)}
			}

			if !schemaName.Valid {
				return nil, ErrNoSchema
			}

			config[i].SchemaName = schemaName.String
		}

		if len(config[i].MigrationsTable) == 0 {
			config[i].MigrationsTable = DefaultMigrationsTable
		}

		fmt.Println(config[i].DatabaseName, "databaseName")

		config[i].migrationsSchemaName = config[i].SchemaName
		config[i].migrationsTableName = config[i].MigrationsTable
		if config[i].MigrationsTableQuoted {
			re := regexp.MustCompile(`"(.*?)"`)
			result := re.FindAllStringSubmatch(config[i].MigrationsTable, -1)
			config[i].migrationsTableName = result[len(result)-1][1]
			if len(result) == 2 {
				config[i].migrationsSchemaName = result[0][1]
			} else if len(result) > 2 {
				return nil, fmt.Errorf("\"%s\" MigrationsTable contains too many dot characters", config[i].MigrationsTable)
			}
		}

		conns = append(conns, conn)
	}

	px := &PgEdge{
		conns:    conns,
		config:   config,
		dbs:      instances,
		isLocked: isLocked,
	}

	fmt.Println(&px.conns, "px.conns")

	if err := px.ensureVersionTable(); err != nil {
		return nil, err
	}
	fmt.Println(&px.conns, "px.conns")

	return px, nil
}

func (p *PgEdge) Open(dbURL string) (database.Driver, error) {
	fmt.Println("Open() is called")

	fmt.Println(dbURL, "dbURL")

	// As PgEdge uses the postgres protocol, and 'postgres' is already a registered database, we need to replace the
	// connect prefix, with the actual protocol, so that the library can differentiate between the implementations
	re := regexp.MustCompile("^(pgedge)")
	// connectString := re.ReplaceAllString(migrate.FilterCustomQuery(purl).String(), "postgres")
	connectString := re.ReplaceAllString(dbURL, "postgres")

	decodedString, err := url.QueryUnescape(connectString)
	if err != nil {
		return nil, err
	}

	connectionStrings := strings.Split(decodedString, ",")

	fmt.Println(connectionStrings[0], "connectionStrings")
	fmt.Println("------------------------------------")
	fmt.Println(connectionStrings[1], "connectionStrings")
	fmt.Println("------------------------------------")
	fmt.Println(connectionStrings[2], "connectionStrings")

	dbs := make([]*sql.DB, 0, len(connectionStrings))
	configs := make([]*Config, 0, len(connectionStrings))
	fmt.Println(connectionStrings, "connectionStrings", len(connectionStrings))

	for _, dbURL := range connectionStrings {
		db, err := sql.Open("postgres", dbURL)
		if err != nil {
			return nil, err
		}

		purl, err := url.Parse(dbURL)
		if err != nil {
			return nil, err
		}

		fmt.Println(purl, "purl")

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

		dbs = append(dbs, db)
		configs = append(configs, &Config{
			MigrationsTable:       migrationsTable,
			MigrationsTableQuoted: migrationsTableQuoted,
			MultiStatementEnabled: multiStatementEnabled,
			DatabaseName:          purl.Path,
			SchemaName:            purl.Query().Get("x-schema-name"),
			StatementTimeout:      time.Duration(statementTimeout) * time.Millisecond,
			MultiStatementMaxSize: multiStatementMaxSize,
		})

		fmt.Println(purl.Path, "migrationsTable")

		fmt.Println(p, "px")
	}
	fmt.Println(dbs, "dbs", len(dbs))
	fmt.Println(configs, "configs", len(configs))
	px, err := WithInstance(dbs, configs)

	if err != nil {
		return nil, err
	}

	fmt.Printf("%+v\n", px)
	fmt.Println("-------------------px--------------------")
	return px, nil
}

func (p *PgEdge) Close() error {
	fmt.Println("Close() is called")
	var errs []error
	for i, conn := range p.conns {
		connErr := conn.Close()
		var dbErr error
		if p.dbs[i] != nil {
			dbErr = p.dbs[i].Close()
		}

		if connErr != nil || dbErr != nil {
			errs = append(errs, fmt.Errorf("conn: %v, db: %v", connErr, dbErr))
		}
	}
	if len(errs) > 0 {
		return multierror.Append(errs[0], errs[1:]...)
	}
	return nil
}

// Locking is done manually with a separate lock table. Implementing advisory locks in PgEdge is being discussed
func (p *PgEdge) Lock() error {
	fmt.Println("Lock() is called")
	var errs []error
	fmt.Println(p.conns, "p.conns")
	fmt.Println(p.config, "p.config")
	if len(p.isLocked) == 0 {
		return fmt.Errorf("p.isLocked is empty")
	}
	for i, conn := range p.conns {
		// if p.config[i].DatabaseName == "" || p.config[i].migrationsSchemaName == "" || p.config[i].migrationsTableName == "" {
		// return fmt.Errorf("database name, schema name, or table name is empty")
		fmt.Println(p.config[i].DatabaseName, p.config[i].migrationsSchemaName, p.config[i].migrationsTableName, "p.config[i].DatabaseName, p.config[i].migrationsSchemaName, p.config[i].migrationsTableName")
		// }
		err := database.CasRestoreOnErr(&p.isLocked[i], false, true, database.ErrLocked, func() error {
			fmt.Println(p.config[i].DatabaseName, "p.config[i].DatabaseName")
			aid, err := database.GenerateAdvisoryLockId(p.config[i].DatabaseName, p.config[i].migrationsSchemaName, p.config[i].migrationsTableName)
			if err != nil {
				return err
			}

			// This will wait indefinitely until the lock can be acquired.
			query := `SELECT pg_advisory_lock($1)`
			if _, err := conn.ExecContext(context.Background(), query, aid); err != nil {
				return &database.Error{OrigErr: err, Err: "try lock failed", Query: []byte(query)}
			}

			return nil
		})
		fmt.Println(err, "err")
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return multierror.Append(errs[0], errs[1:]...)
	}
	return nil
}

// Locking is done manually with a separate lock table. Implementing advisory locks in PgEdge is being discussed
func (p *PgEdge) Unlock() error {
	fmt.Println("Unlock() is called")
	var errs []error
	for i, conn := range p.conns {
		if p.config[i].DatabaseName == "" || p.config[i].migrationsSchemaName == "" || p.config[i].migrationsTableName == "" {
			return fmt.Errorf("database name, schema name, or table name is empty")
		}
		fmt.Print(p.config[i].DatabaseName, "tablename")
		err := database.CasRestoreOnErr(&p.isLocked[i], true, false, database.ErrNotLocked, func() error {
			aid, err := database.GenerateAdvisoryLockId(p.config[i].DatabaseName, p.config[i].migrationsSchemaName, p.config[i].migrationsTableName)
			if err != nil {
				return err
			}

			query := `SELECT pg_advisory_unlock($1)`
			if _, err := conn.ExecContext(context.Background(), query, aid); err != nil {
				return &database.Error{OrigErr: err, Err: "try unlock failed", Query: []byte(query)}
			}
			return nil
		})
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return multierror.Append(errs[0], errs[1:]...)
	}
	return nil
}

func (p *PgEdge) Run(migration io.Reader) error {
	fmt.Println("Run() is called")
	fmt.Println(p.conns, "px.conns", len(p.conns))
	var errs []error
	migr, err := io.ReadAll(migration)
	for i, conn := range p.conns {
		fmt.Println(migration, "migration",i)
		if p.config[i].MultiStatementEnabled {
			var err error
			if e := multistmt.Parse(migration, multiStmtDelimiter, p.config[i].MultiStatementMaxSize, func(m []byte) bool {
				if err = p.runStatement(m, conn, p.config[i]); err != nil {
					return false
				}
				return true
			}); e != nil {
				errs = append(errs, e)
			}
			if err != nil {
				errs = append(errs, err)
			}
		} else {
			
				fmt.Println("migr",migr)
				err = p.runStatement(migr, conn, p.config[i])
				if err != nil {
					errs = append(errs, err)
				}
			}
	}
	if len(errs) > 0 {
		return multierror.Append(errs[0], errs[1:]...)
	}
	return nil
}

func (p *PgEdge) runStatement(statement []byte, conn *sql.Conn, config *Config) error {
	fmt.Println("runStatement() is called")
	ctx := context.Background()
	if config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.StatementTimeout)
		defer cancel()
	}

	query := string(statement)
	if strings.TrimSpace(query) == "" {
		fmt.Println("empty query")
		return nil
	}

	query = filterQuery(query)
	fmt.Println(query, "finalquery")
	if _, err := conn.ExecContext(ctx, query); err != nil {
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
	line = uint(runesCount(sel, '\n') + 1)
	col = uint(pos - 1 - runesLastIndex(sel, '\n'))
	return line, col, true
}

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
	// script = fmt.Sprintf("SELECT spock.replicate_ddl('%s');", script)

	return script
}

func (p *PgEdge) SetVersion(version int, dirty bool) error {
	fmt.Println("SetVersion() is called")
	var errs []error
	for i, conn := range p.conns {
		tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			errs = append(errs, &database.Error{OrigErr: err, Err: "transaction start failed"})
			continue
		}

		query := `TRUNCATE ` + pq.QuoteIdentifier(p.config[i].migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config[i].migrationsTableName)
		if _, err := tx.Exec(query); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = multierror.Append(err, errRollback)
			}
			errs = append(errs, &database.Error{OrigErr: err, Query: []byte(query)})
			continue
		}

		// Also re-write the schema version for nil dirty versions to prevent
		// empty schema version for failed down migration on the first migration
		// See: https://github.com/golang-migrate/migrate/issues/330
		if version >= 0 || (version == database.NilVersion && dirty) {
			query = `INSERT INTO ` + pq.QuoteIdentifier(p.config[i].migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config[i].migrationsTableName) + ` (version, dirty) VALUES ($1, $2)`
			if _, err := tx.Exec(query, version, dirty); err != nil {
				if errRollback := tx.Rollback(); errRollback != nil {
					err = multierror.Append(err, errRollback)
				}
				errs = append(errs, &database.Error{OrigErr: err, Query: []byte(query)})
				continue
			}
		}

		if err := tx.Commit(); err != nil {
			errs = append(errs, &database.Error{OrigErr: err, Err: "transaction commit failed"})
		}
	}
	if len(errs) > 0 {
		return multierror.Append(errs[0], errs[1:]...)
	}
	return nil
}
func (p *PgEdge) Version() (version int, dirty bool, err error) {
	fmt.Println("SetVersion() is called")
	var errs []error
	for i, conn := range p.conns {
		query := `SELECT version, dirty FROM ` + pq.QuoteIdentifier(p.config[i].migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config[i].migrationsTableName) + ` LIMIT 1`
		err = conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
		switch {
		case err == sql.ErrNoRows:
			return database.NilVersion, false, nil

		case err != nil:
			if e, ok := err.(*pq.Error); ok {
				if e.Code.Name() == "undefined_table" {
					return database.NilVersion, false, nil
				}
			}
			errs = append(errs, &database.Error{OrigErr: err, Query: []byte(query)})
			continue

		default:
			return version, dirty, nil
		}
	}
	if len(errs) > 0 {
		return 0, false, multierror.Append(errs[0], errs[1:]...)
	}
	return version, dirty, nil
}

func (p *PgEdge) Drop() (err error) {
	fmt.Println("Drop() is called")
	var errs []error
	for _, db := range p.dbs {
		query := `SELECT table_schema, table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema','spock') AND table_schema NOT LIKE 'pg_toast%';`
		rows, err := db.Query(query)
		if err != nil {
			errs = append(errs, &database.Error{OrigErr: err, Query: []byte(query)})
			continue
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
				errs = append(errs, err)
				continue
			}
			tables = append(tables, struct {
				Schema string
				Table  string
			}{Schema: schema, Table: table})
		}
		if err := rows.Err(); err != nil {
			errs = append(errs, &database.Error{OrigErr: err, Query: []byte(query)})
			continue
		}

		// Drop each table
		for _, t := range tables {
			query = `select spock.replicate_ddl('DROP TABLE IF EXISTS ` + t.Schema + `.` + t.Table + `;')` //CASCADE
			fmt.Println(query, "query", t.Table, "schema", t.Schema)                                       // log the schema name here
			if _, err := db.Exec(query); err != nil {
				errs = append(errs, &database.Error{OrigErr: err, Query: []byte(query)})
			}
		}
	}
	if len(errs) > 0 {
		return multierror.Append(errs[0], errs[1:]...)
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
	var errs []error
	for i, conn := range p.conns {
		fmt.Println(p.config[i].migrationsSchemaName, p.config[i].migrationsTableName, p.config[i].migrationsSchemaName, "p.config[i].migrationsSchemaName, p.config[i].migrationsTableName,p.config[i].migrationsSchemaName")
		query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`
		row := conn.QueryRowContext(context.Background(), query, p.config[i].migrationsSchemaName, p.config[i].migrationsTableName)

		var count int
		err = row.Scan(&count)
		if err != nil {
			errs = append(errs, &database.Error{OrigErr: err, Query: []byte(query)})
			continue
		}

		if count == 1 {
			continue
		}

		query = `CREATE TABLE IF NOT EXISTS ` + pq.QuoteIdentifier(p.config[i].migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config[i].migrationsTableName) + ` (version bigint not null primary key, dirty boolean not null)`
		if _, err = conn.ExecContext(context.Background(), query); err != nil {
			errs = append(errs, &database.Error{OrigErr: err, Query: []byte(query)})
		}
	}
	if len(errs) > 0 {
		return multierror.Append(errs[0], errs[1:]...)
	}
	return nil
}
