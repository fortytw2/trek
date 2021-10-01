package trek

import (
	"context"
	"database/sql"
)

// StdlibDB is a minimal interface onto the standard database/sql library
type StdlibDB interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// A ScanFn scans sql.Rows, preventing accidental closure
type ScanFn func(*sql.Rows) error

// A TxFn is used to execute a series of database operations in a transaction
type TxFn func(DB) error

type DB interface {
	Exec(ctx context.Context, query string, args ...interface{}) error
	Query(ctx context.Context, scanner ScanFn, query string, args ...interface{}) error
	QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row

	Transact(ctx context.Context, txFn TxFn) error
}

// Select is a variant of github.com/jmoiron/sqlx.Select() provided to automatically decode query results
// into the output interface{}
func Select(DB, into interface{}, query string, args ...interface{}) error {
	return nil
}
