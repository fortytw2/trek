package pgmigrate

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sort"

	"github.com/fortytw2/trek"
)

// this is a random number we're using so there's
// a very slim chance of conflicting with anything actual in an application
const defaultMigrationAdvisoryLock = 17828

type Options struct {
	Patterns     []string
	AdvisoryLock int
}

var DefaultOptions = &Options{
	Patterns:     []string{"schema/*.sql"},
	AdvisoryLock: defaultMigrationAdvisoryLock,
}

type OptionFn func(o *Options) error

func WithPattern(patterns ...string) OptionFn {
	return func(o *Options) error {
		o.Patterns = patterns
		return nil
	}
}

func Migrate(db *sql.DB, log trek.Logf, schema fs.FS, opts ...OptionFn) (err error) {
	mo := DefaultOptions

	for _, opt := range opts {
		err := opt(mo)
		if err != nil {
			return err
		}
	}

	migrations, err := getMigrations(schema, mo.Patterns...)
	if err != nil {
		return err
	}

	err = verifySystemTables(db)
	if err != nil {
		return err
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}

	ok, err := tryToLock(conn, mo.AdvisoryLock)
	if err != nil {
		return err
	}

	if !ok {
		log("not running migrations, lock %s is held on table")
		return nil
	}

	defer func() {
		var err2 error
		ok, err2 = unlock(conn, mo.AdvisoryLock)
		if !ok {
			log("did not successfully unlock db, you may need to run 'SELECT pg_advisory_unlock(%d);`", mo.AdvisoryLock)
		}
		if err2 != nil {
			err = fmt.Errorf("%s: %s", err, err2)
		}
	}()

	count, err := countMigrations(conn)
	if err != nil {
		return err
	}

	for i, m := range migrations {
		// skip running ones we've clearly already ran
		if count > 0 {
			count--
			continue
		}

		log("running migration: %s", m.name)
		err := runMigration(i, m.sql, conn)
		if err != nil {
			return err
		}

		err = recordMigration(m.name, conn)
		if err != nil {
			return err
		}
	}

	return nil
}

type migration struct {
	sql  string
	name string
}

func getMigrations(schema fs.FS, patterns ...string) ([]*migration, error) {
	// text/template/helper.go#parseFS
	var filenames []string
	for _, pattern := range patterns {
		list, err := fs.Glob(schema, pattern)
		if err != nil {
			return nil, err
		}
		if len(list) == 0 {
			return nil, fmt.Errorf("template: pattern matches no files: %#q", pattern)
		}
		filenames = append(filenames, list...)
	}

	var m []*migration
	for _, f := range filenames {
		body, err := fs.ReadFile(schema, f)
		if err != nil {
			return nil, err
		}

		m = append(m, &migration{
			name: f,
			sql:  string(body),
		})
	}

	sort.Slice(m, func(i, j int) bool {
		return m[i].name < m[j].name
	})

	return m, nil
}

func verifySystemTables(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE EXTENSION IF NOT EXISTS pgcrypto;
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS trek_migrations (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		name TEXT NOT NULL UNIQUE
	);`)
	if err != nil {
		return err
	}

	return err
}

func countMigrations(db *sql.Conn) (int, error) {
	row := db.QueryRowContext(context.Background(), `
	SELECT count(*) FROM trek_migrations;
	`)

	var count int
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func runMigration(num int, s string, db *sql.Conn) error {
	_, err := db.ExecContext(context.Background(), s)
	return err
}

func recordMigration(name string, db *sql.Conn) error {
	_, err := db.ExecContext(context.Background(), "INSERT INTO trek_migrations (name) VALUES ($1);", name)
	return err
}

func tryToLock(db *sql.Conn, lockID int) (bool, error) {
	row := db.QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock($1);", lockID)

	var locked bool
	err := row.Scan(&locked)
	return locked, err
}

func unlock(db *sql.Conn, lockID int) (bool, error) {
	row := db.QueryRowContext(context.Background(), "SELECT pg_advisory_unlock($1);", lockID)

	var locked bool
	err := row.Scan(&locked)
	return locked, err
}
