package sqlitemigrate

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/fortytw2/lounge"
	"github.com/shurcooL/httpfs/vfsutil"
)

func Migrate(db *sql.DB, log lounge.Log, schema http.FileSystem) (err error) {
	migrations, err := getMigrations(schema)
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

	ok, err := tryToLock(conn)
	if err != nil {
		return err
	}

	if !ok {
		log.Error().Msg("unable to migrate database, lock held on table")
		return nil
	}

	defer func() {
		err2 := unlock(conn)
		if err2 != nil {
			err = fmt.Errorf("%s: %s", err, err2)
		}
	}()

	count, err := countMigrations(conn)
	if err != nil {
		log.Error().Msg(err.Error())
		return err
	}

	for i, m := range migrations {
		// skip running ones we've clearly already ran
		if count > 0 {
			count--
			continue
		}

		migrationSpl := strings.Split(m.name, "_")
		if len(migrationSpl) != 2 {
			// the one case it's ok to bail
			log.Fatal().Msgf("invalid database migration naming: %s", m.name)
		}

		log.Info().Msgf("running database migration %d: %s", i, migrationSpl[1])
		err := runMigration(i, m.sql, conn)
		if err != nil {
			log.Error().Msg(err.Error())
			return err
		}

		err = recordMigration(m.name, conn)
		if err != nil {
			log.Error().Msg(err.Error())
			return err
		}
	}

	return nil
}

type migration struct {
	sql  string
	name string
}

func getMigrations(schema http.FileSystem) ([]*migration, error) {
	files, err := vfsutil.ReadDir(schema, "/")
	if err != nil {
		return nil, err
	}

	var m []*migration
	for _, f := range files {
		body, err := vfsutil.ReadFile(schema, f.Name())
		if err != nil {
			return nil, err
		}

		m = append(m, &migration{
			name: f.Name(),
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
	CREATE TABLE IF NOT EXISTS migration_locks (
		locked INTEGER PRIMARY KEY,
		created_at INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP
	);`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS migrations (
		id INTEGER PRIMARY KEY,
		created_at INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,
		name TEXT NOT NULL UNIQUE
	);`)
	return err
}

func countMigrations(db *sql.Conn) (int, error) {
	row := db.QueryRowContext(context.Background(), `SELECT count(*) FROM migrations;`)

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

func tryToLock(db *sql.Conn) (bool, error) {
	row := db.QueryRowContext(context.Background(), "SELECT count(*) FROM migration_locks;")

	var lockedInt int
	err := row.Scan(&lockedInt)
	if err != nil {
		return false, err
	}

	locked := lockedInt > 0
	if !locked {
		_, err := db.ExecContext(context.Background(), `INSERT INTO migration_locks (locked) VALUES (1);`)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func unlock(db *sql.Conn) error {
	_, err := db.ExecContext(context.Background(), "DELETE FROM migration_locks;")
	return err
}

func recordMigration(name string, db *sql.Conn) error {
	_, err := db.ExecContext(context.Background(), "INSERT INTO migrations (name) VALUES ($1);", name)
	return err
}
