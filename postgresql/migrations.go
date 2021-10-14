package postgresql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/fortytw2/lounge"
	"github.com/fortytw2/trek"
)

func (w *Wrapper) ApplyMigrations(log lounge.Log, migrations []trek.Migration) error {
	conn, err := w.db.Conn(context.Background())
	if err != nil {
		return err
	}

	lockedThisSession, err := w.lock(conn)
	if err != nil {
		return err
	}

	if !lockedThisSession {
		log.Infof("migrations lock already held, not running migrations")
		return nil
	}

	err = verifySystemTables(conn)
	if err != nil {
		return err
	}

	latestMigrationName, err := w.getLatestMigrationName()
	if err != nil {
		return err
	}

	if latestMigrationName == "" {
		log.Infof("no previous migrations found, running all")
	} else {
		log.Infof("last migration found: %s", latestMigrationName)
	}

	migrationsToRun := trek.GetMigrationsAfter(migrations, latestMigrationName)

	defer func() {
		var err2 error
		ok, err2 := w.unlock(conn)
		if !ok {
			log.Infof("did not successfully unlock db, you may need to run manually release any locks held on the db")
		}
		if err2 != nil {
			err = fmt.Errorf("%s: %s", err, err2)
		}
	}()

	for i, m := range migrationsToRun {
		log.Infof("running migration: %s", m.Name)
		err := runMigration(i, m.SQL, conn)
		if err != nil {
			return err
		}

		err = recordMigration(m.Name, conn)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Wrapper) getLatestMigrationName() (string, error) {
	row := w.db.QueryRowContext(context.Background(), `
	SELECT name FROM trek_migrations ORDER BY name DESC LIMIT 1
	`)

	var name string
	err := row.Scan(&name)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}

		return "", err
	}

	return name, nil
}

func (w *Wrapper) lock(c *sql.Conn) (bool, error) {
	row := c.QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock($1);", w.migrationAdvisoryLock)

	var locked bool
	err := row.Scan(&locked)
	return locked, err
}

func (w *Wrapper) unlock(c *sql.Conn) (bool, error) {
	row := c.QueryRowContext(context.Background(), "SELECT pg_advisory_unlock($1);", w.migrationAdvisoryLock)

	var locked bool
	err := row.Scan(&locked)
	return locked, err
}

func verifySystemTables(conn *sql.Conn) error {
	_, err := conn.ExecContext(context.Background(), `
	CREATE EXTENSION IF NOT EXISTS pgcrypto;
	`)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(context.Background(), `
	CREATE TABLE IF NOT EXISTS trek_migrations (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		name TEXT NOT NULL UNIQUE
	)
	`)
	if err != nil {
		return err
	}

	return err
}

func runMigration(num int, s string, db *sql.Conn) error {
	_, err := db.ExecContext(context.Background(), s)
	return err
}

func recordMigration(name string, db *sql.Conn) error {
	_, err := db.ExecContext(context.Background(), "INSERT INTO trek_migrations (name) VALUES ($1);", name)
	return err
}
