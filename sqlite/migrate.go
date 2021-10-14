package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/fortytw2/lounge"
	"github.com/fortytw2/trek"
)

func (w *SQLiteWrapper) ApplyMigrations(log lounge.Log, allMigrations []trek.Migration) (err error) {
	err = verifySystemTables(w.db)
	if err != nil {
		return err
	}

	conn, err := w.db.Conn(context.Background())
	if err != nil {
		return err
	}

	ok, err := tryToLock(conn)
	if err != nil {
		return err
	}

	if !ok {
		log.Errorf("unable to migrate database, lock held on table")
		return nil
	}

	defer func() {
		err2 := unlock(conn)
		if err2 != nil {
			err = fmt.Errorf("%s: %s", err, err2)
		}
	}()

	latestName, err := getLatestMigrationName(conn)
	if err != nil {
		log.Errorf(err.Error())
		return err
	}

	migrations := trek.GetMigrationsAfter(allMigrations, latestName)

	for i, m := range migrations {
		log.Infof("running migration %s", m.Name)
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

func getLatestMigrationName(db *sql.Conn) (string, error) {
	row := db.QueryRowContext(context.Background(), `SELECT name FROM migrations ORDER BY name DESC LIMIT 1;`)

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
