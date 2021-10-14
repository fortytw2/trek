package pgtest

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/fortytw2/dockertest"
	"github.com/fortytw2/lounge"
	"github.com/fortytw2/trek/postgresql"
)

type DB struct {
	*postgresql.Wrapper
	container *dockertest.Container
}

func NewDB(t *testing.T, log lounge.Log) *DB {
	existingDSN, useEnvDB := os.LookupEnv("POSTGRES_DSN")

	// may be nil
	var container *dockertest.Container

	var db *postgresql.Wrapper
	var err error
	if useEnvDB {
		db, err = postgresql.NewWrapper(existingDSN, log)
		if err != nil {
			t.Fatalf("%s", err.Error())
		}
	} else {
		log.Infof("using docker to spawn a test database")
		container, err = dockertest.RunContainer("postgres:13-alpine", "5432", func(addr string) error {
			db, err := sql.Open("postgres", "postgres://postgres:postgres@"+addr+"?sslmode=disable")
			if err != nil {
				return err
			}
			return db.Ping()
		}, "-e", "POSTGRES_PASSWORD=postgres")
		if err != nil {
			t.Fatalf("%s", err.Error())
		}

		db, err = postgresql.NewWrapper("postgres://postgres:postgres@"+container.Addr+"?sslmode=disable", log)
		if err != nil {
			t.Fatalf("%s", err.Error())
		}
	}

	return &DB{
		Wrapper:   db,
		container: container,
	}
}

func (db *DB) Shutdown() {
	if db.container != nil {
		db.container.Shutdown()
	}
}

func (db *DB) Truncate(t *testing.T) {
	err := db.Exec(context.Background(), `
	DO
	$func$
	BEGIN
		EXECUTE
		(SELECT 'TRUNCATE TABLE ' || string_agg(oid::regclass::text, ', ') || ' CASCADE'
			FROM pg_class
			WHERE relkind = 'r' -- table relations
			AND relnamespace = 'public'::regnamespace
		);
	END
	$func$;`)
	if err != nil {
		t.Fatalf("could not truncate db: %s", err)
	}
}
