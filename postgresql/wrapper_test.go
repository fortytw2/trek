package postgresql_test

import (
	"context"
	"embed"
	"os"
	"sync"
	"testing"

	"github.com/fortytw2/lounge"
	"github.com/fortytw2/trek"
	"github.com/fortytw2/trek/postgresql/pgtest"
)

//go:embed testdata/schema1
var schema embed.FS

func TestPostgreSQL(t *testing.T) {
	l := lounge.NewDefaultLog(lounge.WithOutput(os.Stderr))

	db := pgtest.NewDB(t, l)
	defer db.Shutdown()

	migrations, err := trek.GetMigrations(schema)
	if err != nil {
		t.Fatal(err)
	}

	err = trek.Migrate(db, l, migrations)
	if err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow(context.TODO(), "SELECT count(*) FROM monkeys;")

	var count int
	err = row.Scan(&count)
	if err != nil {
		t.Error(err)
	}

	if count != 0 {
		t.Error("did not get zero monkeys")
	}
}

func TestPostgreSQLConcurrentMigrations(t *testing.T) {
	l := lounge.NewDefaultLog(lounge.WithOutput(os.Stderr))

	db := pgtest.NewDB(t, l)
	defer db.Shutdown()

	migrations, err := trek.GetMigrations(schema)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = trek.Migrate(db, l, migrations)
			if err != nil {
				t.Log(err)
			}

		}()
	}

	wg.Wait()

	row := db.QueryRow(context.TODO(), "SELECT count(*) FROM monkeys;")

	var count int
	err = row.Scan(&count)
	if err != nil {
		t.Error(err)
	}

	if count != 0 {
		t.Error("did not get zero monkeys")
	}
}
