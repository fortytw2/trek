package postgresql_test

import (
	"context"
	"embed"
	"testing"

	"github.com/fortytw2/lounge"
	"github.com/fortytw2/trek/postgresql/pgmigrate"
	"github.com/fortytw2/trek/postgresql/pgtest"
)

//go:embed testdata/schema1
var schema embed.FS

func TestPostgreSQL(t *testing.T) {
	db := pgtest.NewDB(t, lounge.NewDefaultLog(), schema, pgmigrate.WithPattern("testdata/schema1/*"))
	defer db.Shutdown()

	row := db.QueryRow(context.TODO(), "SELECT count(*) FROM monkeys;")

	var count int
	err := row.Scan(&count)
	if err != nil {
		t.Error(err)
	}

	if count != 0 {
		t.Error("did not get zero monkeys")
	}
}
