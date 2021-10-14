package sqlite

import (
	"context"
	"embed"
	"os"
	"sync"
	"testing"

	"github.com/fortytw2/lounge"
	"github.com/fortytw2/trek"
)

//go:embed testdata/schema1
var schema embed.FS

func TestSQLiteMigrations(t *testing.T) {
	log := lounge.NewDefaultLog(lounge.WithOutput(os.Stderr), lounge.WithDebugEnabled())
	db, err := NewMemory(log)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()

	migrations, err := trek.GetMigrations(schema)
	if err != nil {
		t.Fatal(err.Error())
	}

	err = trek.Migrate(db, log, migrations)
	if err != nil {
		t.Fatal(err.Error())
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

func TestSQLiteSerializedExecutor(t *testing.T) {
	log := lounge.NewDefaultLog(lounge.WithOutput(os.Stderr), lounge.WithDebugEnabled())

	db, err := NewMemory(log)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()

	var wg sync.WaitGroup

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		err := db.Exec(`CREATE TABLE t1 (id integer primary key autoincrement)`)
		if err != nil {
			panic(err)
		}
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		err := db.Exec(`CREATE TABLE t2 (id integer primary key autoincrement)`)
		if err != nil {
			panic(err)
		}
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		err := db.Exec(`CREATE TABLE t3 (id integer primary key autoincrement)`)
		if err != nil {
			panic(err)
		}
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		err := db.Exec(`CREATE TABLE t4 (id integer primary key autoincrement)`)
		if err != nil {
			panic(err)
		}
	}(&wg)

	wg.Wait()

	row := db.QueryRow(context.TODO(), `SELECT count(*) FROM t1`)

	var count int
	err = row.Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
}
