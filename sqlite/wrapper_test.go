package sqlite

import (
	"context"
	"io/ioutil"
	"sync"
	"testing"

	"github.com/fortytw2/glaive/zlog"
)

func TestDALExecutor(t *testing.T) {
	log := zlog.New(ioutil.Discard)

	db, err := NewMemory(&log, nil)
	if err != nil {
		t.Fatal(err.Error())
	}

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

	db.Stop()
}
