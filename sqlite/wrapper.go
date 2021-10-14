package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/fortytw2/lounge"
	"github.com/fortytw2/trek"

	_ "github.com/mattn/go-sqlite3"
)

const asyncIDLength = 12

// shared sqlite3 settings
var stdDSN = "&cache=shared&_vacuum=2&_rt=0&_foreign_keys=1&_journal_mode=WAL"

type SQLiteWrapper struct {
	db  *sql.DB
	log lounge.Log

	execChan chan chan *execPayload
	shutdown chan chan struct{}
}

type execPayload struct {
	execID string
	query  string
	args   []interface{}

	scanFn trek.ScanFn

	err error
}

func NewMemory(log lounge.Log) (*SQLiteWrapper, error) {
	// TODO: generate a random filename for this, not sure if it matters
	return new(log, "file:test.db?mode=memory"+stdDSN)
}

func New(log lounge.Log, fileName string) (*SQLiteWrapper, error) {
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		log.Infof("no '%s' found, initializing a new database", fileName)
	} else {
		log.Infof("loading existing '%s'", fileName)
	}

	return new(log, fmt.Sprintf(`file:%s?mode=rwc%s`, fileName, stdDSN))
}

func new(log lounge.Log, dsn string) (*SQLiteWrapper, error) {
	sqlDB, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	err = sqlDB.Ping()
	if err != nil {
		return nil, err
	}

	sqlDB.SetConnMaxLifetime(0)
	sqlDB.SetMaxIdleConns(25)
	sqlDB.SetMaxOpenConns(100)

	row := sqlDB.QueryRow("SELECT sqlite_version()")

	var version string
	err = row.Scan(&version)
	if err != nil {
		return nil, err
	}

	log.Infof("sqlite version: %s", version)

	w := &SQLiteWrapper{
		db:       sqlDB,
		log:      log,
		execChan: make(chan chan *execPayload, 64),
		shutdown: make(chan chan struct{}),
	}

	go w.executor()

	return w, nil
}

func (w *SQLiteWrapper) Close() {
	waitForClose := make(chan struct{})

	w.shutdown <- waitForClose

	<-waitForClose
}

func (w *SQLiteWrapper) Query(ctx context.Context, scanner trek.ScanFn, query string, args ...interface{}) error {
	if isWriteQuery(query) {
		return w.internalWriteQuery(scanner, query, args)
	}

	w.log.Debugf("executing sql statement: %q with args %+v", query, args)
	rows, err := w.db.QueryContext(ctx, query, args...)
	if err != nil {
		w.log.Errorf("got error %q while executing %q with args %+v", err.Error(), query, args)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		err = scanner(rows)
		if err != nil {
			return err
		}
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	return nil
}

func (w *SQLiteWrapper) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	w.log.Debugf("executing sql statement: %q with args %+v", query, args)
	return w.db.QueryRowContext(ctx, query, args...)
}

func (w *SQLiteWrapper) Exec(query string, args ...interface{}) error {
	execID := randomString(asyncIDLength)

	w.log.With(map[string]string{"exec_id": execID}).Debugf("dal.Exec called %q %v", query, args)
	waitForReply := make(chan *execPayload)

	w.log.With(map[string]string{"exec_id": execID}).Debugf("writing response chan to execChan")
	// send it to be executed
	w.execChan <- waitForReply

	w.log.With(map[string]string{"exec_id": execID}).Debugf("writing payload to chan")
	waitForReply <- &execPayload{
		execID: execID,
		query:  query,
		args:   args,
	}

	w.log.With(map[string]string{"exec_id": execID}).Debugf("waiting for response")
	// wait for the executor to inform us it's complete
	execResp := <-waitForReply

	return execResp.err
}

func (w *SQLiteWrapper) internalWriteQuery(scanFn trek.ScanFn, query string, args ...interface{}) error {
	execID := randomString(asyncIDLength)

	w.log.With(map[string]string{"exec_id": execID}).Debugf("dal.Exec called %q %v", query, args)
	waitForReply := make(chan *execPayload)

	w.log.With(map[string]string{"exec_id": execID}).Debugf("writing response chan to execChan")
	// send it to be executed
	w.execChan <- waitForReply

	w.log.With(map[string]string{"exec_id": execID}).Debugf("writing payload to chan")
	waitForReply <- &execPayload{
		execID: execID,
		query:  query,
		scanFn: scanFn,
		args:   args,
	}

	w.log.With(map[string]string{"exec_id": execID}).Debugf("waiting for response")
	// wait for the executor to inform us it's complete
	execResp := <-waitForReply

	return execResp.err
}

func isWriteQuery(query string) bool {
	lowerQ := strings.ToLower(query)
	return strings.Contains(lowerQ, "insert") || strings.Contains(lowerQ, "update")
}

func (w *SQLiteWrapper) executor() {
	for {
		select {
		case respChan := <-w.shutdown:
			respChan <- struct{}{}
			return
		case x := <-w.execChan:
			w.log.Debugf("reading request from root channel")
			sqlReq := <-x

			w.log.With(map[string]string{"exec_id": sqlReq.execID}).Debugf("executor running query %q %v", sqlReq.query, sqlReq.args)
			if sqlReq.scanFn != nil {
				rows, err := w.db.Query(sqlReq.query, sqlReq.args...)
				if err != nil {
					w.log.With(map[string]string{"exec_id": sqlReq.execID}).Debugf("error in sql query: %s", err)

					x <- &execPayload{
						err: err,
					}
					continue
				}

				w.log.With(map[string]string{"exec_id": sqlReq.execID}).Debugf("scanning update rows: %s", err)
				for rows.Next() {
					err := sqlReq.scanFn(rows)
					if err != nil {
						w.log.With(map[string]string{"exec_id": sqlReq.execID}).Debugf("writing response back to channel")
						x <- &execPayload{
							err: err,
						}
						break
					}
				}
			} else {
				_, err := w.db.Exec(sqlReq.query, sqlReq.args...)
				if err != nil {
					w.log.With(map[string]string{"exec_id": sqlReq.execID}).Debugf("error in sql exec: %s", err)

					x <- &execPayload{
						err: err,
					}
					continue
				}
				w.log.With(map[string]string{"exec_id": sqlReq.execID}).Debugf("writing response back to channel")
				x <- &execPayload{
					err: nil,
				}
			}
		}
	}
}
