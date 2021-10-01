package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"

	"github.com/fortytw2/glaive/dal"
	"github.com/fortytw2/glaive/dal/sqlite/sqlitemigrate"
	"github.com/fortytw2/glaive/zlog"

	"github.com/fortytw2/glaive/dal/dbtypes"
	"github.com/fortytw2/glaive/random"
	_ "github.com/mattn/go-sqlite3"
)

// shared sqlite3 settings
var stdDSN = "&cache=shared&_vacuum=2&_rt=0&_foreign_keys=1&_journal_mode=WAL"

type SQLiteWrapper struct {
	db  *sql.DB
	log *zlog.Logger

	execChan chan chan *execPayload
	shutdown chan chan struct{}
}

type execPayload struct {
	execID string
	query  string
	args   []interface{}

	isInsert bool
	ID       dbtypes.ID
	err      error
}

func NewMemory(log *zlog.Logger, schema http.FileSystem) (*SQLiteWrapper, error) {
	// TODO: generate a random filename for this, not sure if it matters
	return new(log, "file:test.db?mode=memory"+stdDSN, schema)
}

func New(log *zlog.Logger, fileName string, schema http.FileSystem) (*SQLiteWrapper, error) {
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		log.Warn().Msgf("no '%s' found, initializing a new database", fileName)
	} else {
		log.Warn().Msgf("loading existing '%s'", fileName)
	}

	return new(log, fmt.Sprintf(`file:%s?mode=rwc%s`, fileName, stdDSN), schema)
}

func new(log *zlog.Logger, dsn string, schema http.FileSystem) (*SQLiteWrapper, error) {
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

	log.Info().Msgf("sqlite version: %s", version)

	w := &SQLiteWrapper{
		db:       sqlDB,
		log:      log,
		execChan: make(chan chan *execPayload, 64),
		shutdown: make(chan chan struct{}),
	}

	go w.executor()

	if schema != nil {
		err = sqlitemigrate.Migrate(sqlDB, log, schema)
		if err != nil {
			return nil, err
		}
	} else {
		log.Warn().Msg("no migrations found")
	}

	return w, nil
}

func (w *SQLiteWrapper) Stop() {
	waitForClose := make(chan struct{})

	w.shutdown <- waitForClose

	<-waitForClose
}

func (w *SQLiteWrapper) Query(ctx context.Context, scanner dal.ScanFn, query string, args ...interface{}) error {
	w.log.Trace().Msgf("executing sql statement: %q with args %+v", query, args)
	rows, err := w.db.QueryContext(ctx, query, args...)
	if err != nil {
		w.log.Error().Msgf("got error %q while executing %q with args %+v", err.Error(), query, args)
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
	w.log.Trace().Msgf("executing sql statement: %q with args %+v", query, args)
	return w.db.QueryRowContext(ctx, query, args...)
}

func (w *SQLiteWrapper) Exec(query string, args ...interface{}) error {
	execID := random.ID()

	w.log.Trace().Str("exec_id", execID).Msgf("dal.Exec called %q %v", query, args)
	waitForReply := make(chan *execPayload)

	w.log.Trace().Str("exec_id", execID).Msg("writing response chan to execChan")
	// send it to be executed
	w.execChan <- waitForReply

	w.log.Trace().Str("exec_id", execID).Msg("writing payload to chan")
	waitForReply <- &execPayload{
		execID: execID,
		query:  query,
		args:   args,
	}

	w.log.Trace().Str("exec_id", execID).Msg("waiting for response")
	// wait for the executor to inform us it's complete
	execResp := <-waitForReply

	return execResp.err
}

func (w *SQLiteWrapper) ExecWithReturnID(query string, args ...interface{}) (*dbtypes.ID, error) {
	execID := random.ID()

	w.log.Trace().Str("exec_id", execID).Msgf("dal.Exec called %q %v", query, args)
	waitForReply := make(chan *execPayload)

	w.log.Trace().Str("exec_id", execID).Msg("writing response chan to execChan")
	// send it to be executed
	w.execChan <- waitForReply

	w.log.Trace().Str("exec_id", execID).Msg("writing payload to chan")
	waitForReply <- &execPayload{
		execID:   execID,
		query:    query,
		isInsert: true,
		args:     args,
	}

	w.log.Trace().Str("exec_id", execID).Msg("waiting for response")
	// wait for the executor to inform us it's complete
	execResp := <-waitForReply

	return &execResp.ID, execResp.err
}

func (w *SQLiteWrapper) executor() {
	for {
		select {
		case respChan := <-w.shutdown:
			respChan <- struct{}{}
			return
		case x := <-w.execChan:
			w.log.Trace().Msg("reading request from root channel")
			sqlReq := <-x

			w.log.Trace().Str("exec_id", sqlReq.execID).Msgf("executor running query %q %v", sqlReq.query, sqlReq.args)

			res, err := w.db.Exec(sqlReq.query, sqlReq.args...)
			if err != nil {
				w.log.Debug().Str("exec_id", sqlReq.execID).Msgf("error in sql exec: %s", err)

				x <- &execPayload{
					err: err,
				}
				continue
			}

			if sqlReq.isInsert {
				dbID, err := res.LastInsertId()
				if err != nil {
					w.log.Trace().Str("exec_id", sqlReq.execID).Msgf("executor errored getting last insert id: %q", err)
					continue
				}

				id := dbtypes.ID{Int64: uint64(dbID)}
				w.log.Trace().Str("exec_id", sqlReq.execID).Msg("writing response back to channel")
				x <- &execPayload{
					ID:  id,
					err: err,
				}
			} else {
				w.log.Trace().Str("exec_id", sqlReq.execID).Msg("writing response back to channel")
				x <- &execPayload{
					err: nil,
				}
			}
		}
	}
}

func (w *SQLiteWrapper) DB() *sql.DB {
	return w.db
}
