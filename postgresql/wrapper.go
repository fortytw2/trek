package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/fortytw2/lounge"
	"github.com/fortytw2/trek"

	_ "github.com/lib/pq"
)

func init() {
	var _ trek.DB = &Wrapper{}
}

const defaultAdvisoryLock = 42069

type Wrapper struct {
	log lounge.Log

	migrationAdvisoryLock int

	db         *sql.DB
	sqlWrapper *sqlWrapper
}

func NewWrapper(pgDSN string, log lounge.Log) (*Wrapper, error) {
	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	// Set the maximum lifetime of a connection to 4 hours
	// these are just meant to be pretty sensible defaults
	db.SetConnMaxLifetime(time.Hour * 4)
	db.SetConnMaxIdleTime(time.Minute * 16)
	db.SetMaxIdleConns(16)
	db.SetMaxOpenConns(64)

	row := db.QueryRow("SELECT version()")

	var version string
	err = row.Scan(&version)
	if err != nil {
		return nil, err
	}

	log.Infof("postgresql version: %s", version)

	return &Wrapper{
		log:                   log,
		db:                    db,
		migrationAdvisoryLock: defaultAdvisoryLock,
		sqlWrapper:            newSQLWrapper(log, db),
	}, nil
}

func (w *Wrapper) Query(ctx context.Context, scanner trek.ScanFn, query string, args ...interface{}) error {
	return w.sqlWrapper.Query(ctx, scanner, query, args...)
}

func (w *Wrapper) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return w.sqlWrapper.QueryRow(ctx, query, args...)
}

func (w *Wrapper) Exec(ctx context.Context, query string, args ...interface{}) error {
	return w.sqlWrapper.Exec(ctx, query, args...)
}

func (w *Wrapper) Transact(ctx context.Context, txFn trek.TxFn) error {
	tx, err := w.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		w.log.Errorf("error opening sql tx: %s", err)
		return err
	}

	internalWrapper := newSQLWrapper(w.log, tx)
	err = txFn(internalWrapper)
	if err != nil {
		w.log.Errorf("error within tx execution, rolling back: %s", err)

		rollBackErr := tx.Rollback()
		if rollBackErr != nil {
			w.log.Errorf("error rolling back tx execution: %s", err)
			return fmt.Errorf("error in tx and error rolling back tx: %s rollback: %w", err, rollBackErr)
		}

		return fmt.Errorf("error in tx %w", err)
	}

	err = tx.Commit()
	if err != nil {
		w.log.Errorf("error in sql tx: %s", err)
		return err
	}

	return nil
}
