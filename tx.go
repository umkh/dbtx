package dbtx

import (
	"context"
	"database/sql"
	"log"

	"github.com/jmoiron/sqlx"
)

type txk string

const TxKey txk = "DBTX_KEY"

type TransactionI interface {
	GetClient(ctx context.Context) SQLDB
	StartTx(context.Context) (context.Context, error)
	FinishTx(ctx context.Context, err error) error
}

type Transaction struct {
	db *sqlx.DB
}

func New(db *sqlx.DB) *Transaction {
	return &Transaction{db: db}
}

func (t *Transaction) StartTx(ctx context.Context) (context.Context, error) {
	tx, err := t.db.Beginx()
	if err != nil {
		return ctx, err
	}

	ctx = context.WithValue(ctx, TxKey, tx)
	return ctx, nil
}

func (t *Transaction) GetClient(ctx context.Context) SQLDB {
	tx, ok := ctx.Value(TxKey).(*sqlx.Tx)
	if !ok {
		return t.db
	}

	return tx
}

func (t *Transaction) FinishTx(ctx context.Context, err error) error {
	tx, ok := ctx.Value(TxKey).(*sqlx.Tx)
	if !ok {
		return ErrCTXKeyNotFound
	}

	if err != nil {
		if rollBackErr := tx.Rollback(); rollBackErr != nil {
			log.Printf("transaction rollback error")
			return err
		}
		log.Printf("rolled back")
		return nil
	}

	if commitErr := tx.Commit(); commitErr != nil {
		log.Printf("transaction commit error")
		return commitErr
	}

	return nil
}

type SQLDB interface {
	BindNamed(query string, arg interface{}) (string, []interface{}, error)
	DriverName() string
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	MustExec(query string, args ...interface{}) sql.Result
	MustExecContext(ctx context.Context, query string, args ...interface{}) sql.Result
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
	PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)
	Preparex(query string) (*sqlx.Stmt, error)
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	Rebind(query string) string
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}
