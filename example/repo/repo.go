package repo

import (
	"context"
	"fmt"
	"time"
	transaction "umkh/dbtx"
)

type Repo struct {
	tx transaction.TransactionI
}

func New(tx transaction.TransactionI) *Repo {
	return &Repo{tx: tx}
}

func (r *Repo) CreateUser(ctx context.Context, name string) (id int64, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*2)
	defer cancel()

	client := r.tx.GetClient(ctx)

	query := `INSERT INTO users (name) VALUES ($1) RETURNING id`
	if err = client.QueryRowx(query, name).Scan(&id); err != nil {
		err = fmt.Errorf("%s", err.Error())
		return
	}

	return
}

func (r *Repo) CreateBook(ctx context.Context, name string, price float32) (id int64, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*2)
	defer cancel()

	client := r.tx.GetClient(ctx)

	query := `INSERT INTO books (name, price) VALUES ($1, $2) RETURNING id`
	if err = client.QueryRowxContext(ctx, query, name, price).Scan(&id); err != nil {
		err = fmt.Errorf("%s", err.Error())
		return
	}

	return
}

func (r *Repo) SaleBook(ctx context.Context, userID, bookID int64) (err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*2)
	defer cancel()

	client := r.tx.GetClient(ctx)

	query := `INSERT INTO users_books (user_id, book_id) VALUES ($1, $2)`
	if err = client.QueryRowx(query, userID, bookID).Err(); err != nil {
		err = fmt.Errorf("%s", err.Error())
		return
	}

	return
}
