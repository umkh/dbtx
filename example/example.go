package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/umkh/dbtx"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const urlPG = "postgres://postgres:qwerty@localhost:5432/testdb?sslmode=disable"

func main() {
	db, err := sqlx.Connect("postgres", urlPG)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	tx := dbtx.New(db)

	r := NewRepo(tx)

	if err := TransactionSaleBook(context.Background(), tx, r); err != nil {
		log.Println(err)
	}
}

func TransactionSaleBook(ctx context.Context, tx dbtx.TransactionI, repo *Repo) error {
	ctx, err := tx.StartTx(context.Background())
	if err != nil {
		return err
	}
	defer func() { tx.Finish(ctx, err) }()

	userID, err := repo.CreateUser(ctx, "TestUser")
	if err != nil {
		return err
	}

	bookID, err := repo.CreateBook(ctx, "TestBook", 12.500)
	if err != nil {
		return err
	}

	if err := repo.SaleBook(ctx, userID, bookID); err != nil {
		return err
	}

	return nil
}

type Repo struct {
	tx dbtx.TransactionI
}

func NewRepo(tx dbtx.TransactionI) *Repo {
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
