package main

import (
	"context"
	"log"
	"umkh/dbtx"
	"umkh/dbtx/example/repo"

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

	r := repo.New(tx)

	if err := TransactionSaleBook(context.Background(), tx, r); err != nil {
		log.Println(err)
	}
}

func TransactionSaleBook(ctx context.Context, tx dbtx.TransactionI, repo *repo.Repo) error {
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
