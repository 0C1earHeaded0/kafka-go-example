package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	database "kafka-go-example-consumer/config"

	"github.com/lib/pq"
)

type Storage struct {
	db *sql.DB
}

func NewStorage() (*Storage, error) {
	const op = "storage.postgresql.NewStorage"

	connString := database.GetDSN()

	fmt.Println(connString)

	c, err := pq.NewConnector(connString)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	db := sql.OpenDB(c)

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	stmt, err := db.Prepare(`
	CREATE TABLE IF NOT EXISTS metrics(
		id VARCHAR PRIMARY KEY,
		value JSONB NOT NULL
	)`)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	_, err = stmt.Exec()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) Save(ctx context.Context, id string, jsonToSave string) (string, error) {
	const op = "storage.postgresql.Save"

	if !json.Valid([]byte(jsonToSave)) {
		return "", fmt.Errorf("%s: %s: %s", op, "Invalid JSON data.", jsonToSave)
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return "", fmt.Errorf("Transaction begin error. %s: %w", op, err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
	INSERT INTO metrics(id,value) 
	VALUES ($1,$2) 
	ON CONFLICT (id) DO NOTHING;
	`)
	if err != nil {
		return "", fmt.Errorf("Preparation error. %s: %w", op, err)
	}

	stmt.ExecContext(ctx, id, jsonToSave)

	err = tx.Commit()
	if err != nil {
		return "", fmt.Errorf("Transaction commit error. %s: %w", op, err)
	}

	return jsonToSave, nil
}
