package database

import (
	"database/sql"
	"fmt"
)

const driver = "postgres"

func ConnectDatabase(host string, port int, user, password, name string) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, name)
	db, err := sql.Open(driver, connStr)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}
