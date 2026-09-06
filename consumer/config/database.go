package database

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

func GetDSN() string {
	op := "database.config.loadConfig: "

	host, exists := os.LookupEnv("TRANSACTION_DB_HOST")
	if !exists {
		host = "localhost"
	}

	rawPort, exists := os.LookupEnv("TRANSACTION_DB_PORT")
	if !exists {
		rawPort = "4444"
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		log.Fatal(op + "Cannot parse port variable")
	}

	user, exists := os.LookupEnv("TRANSACTION_DB_USER")
	if !exists {
		user = "postgres"
	}

	password, exists := os.LookupEnv("TRANSACTION_DB_PASSWORD")
	if !exists {
		password = "postgres"
	}

	db, exists := os.LookupEnv("TRANSACTION_DB_NAME")
	if !exists {
		db = "transactions"
	}

	return fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d sslmode=disable",
		user, password, db, host, port)
}
