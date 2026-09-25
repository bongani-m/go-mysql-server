// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command examplewebapp is a CRUD API for the in-memory MySQL server
// in ../_example. Start that server first (users disabled, database mydb),
// then run this program:
//
//	go run .
//
//	curl -s localhost:8080/people?size=2
//	curl -s 'localhost:8080/people?name=Jane'
package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", mysqlDSN())
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("connect to example MySQL server: %v (start it with `go run .` in _example)", err)
	}

	addr := env("HTTP_ADDR", ":8080")
	log.Printf("API listening on %s", addr)
	if err := http.ListenAndServe(addr, NewHandler(NewMySQLStore(db))); err != nil {
		log.Fatal(err)
	}
}

func mysqlDSN() string {
	cfg := mysql.Config{
		User:                 env("MYSQL_USER", "root"),
		Passwd:               os.Getenv("MYSQL_PASSWORD"),
		Net:                  "tcp",
		Addr:                 env("MYSQL_HOST", "localhost") + ":" + env("MYSQL_PORT", "3306"),
		DBName:               env("MYSQL_DB", "mydb"),
		ParseTime:            true,
		Loc:                  time.UTC,
		AllowNativePasswords: true,
	}
	return cfg.FormatDSN()
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
