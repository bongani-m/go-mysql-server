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

// Command examplewebapp is a CRUD API for the MySQL server in ../_persist.
// Start that server first, then run this program:
//
//	go run .
//
//	curl -s localhost:8080/people?size=2
//	curl -s 'localhost:8080/people?name=Jane'
//
// Writes use MYSQL_HOST and MYSQL_PORT (default localhost:3306). To read from
// the other nodes in _persist/compose.yaml:
//
//	MYSQL_READ_ADDRS=127.0.0.1:3307,127.0.0.1:3308 go run .
//
// Those nodes can lag the leader. Leave MYSQL_READ_ADDRS unset to read and
// write the same server.
package main

import (
	"context"
	"database/sql"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

func main() {
	primaryAddr := net.JoinHostPort(env("MYSQL_HOST", "localhost"), env("MYSQL_PORT", "3306"))
	primary, err := openMySQL(primaryAddr)
	if err != nil {
		log.Fatalf("connect to %s: %v", primaryAddr, err)
	}
	defer primary.Close()

	var replicas []*sql.DB
	replicaAddrs := splitAddrs(os.Getenv("MYSQL_READ_ADDRS"))
	for _, addr := range replicaAddrs {
		db, err := openMySQL(addr)
		if err != nil {
			log.Fatalf("connect to read replica %s: %v", addr, err)
		}
		defer db.Close()
		replicas = append(replicas, db)
	}
	if len(replicaAddrs) == 0 {
		log.Printf("MySQL %s", primaryAddr)
	} else {
		log.Printf("MySQL writes %s, reads %s", primaryAddr, strings.Join(replicaAddrs, ", "))
	}

	addr := env("HTTP_ADDR", ":8080")
	log.Printf("API listening on %s", addr)
	if err := http.ListenAndServe(addr, NewHandler(NewMySQLStore(primary, replicas...))); err != nil {
		log.Fatal(err)
	}
}

func openMySQL(addr string) (*sql.DB, error) {
	db, err := sql.Open("mysql", mysqlDSN(addr))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func mysqlDSN(addr string) string {
	cfg := mysql.Config{
		User:                 env("MYSQL_USER", "root"),
		Passwd:               os.Getenv("MYSQL_PASSWORD"),
		Net:                  "tcp",
		Addr:                 addr,
		DBName:               env("MYSQL_DB", "mydb"),
		ParseTime:            true,
		Loc:                  time.UTC,
		AllowNativePasswords: true,
	}
	return cfg.FormatDSN()
}

// splitAddrs parses a comma-separated host:port list. Empty input is no replicas.
func splitAddrs(raw string) []string {
	if raw == "" {
		return nil
	}
	var addrs []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			addrs = append(addrs, part)
		}
	}
	return addrs
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
