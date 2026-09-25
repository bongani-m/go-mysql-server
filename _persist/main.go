package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dolthub/vitess/go/vt/proto/query"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/persist"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Persistent MySQL server for the example people table. Rows live in a Badger
// directory and are still there after this process exits.
//
//	go run ./_persist
//	mysql --host=127.0.0.1 --port=3306 --user=root mydb --execute="SELECT name, email FROM mytable;"
//
// The HTTP API in _example_webapp connects to this server unchanged.
// Set GMS_DATA to choose the directory. The default is data/gms.

var (
	dbName    = "mydb"
	tableName = "mytable"
	address   = "localhost"
	port      = 3306
)

func main() {
	path := os.Getenv("GMS_DATA")
	if path == "" {
		path = "data/gms"
	}
	store, err := persist.Open(path)
	if err != nil {
		log.Fatalf("open %s: %v", path, err)
	}
	defer store.Close()

	ctx := sql.NewContext(context.Background())
	if err := ensureExample(ctx, store); err != nil {
		log.Fatalf("seed %s.%s: %v", dbName, tableName, err)
	}

	engine := sqle.NewDefault(store)
	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
	}
	s, err := server.NewServer(config, engine, sql.NewContext, persist.NewSessionBuilder(store), nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("persistent MySQL listening on %s, data file %s", config.Address, path)
	if err = s.Start(); err != nil {
		log.Fatal(err)
	}
}

func ensureExample(ctx *sql.Context, store *persist.Store) error {
	if !store.HasDatabase(ctx, dbName) {
		if err := store.CreateDatabase(ctx, dbName); err != nil {
			return err
		}
	}
	db, err := store.Database(ctx, dbName)
	if err != nil {
		return err
	}
	_, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	if err := db.(sql.TableCreator).CreateTable(ctx, tableName, peopleSchema(), sql.Collation_Default, ""); err != nil {
		return err
	}
	table, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("table %s was not created", tableName)
	}

	inserter := table.(sql.InsertableTable).Inserter(ctx)
	inserter.StatementBegin(ctx)
	created := time.Unix(0, 1667304000000001000).UTC()
	for _, person := range seedPeople {
		err := inserter.Insert(ctx, sql.NewRow(
			person.id,
			person.name,
			person.email,
			types.MustJSON(person.phones),
			created,
		))
		if err != nil {
			_ = inserter.DiscardChanges(ctx, err)
			_ = inserter.Close(ctx)
			return err
		}
	}
	if err := inserter.StatementComplete(ctx); err != nil {
		return err
	}
	return inserter.Close(ctx)
}

func peopleSchema() sql.PrimaryKeySchema {
	return sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: tableName, PrimaryKey: true, AutoIncrement: true},
		{Name: "name", Type: types.Text, Nullable: false, Source: tableName},
		{Name: "email", Type: types.Text, Nullable: false, Source: tableName},
		{Name: "phone_numbers", Type: types.JSON, Nullable: false, Source: tableName},
		{Name: "created_at", Type: types.MustCreateDatetimeType(query.Type_DATETIME, 6), Nullable: false, Source: tableName},
	})
}

var seedPeople = []struct {
	id     int64
	name   string
	email  string
	phones string
}{
	{1, "Jane Deo", "janedeo@gmail.com", `["556-565-566","777-777-777"]`},
	{2, "Jane Doe", "jane@doe.com", `[]`},
	{3, "John Doe", "john@doe.com", `["555-555-555"]`},
	{4, "John Doe", "johnalt@doe.com", `[]`},
}
