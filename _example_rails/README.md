# Example Rails app

A small CRUD app for `mydb.mytable` on the MySQL cluster in `../_persist`.

Writes go to n1 (`127.0.0.1:3306`). GET requests round-robin across n2 (`3307`) and n3 (`3308`). The request right after a create, update, or delete reads n1, because the other nodes can lag.

## Run

Start the cluster from `go-mysql-server`:

```bash
docker compose -f _persist/compose.yaml up --build
```

Then:

```bash
cd _example_rails
bundle install
bin/rails server
```

Open http://localhost:3000.

| Variable | Default |
|----------|---------|
| `MYSQL_HOST` | `127.0.0.1` |
| `MYSQL_PORT` | `3306` (writes) |
| `MYSQL_REPLICA_N2_PORT` | `3307` |
| `MYSQL_REPLICA_N3_PORT` | `3308` |
| `MYSQL_USER` | `root` |
| `MYSQL_PASSWORD` | `dev-only-change-me` |
| `MYSQL_TLS_CA` | `../_persist/certs/ca.crt`. Set to `off` for a plaintext server. |
| `MYSQL_DB` | `mydb` |

The table already exists. This app does not create or migrate it.
