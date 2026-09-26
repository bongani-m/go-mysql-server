# Example Rails app

A small CRUD app for `mydb.mytable` on the MySQL cluster in `../_persist`.

`MYSQL_ADDRS` lists every MySQL address. Reads and writes use any of them. A browser session stays on one node, so the page after a write sees that write. A broken connection moves the session to the next address. A write whose connection breaks before a result comes back is sent again. Another connection can still see an older copy.

## Run

Start the cluster from `go-mysql-server`:

```bash
docker compose -f _persist/compose.yaml up --build
```

Then:

```bash
cd _example_rails
bundle install
MYSQL_ADDRS=127.0.0.1:3306,127.0.0.1:3307,127.0.0.1:3308 \
bin/rails server
```

Open http://localhost:3000.

| Variable | Default |
|----------|---------|
| `MYSQL_ADDRS` | unset. Uses `MYSQL_HOST`:`MYSQL_PORT` |
| `MYSQL_HOST` | `127.0.0.1` |
| `MYSQL_PORT` | `3306` |
| `MYSQL_USER` | `root` |
| `MYSQL_PASSWORD` | `dev-only-change-me` |
| `MYSQL_TLS_CA` | `../_persist/certs/ca.crt`. Set to `off` for a plaintext server. |
| `MYSQL_DB` | `mydb` |

The table already exists. This app does not create or migrate it.
