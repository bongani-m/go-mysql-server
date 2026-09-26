# Example FastAPI app

A JSON CRUD API for `mydb.mytable` on the MySQL server in `../_persist`. Same routes as `../_example_webapp`.

`MYSQL_ADDRS` lists every MySQL address. Reads and writes use any of them. A broken connection tries the next address. Another connection can still see an older copy. Leave `MYSQL_ADDRS` unset to use `MYSQL_HOST`:`MYSQL_PORT`.

## Run

Start the cluster from `go-mysql-server`:

```bash
docker compose -f _persist/compose.yaml up --build
```

Then:

```bash
cd _example_fastapi
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
MYSQL_ADDRS=127.0.0.1:3306,127.0.0.1:3307,127.0.0.1:3308 \
python main.py
```

Open http://localhost:8080/docs.

```bash
curl -s localhost:8080/people?size=2
curl -s -X POST localhost:8080/people \
  -H 'content-type: application/json' \
  -d '{"name":"Ada Lovelace","email":"ada@example.com","phone_numbers":["111-222-333"]}'
```

| Variable | Default |
|----------|---------|
| `MYSQL_ADDRS` | unset. Uses `MYSQL_HOST`:`MYSQL_PORT` |
| `MYSQL_HOST` | `localhost` |
| `MYSQL_PORT` | `3306` |
| `MYSQL_USER` | `root` |
| `MYSQL_PASSWORD` | `dev-only-change-me` |
| `MYSQL_TLS_CA` | `../_persist/certs/ca.crt`. Set to `off` for a plaintext server. |
| `MYSQL_DB` | `mydb` |
| `HTTP_ADDR` | `:8080` |

The table already exists. This app does not create it.

Handler tests use an in-memory store and do not need MySQL:

```bash
pip install pytest httpx
pytest
```
