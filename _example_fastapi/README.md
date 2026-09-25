# Example FastAPI app

A JSON CRUD API for `mydb.mytable` on the MySQL server in `../_persist`. Same routes as `../_example_webapp`.

Writes go to `MYSQL_HOST`:`MYSQL_PORT` (default `localhost:3306`). When `MYSQL_READ_ADDRS` is set, list and get round-robin across those addresses. The other nodes can lag the leader.

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
MYSQL_PASSWORD=dev-only-change-me \
MYSQL_READ_ADDRS=127.0.0.1:3307,127.0.0.1:3308 \
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
| `MYSQL_HOST` | `localhost` |
| `MYSQL_PORT` | `3306` (writes) |
| `MYSQL_READ_ADDRS` | empty (read the write server) |
| `MYSQL_USER` | `root` |
| `MYSQL_PASSWORD` | empty |
| `MYSQL_TLS_CA` | `../_persist/certs/server.crt`. Set to `off` for a plaintext server. |
| `MYSQL_DB` | `mydb` |
| `HTTP_ADDR` | `:8080` |

The table already exists. This app does not create it.

Handler tests use an in-memory store and do not need MySQL:

```bash
pip install pytest httpx
pytest
```
