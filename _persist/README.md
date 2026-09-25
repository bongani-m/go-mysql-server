# _persist

A MySQL server backed by one Badger directory. Rows are still there after the process exits.

Run these commands from `go-mysql-server`. The example web app in `_example_webapp` connects to this server unchanged.

## One process

Cluster mode stays off unless `GMS_RAFT_ADDR` is set.

```bash
go run ./_persist
mysql --host=127.0.0.1 --port=3306 --user=root mydb --execute="SELECT name, email FROM mytable;"
```

The server listens on `localhost:3306`. Data is stored in `data/gms`. Set `GMS_DATA` to use another directory.

Building needs ICU headers. On this machine:

```bash
CGO_CPPFLAGS="-I/opt/homebrew/opt/icu4c/include" \
CGO_LDFLAGS="-L/opt/homebrew/opt/icu4c/lib" \
go run ./_persist
```

## One container

```bash
docker compose -f _persist/compose.yaml build
docker run --rm -p 3306:3306 -v gms-data:/data -e GMS_MYSQL_HOST=0.0.0.0 gms-persist
```

`GMS_MYSQL_HOST=0.0.0.0` is required for the published port to accept connections. Data inside the container is `/data/gms`.

`docker compose up n1` still starts `n2` and `n3`, because `n1` depends on them and every service sets `GMS_RAFT_ADDR`.

## Three nodes

```bash
docker compose -f _persist/compose.yaml up --build
```

`n1` bootstraps the Raft group. `n2` and `n3` join it. Writes succeed on the leader. The other nodes serve local reads and can lag.

| Node | Host port |
|------|-----------|
| n1   | 3306      |
| n2   | 3307      |
| n3   | 3308      |

```bash
mysql --host=127.0.0.1 --port=3306 --user=root mydb --execute="SELECT name, email FROM mytable;"
mysql --host=127.0.0.1 --port=3307 --user=root mydb --execute="SELECT name, email FROM mytable;"
```

Raft stays on the compose network. Each node keeps `/data` in its own volume.

## Environment

| Variable | Role |
|----------|------|
| `GMS_DATA` | Badger directory. Default `data/gms`. |
| `GMS_MYSQL_HOST` | MySQL bind address. Default `localhost`. Use `0.0.0.0` in containers. |
| `GMS_MYSQL_PORT` | MySQL port. Default `3306`. |
| `GMS_RAFT_ADDR` | Turns cluster mode on and sets the Raft bind address, such as `0.0.0.0:7001`. |
| `GMS_RAFT_ADVERTISE` | Address other nodes dial. Defaults to `GMS_RAFT_ADDR`. |
| `GMS_NODE_ID` | Raft server id. Defaults to the bind address. |
| `GMS_RAFT_PEERS` | `id=host:port` list, comma-separated. |
| `GMS_RAFT_BOOTSTRAP` | `1` on exactly one node, the first time the group starts. |
| `GMS_RAFT_DIR` | Raft log, snapshots, server UUID, and binlog. Default is beside the data directory. |
| `GMS_SERVER_UUID` | Shared by every node. It is the GTID server id in the binlog. |
