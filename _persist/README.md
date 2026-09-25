# _persist

A MySQL server backed by one Badger directory. Rows are still there after the process exits.

Run these commands from `go-mysql-server`. The example web app in `_example_webapp` uses this server for writes. Set `MYSQL_READ_ADDRS` there to read from the other compose nodes.

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

`n1` bootstraps the Raft group. `n2` and `n3` join it. Writes succeed on the leader. The other nodes are read-only until one of them is elected, and they can lag. A client that writes to a follower gets the read-only error. `FLUSH BINARY LOGS` on the leader rolls every node's binlog together. Any node can stream that binlog; the GTID stream is the same after a promotion.

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

The example API sends writes to `n1` and reads to `n2` and `n3`:

```bash
cd _example_webapp
MYSQL_READ_ADDRS=127.0.0.1:3307,127.0.0.1:3308 go run .
```

Leave `MYSQL_READ_ADDRS` unset to use only `localhost:3306`.

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
| `GMS_BINLOG_MAX_SIZE` | Rolls the binlog after a transaction crosses this many bytes. Default is 1 GiB. `FLUSH BINARY LOGS` rolls it immediately. |
| `GMS_SOURCE_HOST` | Upstream MySQL host. When set, the current primary replicates from it. Set the same value on every node. |
| `GMS_SOURCE_PORT` | Upstream MySQL port. Default `3306`. |
| `GMS_SOURCE_USER` | Upstream MySQL user. |
| `GMS_SOURCE_PASSWORD` | Upstream MySQL password. It stays in a local file beside the Raft directory. A promoted node can dial only if it already has this password. |

`CHANGE REPLICATION SOURCE TO`, `START REPLICA`, and `STOP REPLICA` configure that upstream job. The primary is the only node that connects. After a failover the new primary continues from the GTID stored with the applied rows. Replication filters are unsupported.
