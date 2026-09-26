# _persist

A MySQL server backed by one Badger directory. Rows are still there after the process exits.

Run these commands from `go-mysql-server`. The example web app in `_example_webapp` uses this server for writes. Set `MYSQL_READ_ADDRS` there to read from the other compose nodes.

## One process

Cluster mode stays off unless `GMS_RAFT_ADDR` is set.

```bash
GMS_BOOTSTRAP_PASSWORD=secret go run ./_persist
mysql --host=127.0.0.1 --port=3306 --user=root --password=secret mydb --execute="SELECT name, email FROM mytable;"
```

The first boot creates one `mysql_native_password` account. `GMS_BOOTSTRAP_PASSWORD` is required then and is not saved for later boots; the account lives in the data directory. The default user is `root` and the default host is `%`. A follower that has not received the account yet rejects every login.

The server listens on `localhost:3306`. Data is stored in `data/gms`. Set `GMS_DATA` to use another directory. Leave `GMS_TLS_CERT` and `GMS_TLS_KEY` unset to stay on plaintext. Set both to require TLS.

Building needs ICU headers. On this machine:

```bash
CGO_CPPFLAGS="-I/opt/homebrew/opt/icu4c/include" \
CGO_LDFLAGS="-L/opt/homebrew/opt/icu4c/lib" \
go run ./_persist
```

## One container

```bash
docker compose -f _persist/compose.yaml build
docker run --rm -p 3306:3306 -v gms-data:/data \
  -e GMS_MYSQL_HOST=0.0.0.0 \
  -e GMS_BOOTSTRAP_PASSWORD=secret \
  gms-persist
```

`GMS_MYSQL_HOST=0.0.0.0` is required for the published port to accept connections. Data inside the container is `/data/gms`.

`docker compose up n1` still starts `n2` and `n3`, because `n1` depends on them and every service sets `GMS_RAFT_ADDR`.

## Local testing

Create the test CA and server certificate before the first `up`. They are not committed. Compose mounts `_persist/certs` at `/certs`, and Raft reads `ca.crt`, `server.crt`, and `server.key` from there. Without `ca.crt` each node logs `open /certs/ca.crt: no such file or directory` and exits, and Compose restarts it. The server certificate is signed by that CA, carries `serverAuth` and `clientAuth`, and names the addresses clients dial: `127.0.0.1` from the host, the node names, and the private addresses Compose assigns. A stack that is already restarting picks the files up after `docker compose -f _persist/compose.yaml restart`.

```bash
mkdir -p _persist/certs
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout _persist/certs/ca.key \
  -out _persist/certs/ca.crt \
  -days 365 \
  -subj "/CN=gms-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl req -newkey rsa:2048 -nodes \
  -keyout _persist/certs/server.key \
  -out _persist/certs/server.csr \
  -subj "/CN=gms" \
  -addext "subjectAltName=DNS:localhost,DNS:n1,DNS:n2,DNS:n3,IP:127.0.0.1,IP:10.116.0.2,IP:10.116.0.3,IP:10.116.0.4" \
  -addext "extendedKeyUsage=serverAuth,clientAuth"
openssl x509 -req -in _persist/certs/server.csr \
  -CA _persist/certs/ca.crt -CAkey _persist/certs/ca.key -CAcreateserial \
  -out _persist/certs/server.crt -days 365 \
  -copy_extensions copy
docker compose -f _persist/compose.yaml up --build
```

| Node | Host port |
|------|-----------|
| n1   | 3306      |
| n2   | 3307      |
| n3   | 3308      |

Check the leader, then a follower:

```bash
mysql --host=127.0.0.1 --port=3306 --user=root --password=dev-only-change-me \
  --ssl-mode=REQUIRED --ssl-ca=_persist/certs/ca.crt \
  mydb --execute="SELECT name, email FROM mytable;"
mysql --host=127.0.0.1 --port=3307 --user=root --password=dev-only-change-me \
  --ssl-mode=REQUIRED --ssl-ca=_persist/certs/ca.crt \
  mydb --execute="SELECT name, email FROM mytable;"
```

## Three nodes

Compose sets `GMS_BOOTSTRAP_PASSWORD` to `dev-only-change-me` unless you override it. That value is used only when a node has no accounts yet. TLS is required on the published MySQL ports. Each node binds Raft and MySQL to its address on the `vpc` network: `10.116.0.2`, `10.116.0.3`, and `10.116.0.4`. Raft on port 7001 and the write-forward port 7002 stay on that network and require mutual TLS. The certificate above is the client certificate as well as the server certificate.

A volume created before those addresses still has the old Raft peer list (`n1:7001` and so on). Remove it before the first start on this plan: `docker compose -f _persist/compose.yaml down -v`.

The same addresses, with host networking and a Cloud Firewall, are what a DigitalOcean deployment uses. See [droplets/README.md](droplets/README.md).

`n1` bootstraps the Raft group. `n2` and `n3` join it. Writes succeed on the leader. A follower forwards a write, or a whole explicit transaction, to the current leader, then waits until that commit is applied locally before the next read on the same connection. Other connections can still see an older copy. `FLUSH BINARY LOGS` on the leader rolls every node's binlog together. Any node can stream that binlog; the GTID stream is the same after a promotion.

Raft stays on `10.116.0.0/24` and is not published to the host. Each node keeps `/data` in its own volume.

The example API sends writes to `n1` and reads to `n2` and `n3`:

```bash
cd _example_webapp
MYSQL_READ_ADDRS=127.0.0.1:3307,127.0.0.1:3308 \
go run .
```

The example clients trust `_persist/certs/ca.crt` unless `MYSQL_TLS_CA` is set. `MYSQL_TLS_CA=off` connects without TLS. Leave `MYSQL_READ_ADDRS` unset to use only `localhost:3306`.

## Environment

| Variable | Role |
|----------|------|
| `GMS_DATA` | Badger directory. Default `data/gms`. |
| `GMS_MYSQL_HOST` | MySQL bind address. Default `localhost`. Use `0.0.0.0` for one published container. The three-node compose file binds each node's private address. |
| `GMS_MYSQL_PORT` | MySQL port. Default `3306`. |
| `GMS_RAFT_ADDR` | Turns cluster mode on and sets the Raft bind address, such as `10.116.0.2:7001`. `0.0.0.0` listens on every interface. A non-loopback address requires `GMS_RAFT_TLS_CERT`, `GMS_RAFT_TLS_KEY`, and `GMS_RAFT_TLS_CA`. |
| `GMS_RAFT_TLS_CERT` | PEM certificate for Raft and the write-forward port. It needs `serverAuth` and `clientAuth`. |
| `GMS_RAFT_TLS_KEY` | PEM private key for that certificate. |
| `GMS_RAFT_TLS_CA` | PEM CA that signed the certificate. Peers present the certificate and verify it with this CA. |
| `GMS_FORWARD_ADDR` | Write-forward listen address. The default is port 7002 on the Raft advertise host. |
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
| `GMS_BOOTSTRAP_PASSWORD` | Password for the first account. Required on the first boot of a standalone process or the Raft leader. Ignored once accounts are stored. |
| `GMS_BOOTSTRAP_USER` | First account name. Default `root`. |
| `GMS_BOOTSTRAP_HOST` | Host pattern for that account. Default `%`, so published Docker ports and other containers can connect. |
| `GMS_TLS_CERT` | PEM certificate for the MySQL listener. Set together with `GMS_TLS_KEY` to require TLS. |
| `GMS_TLS_KEY` | PEM private key for the MySQL listener. |

`CHANGE REPLICATION SOURCE TO`, `START REPLICA`, and `STOP REPLICA` configure that upstream job. The primary is the only node that connects. After a failover the new primary continues from the GTID stored with the applied rows. Replication filters are unsupported.
