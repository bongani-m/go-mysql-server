#!/usr/bin/env bash
# Run the same workload against one persist node, the three-node cluster, MySQL, or TiDB.
#
# From go-mysql-server:
#
#   _persist/stress/run.sh single
#   _persist/stress/run.sh cluster
#   _persist/stress/run.sh mysql
#   _persist/stress/run.sh tidb
#   _persist/stress/run.sh compare
#
# Flags after -- are passed to the stress client:
#
#   _persist/stress/run.sh compare -- -duration 60s -concurrency 32 -seed 20000
#
# compare runs the targets one after another so they do not share the CPU.
# Every run writes _persist/stress/summary.md.
# KEEP=1 leaves the containers up after the run.
# Wipe stored data with:
#
#   docker compose -f _persist/stress/compose.yaml -p gms-stress down -v
#
# Ports: single 3316, cluster leader 3326, followers 3327 and 3328, MySQL 3336, TiDB 3346.
# User root, password stress, database stress.
# Every target serves TLS. The script creates _persist/stress/certs on first use.
# TiDB setup uses plaintext on the Docker network, then the client connects with TLS.

set -euo pipefail

dir=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$dir/../.." && pwd)
compose=(docker compose -f "$dir/compose.yaml" -p gms-stress)
summary=""
results=$dir/results
json_files=()
tls_ca=$dir/certs/ca.crt
mkdir -p "$results"

# caching_sha2_password is refused without TLS, so every target uses this cert.
# The CA also authenticates Raft between the cluster nodes.
if [[ ! -f "$tls_ca" || ! -f "$dir/certs/server.crt" || ! -f "$dir/certs/server.key" ]]; then
	mkdir -p "$dir/certs"
	openssl req -x509 -newkey rsa:2048 -nodes \
		-keyout "$dir/certs/ca.key" \
		-out "$tls_ca" \
		-days 365 \
		-subj "/CN=gms-stress-ca" \
		-addext "basicConstraints=critical,CA:TRUE" \
		-addext "keyUsage=critical,keyCertSign,cRLSign"
	openssl req -newkey rsa:2048 -nodes \
		-keyout "$dir/certs/server.key" \
		-out "$dir/certs/server.csr" \
		-subj "/CN=gms-stress" \
		-addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:10.117.0.2,IP:10.117.0.3,IP:10.117.0.4" \
		-addext "extendedKeyUsage=serverAuth,clientAuth"
	openssl x509 -req -in "$dir/certs/server.csr" \
		-CA "$tls_ca" -CAkey "$dir/certs/ca.key" -CAcreateserial \
		-out "$dir/certs/server.crt" -days 365 \
		-copy_extensions copy
	chmod 644 "$dir/certs/server.key" "$dir/certs/server.crt" "$tls_ca"
fi

target=${1:-}
shift || true
if [[ "${1:-}" == "--" ]]; then
	shift
fi
extra=("$@")

usage() {
	echo "usage: $0 single|cluster|mysql|tidb|compare [-- stress flags]" >&2
	exit 2
}

case "$target" in
single | cluster | mysql | tidb | compare) ;;
*) usage ;;
esac

profile=""
sampler_pid=""
cleanup() {
	status=$?
	stop_sampler
	if [[ $status -ne 0 && -n "$profile" ]]; then
		"${compose[@]}" --profile "$profile" logs --tail 80 || true
	fi
	if [[ -n "$profile" && "${KEEP:-}" != 1 ]]; then
		"${compose[@]}" --profile "$profile" down
	fi
}
trap cleanup EXIT

build_image() {
	"${compose[@]}" --profile single build
}

up() {
	profile=$1
	case "$profile" in
	mysql | tidb)
		"${compose[@]}" --profile "$profile" up -d
		;;
	*)
		build_image
		"${compose[@]}" --profile "$profile" up -d
		;;
	esac
	if [[ "$profile" == tidb ]]; then
		prepare_tidb
	fi
}

# TiDB answers a MySQL ping before TiKV can serve SQL. Retry DDL, then set the
# password the stress client uses. A reused volume already has that password.
prepare_tidb() {
	local network
	local deadline=$((SECONDS + 30))
	while (( SECONDS < deadline )); do
		if docker inspect gms-stress-tidb-1 >/dev/null 2>&1; then
			break
		fi
		sleep 1
	done
	if ! docker inspect gms-stress-tidb-1 >/dev/null 2>&1; then
		echo "tidb container did not start" >&2
		exit 1
	fi
	network=$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' gms-stress-tidb-1 | awk '{print $1}')
	if [[ -z "$network" ]]; then
		echo "tidb container has no network" >&2
		exit 1
	fi
	docker run --rm --network "$network" --entrypoint bash mysql:8.4 -c '
set -euo pipefail
deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
	if mysql --protocol=TCP -h tidb -P 4000 -uroot --ssl-mode=DISABLED --connect-timeout=3 \
		-e "CREATE DATABASE IF NOT EXISTS stress" >/dev/null 2>&1; then
		mysql --protocol=TCP -h tidb -P 4000 -uroot --ssl-mode=DISABLED --connect-timeout=3 \
			-e "ALTER USER '\''root'\''@'\''%'\'' IDENTIFIED BY '\''stress'\''"
		exit 0
	fi
	if mysql --protocol=TCP -h tidb -P 4000 -uroot -pstress --ssl-mode=DISABLED --connect-timeout=3 \
		-e "CREATE DATABASE IF NOT EXISTS stress" >/dev/null 2>&1; then
		exit 0
	fi
	sleep 2
done
echo "tidb did not become ready" >&2
exit 1
'
}

run_client() {
	local label=$1
	shift
	(
		cd "$root"
		go run ./_persist/stress -label "$label" "$@" ${extra[@]+"${extra[@]}"}
	)
}

containers_for() {
	case "$1" in
	single) echo gms-stress-single-1 ;;
	cluster) echo gms-stress-n1-1 gms-stress-n2-1 gms-stress-n3-1 ;;
	mysql) echo gms-stress-mysql-1 ;;
	tidb) echo gms-stress-pd-1 gms-stress-tikv1-1 gms-stress-tikv2-1 gms-stress-tikv3-1 gms-stress-tidb-1 ;;
	*) return 1 ;;
	esac
}

# Disk for TiDB is the TiKV data directories. PD and the SQL server hold no table data.
disk_containers_for() {
	case "$1" in
	tidb) echo gms-stress-tikv1-1 gms-stress-tikv2-1 gms-stress-tikv3-1 ;;
	*) containers_for "$1" ;;
	esac
}

disk_path_for() {
	case "$1" in
	mysql) echo /var/lib/mysql ;;
	*) echo /data ;;
	esac
}

start_sampler() {
	local name=$1
	local out=$results/$name.stats
	: >"$out"
	local -a cs
	read -r -a cs <<<"$(containers_for "$name")"
	(
		while true; do
			docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' "${cs[@]}" >>"$out" 2>/dev/null || true
			sleep 1
		done
	) &
	sampler_pid=$!
}

stop_sampler() {
	if [[ -n "$sampler_pid" ]]; then
		kill "$sampler_pid" 2>/dev/null || true
		wait "$sampler_pid" 2>/dev/null || true
		sampler_pid=""
	fi
}

# du of the data directory on each container. One line per container: name bytes.
write_disk() {
	local name=$1
	local path
	path=$(disk_path_for "$name")
	local -a cs
	read -r -a cs <<<"$(disk_containers_for "$name")"
	local out=$results/$name.disk
	: >"$out"
	local c bytes
	for c in "${cs[@]}"; do
		bytes=$(docker exec "$c" du -sb "$path" 2>/dev/null | awk 'NR==1 {print $1}')
		if [[ -z "$bytes" ]]; then
			bytes=$(docker exec "$c" du -sk "$path" | awk 'NR==1 {print $1 * 1024}')
		fi
		if [[ -z "$bytes" ]]; then
			echo "could not measure disk usage of $c:$path" >&2
			exit 1
		fi
		echo "$c $bytes" >>"$out"
	done
}

run_target() {
	local name=$1
	up "$name"
	local log=/dev/stdout
	if [[ -n "$summary" ]]; then
		log=$summary
	fi
	local json=$results/$name.json
	start_sampler "$name"
	set +e
	case "$name" in
	single)
		run_client gms-single -write 127.0.0.1:3316 -tls-ca "$tls_ca" -json "$json" | tee -a "$log"
		;;
	cluster)
		run_client gms-cluster -write 127.0.0.1:3326 -read 127.0.0.1:3327,127.0.0.1:3328 -tls-ca "$tls_ca" -json "$json" | tee -a "$log"
		;;
	mysql)
		run_client mysql -write 127.0.0.1:3336 -tls-ca "$tls_ca" -json "$json" | tee -a "$log"
		;;
	tidb)
		run_client tidb -write 127.0.0.1:3346 -tls-ca "$tls_ca" -json "$json" | tee -a "$log"
		;;
	esac
	local client_status=${PIPESTATUS[0]}
	set -e
	stop_sampler
	if [[ $client_status -ne 0 ]]; then
		exit "$client_status"
	fi
	write_disk "$name"
	json_files+=("$json")
	if [[ "${KEEP:-}" != 1 ]]; then
		"${compose[@]}" --profile "$profile" down
		profile=""
	fi
}

write_summary() {
	(
		cd "$root"
		go run ./_persist/stress -render "$dir/summary.md" "${json_files[@]}"
	)
}

if [[ "$target" == compare ]]; then
	summary=$dir/last-compare.txt
	: >"$summary"
	for name in single cluster mysql tidb; do
		echo "======== $name ========" | tee -a "$summary"
		run_target "$name"
	done
	write_summary
	echo
	echo "comparison"
	grep '^SUMMARY ' "$summary" || true
	echo "full output: $summary"
	exit 0
fi

run_target "$target"
write_summary
