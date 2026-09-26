// Stress-tests a MySQL-protocol server with one small-site workload.
//
// The same binary talks to a single persist node, a Raft cluster, or MySQL.
// Writes go to -write. Reads go to -read (comma-separated). On a cluster,
// point -write at the leader and -read at the followers.
//
//	go run ./_persist/stress -label gms-single -write 127.0.0.1:3316
//	go run ./_persist/stress -label gms-cluster \
//	  -write 127.0.0.1:3326 -read 127.0.0.1:3327,127.0.0.1:3328
//	go run ./_persist/stress -label mysql -write 127.0.0.1:3336
//
// _persist/stress/run.sh starts the matching Docker services and calls this.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

func main() {
	var (
		label       = flag.String("label", "server", "name printed on the report")
		writeAddr   = flag.String("write", "127.0.0.1:3316", "host:port for writes and schema setup")
		readAddrs   = flag.String("read", "", "comma-separated host:port for reads; defaults to -write")
		user        = flag.String("user", "root", "MySQL user")
		password    = flag.String("password", "stress", "MySQL password")
		dbName      = flag.String("db", "stress", "database created for the run")
		tlsCA       = flag.String("tls-ca", "", "PEM CA file; required when the server demands TLS")
		concurrency = flag.Int("concurrency", 8, "simultaneous clients")
		duration    = flag.Duration("duration", 20*time.Second, "measured run length")
		warmup      = flag.Duration("warmup", 2*time.Second, "unmeasured run before the clock starts")
		seedN       = flag.Int("seed", 5000, "accounts inserted before the run; each gets two notes")
		batch       = flag.Int("batch", 100, "rows per seed INSERT")
		readPct     = flag.Int("read-pct", 80, "percent of operations that are reads")
		readyWait   = flag.Duration("ready-wait", 2*time.Minute, "how long to wait for the server to accept connections")
		jsonPath    = flag.String("json", "", "write the report JSON to this path")
		renderPath  = flag.String("render", "", "write a markdown summary from the JSON reports named as arguments, then exit")
	)
	flag.Parse()

	if *renderPath != "" {
		if flag.NArg() == 0 {
			fatalf("-render needs at least one JSON report")
		}
		if err := renderMarkdown(*renderPath, flag.Args()); err != nil {
			fatalf("summary: %v", err)
		}
		fmt.Printf("summary: %s\n", *renderPath)
		return
	}

	if *concurrency < 1 {
		fatalf("-concurrency must be at least 1")
	}
	if *seedN < 1 {
		fatalf("-seed must be at least 1")
	}
	if *batch < 1 {
		fatalf("-batch must be at least 1")
	}
	if *readPct < 0 || *readPct > 100 {
		fatalf("-read-pct must be between 0 and 100")
	}
	if *duration <= 0 {
		fatalf("-duration must be positive")
	}

	reads := splitAddrs(*readAddrs)
	if len(reads) == 0 {
		reads = []string{*writeAddr}
	}
	if err := registerCA(*tlsCA); err != nil {
		fatalf("tls: %v", err)
	}

	ctx := context.Background()
	fmt.Printf("waiting for %s\n", *writeAddr)
	admin, err := openDB(ctx, *writeAddr, *user, *password, "", 1, *readyWait, *tlsCA != "")
	if err != nil {
		fatalf("connect %s: %v", *writeAddr, err)
	}

	fmt.Printf("preparing database %s\n", *dbName)
	if err := setup(ctx, admin, *dbName); err != nil {
		admin.Close()
		fatalf("schema: %v", err)
	}
	admin.Close()

	writeDB, err := openDB(ctx, *writeAddr, *user, *password, *dbName, *concurrency, *readyWait, *tlsCA != "")
	if err != nil {
		fatalf("connect %s/%s: %v", *writeAddr, *dbName, err)
	}
	defer writeDB.Close()

	fmt.Printf("seeding %d accounts\n", *seedN)
	seedStart := time.Now()
	if err := seed(ctx, writeDB, *seedN, *batch); err != nil {
		fatalf("seed: %v", err)
	}
	seedTook := time.Since(seedStart)
	fmt.Printf("seed finished in %s\n", seedTook.Round(time.Millisecond))

	readDBs := make([]*sql.DB, len(reads))
	for i, addr := range reads {
		if addr == *writeAddr {
			readDBs[i] = writeDB
			continue
		}
		db, err := openDB(ctx, addr, *user, *password, *dbName, *concurrency, *readyWait, *tlsCA != "")
		if err != nil {
			fatalf("connect read %s: %v", addr, err)
		}
		defer db.Close()
		readDBs[i] = db
	}
	fmt.Printf("waiting until every read address has account %d\n", *seedN)
	if err := waitForSeed(ctx, readDBs, *seedN, *readyWait); err != nil {
		fatalf("replicas: %v", err)
	}

	workload := &workload{
		writeDB:  writeDB,
		readDBs:  readDBs,
		accounts: *seedN,
		ops:      buildOps(*readPct),
	}

	if *warmup > 0 {
		fmt.Printf("warmup %s with %d clients\n", warmup.Round(time.Millisecond), *concurrency)
		runPhase(ctx, workload, *concurrency, *warmup, false)
	}
	fmt.Printf("measuring %s\n", duration.Round(time.Millisecond))
	stats := runPhase(ctx, workload, *concurrency, *duration, true)

	rep := stats.report(*label, *writeAddr, reads, *seedN, seedTook, *concurrency, *duration, *readPct)
	fmt.Print(rep.text())
	if *jsonPath != "" {
		if err := os.WriteFile(*jsonPath, rep.json(), 0o644); err != nil {
			fatalf("json: %v", err)
		}
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func splitAddrs(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func registerCA(path string) error {
	if path == "" {
		return nil
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return fmt.Errorf("%s has no certificates", path)
	}
	return mysql.RegisterTLSConfig("stress", &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	})
}

func openDB(ctx context.Context, addr, user, password, dbName string, conns int, wait time.Duration, tlsOn bool) (*sql.DB, error) {
	cfg := mysql.Config{
		User:                 user,
		Passwd:               password,
		Net:                  "tcp",
		Addr:                 addr,
		DBName:               dbName,
		ParseTime:            true,
		Timeout:              5 * time.Second,
		ReadTimeout:          time.Minute,
		WriteTimeout:         time.Minute,
		AllowNativePasswords: true,
		Params:               map[string]string{"charset": "utf8mb4"},
	}
	if tlsOn {
		cfg.TLSConfig = "stress"
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	if conns < 1 {
		conns = 1
	}
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)

	deadline := time.Now().Add(wait)
	var last error
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		last = db.PingContext(pingCtx)
		cancel()
		if last == nil {
			return db, nil
		}
		if time.Now().After(deadline) {
			db.Close()
			return nil, last
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func setup(ctx context.Context, db *sql.DB, name string) error {
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(name)); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "USE "+quoteIdent(name)); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS notes"); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS accounts"); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `
CREATE TABLE accounts (
  id BIGINT NOT NULL AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL,
  name VARCHAR(255) NOT NULL,
  status TINYINT NOT NULL,
  created_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY accounts_email (email),
  KEY accounts_status_created (status, created_at)
)`)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
CREATE TABLE notes (
  id BIGINT NOT NULL AUTO_INCREMENT,
  account_id BIGINT NOT NULL,
  body VARCHAR(255) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  KEY notes_account (account_id),
  CONSTRAINT notes_account_fk FOREIGN KEY (account_id) REFERENCES accounts (id)
)`)
	return err
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func seed(ctx context.Context, db *sql.DB, n, batch int) error {
	now := time.Now().UTC().Truncate(time.Microsecond)
	for from := 0; from < n; from += batch {
		to := from + batch
		if to > n {
			to = n
		}
		if err := insertAccounts(ctx, db, from, to, now); err != nil {
			return err
		}
	}
	// Two notes per account. A batch of accounts becomes two notes each.
	noteBatch := batch
	if noteBatch > n {
		noteBatch = n
	}
	for from := 0; from < n; from += noteBatch {
		to := from + noteBatch
		if to > n {
			to = n
		}
		if err := insertNotes(ctx, db, from, to, now); err != nil {
			return err
		}
	}
	return nil
}

func insertAccounts(ctx context.Context, db *sql.DB, from, to int, now time.Time) error {
	var b strings.Builder
	b.WriteString("INSERT INTO accounts (email, name, status, created_at) VALUES ")
	args := make([]any, 0, (to-from)*4)
	for i := from; i < to; i++ {
		if i > from {
			b.WriteByte(',')
		}
		b.WriteString("(?,?,?,?)")
		id := i + 1
		args = append(args,
			fmt.Sprintf("user%d@example.com", id),
			fmt.Sprintf("User %d", id),
			id%3,
			now,
		)
	}
	_, err := db.ExecContext(ctx, b.String(), args...)
	return err
}

func insertNotes(ctx context.Context, db *sql.DB, from, to int, now time.Time) error {
	var b strings.Builder
	b.WriteString("INSERT INTO notes (account_id, body, created_at) VALUES ")
	args := make([]any, 0, (to-from)*2*3)
	first := true
	for i := from; i < to; i++ {
		id := i + 1
		for _, body := range []string{"hello", "follow-up"} {
			if !first {
				b.WriteByte(',')
			}
			first = false
			b.WriteString("(?,?,?)")
			args = append(args, id, body, now)
		}
	}
	_, err := db.ExecContext(ctx, b.String(), args...)
	return err
}

func waitForSeed(ctx context.Context, dbs []*sql.DB, n int, wait time.Duration) error {
	want := fmt.Sprintf("user%d@example.com", n)
	deadline := time.Now().Add(wait)
	for i, db := range dbs {
		for {
			var email string
			err := db.QueryRowContext(ctx, "SELECT email FROM accounts WHERE id = ?", n).Scan(&email)
			if err == nil && email == want {
				break
			}
			if time.Now().After(deadline) {
				if err == nil {
					err = fmt.Errorf("email %q", email)
				}
				return fmt.Errorf("read address %d missing account %d: %w", i, n, err)
			}
			time.Sleep(300 * time.Millisecond)
		}
	}
	return nil
}

type opKind int

const (
	opPointRead opKind = iota
	opEmailRead
	opNotesRead
	opInsert
	opUpdate
	opTx
)

func (k opKind) String() string {
	switch k {
	case opPointRead:
		return "point_read"
	case opEmailRead:
		return "email_read"
	case opNotesRead:
		return "notes_read"
	case opInsert:
		return "insert_note"
	case opUpdate:
		return "update_status"
	case opTx:
		return "tx_note"
	default:
		return "unknown"
	}
}

type weightedOp struct {
	kind   opKind
	weight int
}

// buildOps splits readPct across three indexed reads (5:2:1) and the rest
// across insert, update, and a short transaction (2:1:1).
func buildOps(readPct int) []weightedOp {
	read := readPct * 10
	write := (100 - readPct) * 10
	point := read * 5 / 8
	email := read * 2 / 8
	notes := read - point - email
	insert := write / 2
	update := write / 4
	tx := write - insert - update
	all := []weightedOp{
		{opPointRead, point},
		{opEmailRead, email},
		{opNotesRead, notes},
		{opInsert, insert},
		{opUpdate, update},
		{opTx, tx},
	}
	var out []weightedOp
	for _, op := range all {
		if op.weight > 0 {
			out = append(out, op)
		}
	}
	return out
}

type workload struct {
	writeDB  *sql.DB
	readDBs  []*sql.DB
	accounts int
	ops      []weightedOp
}

func (w *workload) readDB(worker int) *sql.DB {
	return w.readDBs[worker%len(w.readDBs)]
}

func (w *workload) do(ctx context.Context, worker int, kind opKind, rng uint64) error {
	id := int(rng%uint64(w.accounts)) + 1
	switch kind {
	case opPointRead:
		var email string
		var status int
		err := w.readDB(worker).QueryRowContext(ctx,
			"SELECT email, status FROM accounts WHERE id = ?", id).Scan(&email, &status)
		return err
	case opEmailRead:
		var got int
		err := w.readDB(worker).QueryRowContext(ctx,
			"SELECT id FROM accounts WHERE email = ?",
			fmt.Sprintf("user%d@example.com", id)).Scan(&got)
		return err
	case opNotesRead:
		rows, err := w.readDB(worker).QueryContext(ctx,
			"SELECT id, body FROM notes WHERE account_id = ? ORDER BY id DESC LIMIT 5", id)
		if err != nil {
			return err
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			var noteID int
			var body string
			if err := rows.Scan(&noteID, &body); err != nil {
				return err
			}
			n++
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("account %d has no notes", id)
		}
		return nil
	case opInsert:
		_, err := w.writeDB.ExecContext(ctx,
			"INSERT INTO notes (account_id, body, created_at) VALUES (?, ?, ?)",
			id, "live", time.Now().UTC())
		return err
	case opUpdate:
		_, err := w.writeDB.ExecContext(ctx,
			"UPDATE accounts SET status = ? WHERE id = ?", id%3, id)
		return err
	case opTx:
		tx, err := w.writeDB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO notes (account_id, body, created_at) VALUES (?, ?, ?)",
			id, "tx", time.Now().UTC()); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"UPDATE accounts SET status = ? WHERE id = ?", (id+1)%3, id); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	default:
		return fmt.Errorf("unknown op %d", kind)
	}
}

type samples struct {
	lat  [][]time.Duration
	errs []int
	msg  []string
}

func newSamples() *samples {
	return &samples{
		lat:  make([][]time.Duration, opTx+1),
		errs: make([]int, opTx+1),
		msg:  make([]string, opTx+1),
	}
}

func (s *samples) add(kind opKind, d time.Duration, err error) {
	if err != nil {
		s.errs[kind]++
		if s.msg[kind] == "" {
			s.msg[kind] = err.Error()
		}
		return
	}
	s.lat[kind] = append(s.lat[kind], d)
}

func (s *samples) merge(other *samples) {
	for k := opKind(0); k <= opTx; k++ {
		s.lat[k] = append(s.lat[k], other.lat[k]...)
		s.errs[k] += other.errs[k]
		if s.msg[k] == "" {
			s.msg[k] = other.msg[k]
		}
	}
}

func runPhase(ctx context.Context, w *workload, concurrency int, d time.Duration, record bool) *samples {
	phaseCtx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	var wg sync.WaitGroup
	out := make([]*samples, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			out[id] = worker(phaseCtx, w, id, record)
		}(i)
	}
	wg.Wait()
	merged := newSamples()
	if !record {
		return merged
	}
	for _, s := range out {
		if s != nil {
			merged.merge(s)
		}
	}
	return merged
}

func worker(ctx context.Context, w *workload, id int, record bool) *samples {
	s := newSamples()
	// SplitMix64 seeded per worker. The mix only picks the next statement.
	var state uint64 = uint64(id+1) * 0x9E3779B97F4A7C15
	next := func() uint64 {
		state += 0x9E3779B97F4A7C15
		z := state
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		return z ^ (z >> 31)
	}
	total := 0
	for _, op := range w.ops {
		total += op.weight
	}
	for {
		if ctx.Err() != nil {
			return s
		}
		roll := int(next() % uint64(total))
		kind := w.ops[len(w.ops)-1].kind
		for _, op := range w.ops {
			if roll < op.weight {
				kind = op.kind
				break
			}
			roll -= op.weight
		}
		start := time.Now()
		err := w.do(ctx, id, kind, next())
		elapsed := time.Since(start)
		if ctx.Err() != nil {
			return s
		}
		if record {
			s.add(kind, elapsed, err)
		}
	}
}

type opReport struct {
	Name   string  `json:"name"`
	Ops    int     `json:"ops"`
	Errors int     `json:"errors"`
	OpsSec float64 `json:"ops_per_sec"`
	AvgMs  float64 `json:"avg_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MaxMs  float64 `json:"max_ms"`
	Error  string  `json:"error,omitempty"`
}

type report struct {
	Label           string     `json:"label"`
	Write           string     `json:"write"`
	Read            []string   `json:"read"`
	SeedAccounts    int        `json:"seed_accounts"`
	SeedNotes       int        `json:"seed_notes"`
	SeedSeconds     float64    `json:"seed_seconds"`
	Concurrency     int        `json:"concurrency"`
	DurationSeconds float64    `json:"duration_seconds"`
	ReadPct         int        `json:"read_pct"`
	Ops             []opReport `json:"ops"`
	OpsPerSec       float64    `json:"ops_per_sec"`
	Errors          int        `json:"errors"`
	P50Ms           float64    `json:"p50_ms"`
	P95Ms           float64    `json:"p95_ms"`
	P99Ms           float64    `json:"p99_ms"`
}

func (s *samples) report(label, write string, reads []string, accounts int, seedTook time.Duration, concurrency int, duration time.Duration, readPct int) report {
	rep := report{
		Label:           label,
		Write:           write,
		Read:            reads,
		SeedAccounts:    accounts,
		SeedNotes:       accounts * 2,
		SeedSeconds:     seedTook.Seconds(),
		Concurrency:     concurrency,
		DurationSeconds: duration.Seconds(),
		ReadPct:         readPct,
	}
	var all []time.Duration
	seconds := duration.Seconds()
	for k := opKind(0); k <= opTx; k++ {
		lat := append([]time.Duration(nil), s.lat[k]...)
		if len(lat) == 0 && s.errs[k] == 0 {
			continue
		}
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		op := opReport{
			Name:   k.String(),
			Ops:    len(lat),
			Errors: s.errs[k],
			Error:  s.msg[k],
		}
		if seconds > 0 {
			op.OpsSec = float64(len(lat)) / seconds
		}
		if len(lat) > 0 {
			var sum time.Duration
			for _, d := range lat {
				sum += d
			}
			op.AvgMs = ms(sum / time.Duration(len(lat)))
			op.P50Ms = ms(percentile(lat, 0.50))
			op.P95Ms = ms(percentile(lat, 0.95))
			op.P99Ms = ms(percentile(lat, 0.99))
			op.MaxMs = ms(lat[len(lat)-1])
			all = append(all, lat...)
		}
		rep.Errors += s.errs[k]
		rep.Ops = append(rep.Ops, op)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	if seconds > 0 {
		rep.OpsPerSec = float64(len(all)) / seconds
	}
	if len(all) > 0 {
		rep.P50Ms = ms(percentile(all, 0.50))
		rep.P95Ms = ms(percentile(all, 0.95))
		rep.P99Ms = ms(percentile(all, 0.99))
	}
	return rep
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func (r report) text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "\nlabel:        %s\n", r.Label)
	fmt.Fprintf(&b, "write:        %s\n", r.Write)
	fmt.Fprintf(&b, "read:         %s\n", strings.Join(r.Read, ","))
	fmt.Fprintf(&b, "seed:         %d accounts, %d notes in %.2fs\n", r.SeedAccounts, r.SeedNotes, r.SeedSeconds)
	fmt.Fprintf(&b, "concurrency:  %d\n", r.Concurrency)
	fmt.Fprintf(&b, "duration:     %.0fs\n", r.DurationSeconds)
	fmt.Fprintf(&b, "read pct:     %d\n\n", r.ReadPct)
	fmt.Fprintf(&b, "%-14s %8s %8s %10s %10s %10s %10s %10s %10s\n",
		"op", "ops", "errors", "ops/s", "avg", "p50", "p95", "p99", "max")
	for _, op := range r.Ops {
		fmt.Fprintf(&b, "%-14s %8d %8d %10.1f %10s %10s %10s %10s %10s\n",
			op.Name, op.Ops, op.Errors, op.OpsSec,
			fmtMs(op.AvgMs), fmtMs(op.P50Ms), fmtMs(op.P95Ms), fmtMs(op.P99Ms), fmtMs(op.MaxMs))
		if op.Error != "" {
			fmt.Fprintf(&b, "  first error: %s\n", op.Error)
		}
	}
	fmt.Fprintf(&b, "\nSUMMARY label=%s ops_s=%.2f errors=%d p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f seed_s=%.2f\n",
		r.Label, r.OpsPerSec, r.Errors, r.P50Ms, r.P95Ms, r.P99Ms, r.SeedSeconds)
	return b.String()
}

func fmtMs(v float64) string {
	if v <= 0 {
		return "-"
	}
	if v < 1 {
		return fmt.Sprintf("%.0fµs", v*1000)
	}
	if v < 1000 {
		return fmt.Sprintf("%.2fms", v)
	}
	return fmt.Sprintf("%.2fs", v/1000)
}

func (r report) json() []byte {
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return []byte("{}\n")
	}
	return append(raw, '\n')
}
