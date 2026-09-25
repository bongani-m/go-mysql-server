package persist

import (
	"bytes"
	"encoding/gob"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

// errReplicate aborts the Badger transaction that computed a commit. The
// recorded key/value operations are proposed to Raft and applied only after
// a quorum persists them.
var errReplicate = errors.New("persist: replicate batch")

// kvOp is one recorded Badger write. Delete is set instead of an empty value
// so a stored empty value stays distinct from a removal.
type kvOp struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// rowChange is the before/after image of one row edit, used to build a
// MySQL row binlog event. Schema is the table's stored schema bytes.
type rowChange struct {
	Database string
	Table    string
	Schema   []byte
	Op       int
	Before   []byte
	After    []byte
}

// replBatch is one Raft log entry: the key/value writes, row images for DML,
// and the statement text for a catalog change that has no row images.
type replBatch struct {
	Ops       []kvOp
	Rows      []rowChange
	Statement string
	Unix      uint32
}

// recordingTxn copies every Set and Delete while the real transaction still
// runs, so the batch matches what this commit would have written.
type recordingTxn struct {
	*badger.Txn
	ops  []kvOp
	rows []rowChange
}

func (t *recordingTxn) Set(key, val []byte) error {
	t.ops = append(t.ops, kvOp{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), val...),
	})
	return t.Txn.Set(key, val)
}

func (t *recordingTxn) Delete(key []byte) error {
	t.ops = append(t.ops, kvOp{
		Key:    append([]byte(nil), key...),
		Delete: true,
	})
	return t.Txn.Delete(key)
}

func (tx *kvTx) noteRow(ch rowChange) {
	rec, ok := tx.txn.(*recordingTxn)
	if !ok || rec == nil {
		return
	}
	ch.Schema = append([]byte(nil), ch.Schema...)
	ch.Before = append([]byte(nil), ch.Before...)
	ch.After = append([]byte(nil), ch.After...)
	rec.rows = append(rec.rows, ch)
}

// commit runs fn as the single writer. Without a cluster it commits directly.
// With a cluster the leader records the writes, rolls them back, and proposes
// the batch. Followers reject the SQL write.
func (s *Store) commit(statement string, fn func(tx *kvTx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.badgerDB()
	if s.cluster == nil {
		return db.Update(func(txn *badger.Txn) error {
			return fn(&kvTx{txn: txn})
		})
	}
	if s.cluster.raft.State() != raft.Leader {
		return s.cluster.notLeader()
	}
	rec := &recordingTxn{}
	err := db.Update(func(txn *badger.Txn) error {
		rec.Txn = txn
		if err := fn(&kvTx{txn: rec}); err != nil {
			return err
		}
		return errReplicate
	})
	if err != nil && !errors.Is(err, errReplicate) {
		return err
	}
	if len(rec.ops) == 0 {
		return nil
	}
	return s.cluster.propose(replBatch{
		Ops:       rec.ops,
		Rows:      rec.rows,
		Statement: statement,
		Unix:      uint32(time.Now().Unix()),
	})
}

func encodeBatch(batch replBatch) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(batch); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeBatch(raw []byte) (replBatch, error) {
	var batch replBatch
	err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&batch)
	return batch, err
}

// applyOps writes a committed batch into the local Badger. Puts and deletes
// are applied in order and are safe to repeat for the same Raft index.
func (s *Store) applyOps(ops []kvOp) error {
	if len(ops) == 0 {
		return nil
	}
	return s.badgerDB().Update(func(txn *badger.Txn) error {
		for _, op := range ops {
			if op.Delete {
				if err := txn.Delete(op.Key); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				continue
			}
			if err := txn.Set(op.Key, op.Value); err != nil {
				return err
			}
		}
		return nil
	})
}
