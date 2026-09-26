package persist

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/hashicorp/raft"
)

// storeFSM applies committed key/value batches and catches a new replica up
// from a Badger backup.
type storeFSM struct {
	store *Store
}

var _ raft.BatchingFSM = (*storeFSM)(nil)

func (f *storeFSM) Apply(log *raft.Log) interface{} {
	resp := f.applyOne(log)
	if log.Type == raft.LogCommand || log.Type == raft.LogConfiguration {
		f.store.noteFSMApplied(log.Index)
	}
	return resp
}

// ApplyBatch applies each entry in its own Badger transaction. A later
// failure does not roll back an earlier entry. The Raft log is already
// durable; Badger is fsynced on snapshot and shutdown, and a crash replays
// any batch that did not reach disk.
func (f *storeFSM) ApplyBatch(logs []*raft.Log) []interface{} {
	out := make([]interface{}, len(logs))
	var max uint64
	for i, log := range logs {
		out[i] = f.applyOne(log)
		if log.Index > max {
			max = log.Index
		}
	}
	if max > 0 {
		f.store.noteFSMApplied(max)
	}
	return out
}

func (f *storeFSM) applyOne(log *raft.Log) interface{} {
	if log.Type != raft.LogCommand {
		return nil
	}
	batch, err := decodeBatch(log.Data)
	if err != nil {
		return err
	}
	// Decide before noteApplied drops this batch from the leader's in-flight list.
	local := f.store.privilegeProposed(batch.ID)
	err = f.store.applyOpsAt(log.Index, batch.Ops)
	f.store.noteApplied(batch.ID)
	if err != nil {
		return err
	}
	if err := f.store.reloadPrivileges(batch, local); err != nil {
		return err
	}
	if err := f.store.appendBinlog(log.Index, batch); err != nil {
		return err
	}
	if batch.Rotate {
		if err := f.store.rotateBinlog(); err != nil {
			return err
		}
	}
	return nil
}

func (f *storeFSM) Snapshot() (raft.FSMSnapshot, error) {
	if err := f.store.syncData(); err != nil {
		return nil, err
	}
	var body bytes.Buffer
	version, err := f.store.badgerDB().Backup(&body, 0)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, version); err != nil {
		return nil, err
	}
	if _, err := buf.Write(body.Bytes()); err != nil {
		return nil, err
	}
	return &storeSnapshot{store: f.store, data: buf.Bytes()}, nil
}

func (f *storeFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if err := f.store.installBackup(rc); err != nil {
		return err
	}
	f.store.noteFSMApplied(f.store.readRaftApplied())
	// The backup replaced the directory. Memory still has the old accounts
	// unless this process has not attached a privilege database yet.
	return f.store.reloadPrivilegesFromDisk()
}

type storeSnapshot struct {
	store *Store
	data  []byte
}

func (s *storeSnapshot) Persist(sink raft.SnapshotSink) error {
	if s.store != nil {
		if err := s.store.syncData(); err != nil {
			_ = sink.Cancel()
			return err
		}
	}
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *storeSnapshot) Release() {}

// installBackup replaces the open Badger directory with the keys in a full
// backup. Load merges into whatever is already open, so the replacement
// starts from an empty directory.
func (s *Store) installBackup(r io.Reader) error {
	var version uint64
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return err
	}
	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	old := s.db
	if err := old.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(s.path); err != nil {
		return err
	}
	db, err := openBadger(s.path, s.syncWrites)
	if err != nil {
		return err
	}
	if err := db.Load(r, 256); err != nil {
		db.Close()
		return err
	}
	s.db = db
	return nil
}
