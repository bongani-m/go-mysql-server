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

func (f *storeFSM) Apply(log *raft.Log) interface{} {
	if log.Type != raft.LogCommand {
		return nil
	}
	batch, err := decodeBatch(log.Data)
	if err != nil {
		return err
	}
	err = f.store.applyOps(batch.Ops)
	f.store.noteApplied(batch.ID)
	if err != nil {
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
	return &storeSnapshot{data: buf.Bytes()}, nil
}

func (f *storeFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return f.store.installBackup(rc)
}

type storeSnapshot struct {
	data []byte
}

func (s *storeSnapshot) Persist(sink raft.SnapshotSink) error {
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
