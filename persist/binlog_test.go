package persist

import (
	"path/filepath"
	"testing"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"
)

func TestBinlogRotateAndResume(t *testing.T) {
	dir := t.TempDir()
	b, err := openBinlog(dir, testServerUUID, 1)
	require.NoError(t, err)
	require.NoError(t, b.append(1, replBatch{Statement: "create table t (id int)", Unix: 1}))
	require.NoError(t, b.append(2, replBatch{Statement: "insert into t values (1)", Unix: 2}))
	names := b.fileNames()
	require.GreaterOrEqual(t, len(names), 2)

	var seqs []int64
	for _, name := range names {
		events, format, err := readBinlogFile(filepath.Join(dir, name))
		require.NoError(t, err)
		for _, ev := range events {
			if !ev.IsGTID() {
				continue
			}
			gtid, _, err := ev.GTID(format)
			require.NoError(t, err)
			seqs = append(seqs, gtid.(mysql.Mysql56GTID).Sequence)
		}
	}
	require.Equal(t, []int64{1, 2}, seqs)

	set := mysql.Mysql56GTIDSet{}
	set = set.AddGTID(mysql.Mysql56GTID{Server: b.sid, Sequence: 1}).(mysql.Mysql56GTIDSet)
	got, err := b.eventsFor(set)
	require.NoError(t, err)
	var sawRotate, saw2 bool
	for _, ev := range got {
		if ev.IsRotate() {
			sawRotate = true
		}
		if !ev.IsGTID() {
			continue
		}
		gtid, _, err := ev.GTID(b.format)
		require.NoError(t, err)
		seq := gtid.(mysql.Mysql56GTID).Sequence
		require.NotEqual(t, int64(1), seq)
		if seq == 2 {
			saw2 = true
		}
	}
	require.True(t, sawRotate)
	require.True(t, saw2)
	require.True(t, b.executed.ContainsGTID(mysql.Mysql56GTID{Server: b.sid, Sequence: 2}))
	last := names[len(names)-1]
	b.close()

	reopened, err := openBinlog(dir, testServerUUID, 1)
	require.NoError(t, err)
	defer reopened.close()
	require.Equal(t, last, reopened.name)
	require.True(t, reopened.executed.ContainsGTID(mysql.Mysql56GTID{Server: b.sid, Sequence: 1}))
	require.True(t, reopened.executed.ContainsGTID(mysql.Mysql56GTID{Server: b.sid, Sequence: 2}))
}
