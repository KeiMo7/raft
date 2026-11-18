// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type recoveryTestSnapshot struct {
	entries []string
}

func (s *recoveryTestSnapshot) Persist(sink SnapshotSink) error {
	var out strings.Builder
	fmt.Fprintf(&out, "COUNT %d\n", len(s.entries))
	for _, entry := range s.entries {
		encoded, err := json.Marshal(entry)
		if err != nil {
			_ = sink.Cancel()
			return err
		}
		out.Write(encoded)
		out.WriteByte('\n')
	}
	if _, err := io.WriteString(sink, out.String()); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *recoveryTestSnapshot) Release() {}

type recoveryTestFSM struct {
	mu sync.Mutex

	entries []string
	applied uint64
	total   uint64

	failAfter uint64
}

func newRecoveryTestFSM(entries ...string) *recoveryTestFSM {
	return &recoveryTestFSM{entries: append([]string(nil), entries...)}
}

func (f *recoveryTestFSM) Apply(log *Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, string(log.Data))
	return len(f.entries)
}

func (f *recoveryTestFSM) Snapshot() (FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &recoveryTestSnapshot{entries: append([]string(nil), f.entries...)}, nil
}

func (f *recoveryTestFSM) Restore(source io.ReadCloser) error {
	scanner := bufio.NewScanner(source)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return err
		}
		return fmt.Errorf("missing snapshot count")
	}
	var total uint64
	if _, err := fmt.Sscanf(scanner.Text(), "COUNT %d", &total); err != nil {
		return fmt.Errorf("invalid snapshot count: %w", err)
	}

	f.mu.Lock()
	f.entries = nil
	f.applied = 0
	f.total = total
	f.mu.Unlock()

	for i := uint64(0); i < total; i++ {
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return err
			}
			return fmt.Errorf("snapshot ended after %d of %d entries", i, total)
		}
		var entry string
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("invalid entry %d: %w", i, err)
		}
		f.mu.Lock()
		f.entries = append(f.entries, entry)
		f.applied++
		applied := f.applied
		f.mu.Unlock()
		if f.failAfter > 0 && applied >= f.failAfter {
			return fmt.Errorf("injected restore failure after %d entries", applied)
		}
	}
	return scanner.Err()
}

func (f *recoveryTestFSM) RestoreProgress() (uint64, uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied, f.total
}

func (f *recoveryTestFSM) StateHash() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := sha256.New()
	for _, entry := range f.entries {
		_, _ = io.WriteString(h, strconv.Itoa(len(entry)))
		_, _ = io.WriteString(h, ":")
		_, _ = io.WriteString(h, entry)
		_, _ = io.WriteString(h, ";")
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (f *recoveryTestFSM) Entries() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.entries...)
}

func TestFileSnapshotStore_IntegrityFailures(t *testing.T) {
	t.Run("truncated", func(t *testing.T) {
		dir := t.TempDir()
		store, id := createRecoverySnapshot(t, dir, newRecoveryTestFSM("one", "two", "three"), 10, 2)
		meta, err := store.readMeta(id)
		if err != nil {
			t.Fatal(err)
		}
		statePath := filepath.Join(dir, snapPath, id, stateFilePath)
		if err := os.Truncate(statePath, meta.Size/2); err != nil {
			t.Fatal(err)
		}

		_, source, err := store.Open(id)
		if err == nil {
			_ = source.Close()
			t.Fatal("expected truncated snapshot to fail validation")
		}
		if !errors.Is(err, ErrSnapshotCorrupt) {
			t.Fatalf("expected ErrSnapshotCorrupt, got %v", err)
		}
		t.Logf("truncated snapshot rejected: %v", err)
		if !strings.Contains(err.Error(), "truncated") {
			t.Fatalf("expected truncation detail, got %v", err)
		}
		if _, statErr := os.Stat(statePath); statErr != nil {
			t.Fatalf("corrupt snapshot was removed: %v", statErr)
		}
	})

	t.Run("checksum", func(t *testing.T) {
		dir := t.TempDir()
		store, id := createRecoverySnapshot(t, dir, newRecoveryTestFSM("one", "two", "three"), 10, 2)
		statePath := filepath.Join(dir, snapPath, id, stateFilePath)
		data, err := os.ReadFile(statePath)
		if err != nil {
			t.Fatal(err)
		}
		data[len(data)/2] ^= 0xff
		if err := os.WriteFile(statePath, data, 0o600); err != nil {
			t.Fatal(err)
		}

		_, _, err = store.Open(id)
		if !errors.Is(err, ErrSnapshotCorrupt) {
			t.Fatalf("expected ErrSnapshotCorrupt, got %v", err)
		}
		var integrityErr *SnapshotIntegrityError
		if !errors.As(err, &integrityErr) || integrityErr.ExpectedCRC == "" || integrityErr.ActualCRC == "" {
			t.Fatalf("expected CRC details, got %#v", integrityErr)
		}
		t.Logf("checksum failure rejected: %v", err)
	})

	t.Run("unsupported version", func(t *testing.T) {
		dir := t.TempDir()
		store, id := createRecoverySnapshot(t, dir, newRecoveryTestFSM("one"), 10, 2)
		rewriteSnapshotMeta(t, dir, id, func(meta *fileSnapshotMeta) {
			meta.Version = SnapshotVersionMax + 1
		})

		_, err := store.List()
		if !errors.Is(err, ErrSnapshotVersion) {
			t.Fatalf("expected ErrSnapshotVersion from List, got %v", err)
		}
		_, _, err = store.Open(id)
		if !errors.Is(err, ErrSnapshotVersion) {
			t.Fatalf("expected ErrSnapshotVersion from Open, got %v", err)
		}
		t.Logf("unsupported version rejected: %v", err)
	})

	t.Run("legacy version zero", func(t *testing.T) {
		dir := t.TempDir()
		store, id := createRecoverySnapshot(t, dir, newRecoveryTestFSM("one"), 10, 2)
		rewriteSnapshotMeta(t, dir, id, func(meta *fileSnapshotMeta) {
			meta.Version = 0
		})

		meta, source, err := store.Open(id)
		if err != nil {
			t.Fatalf("legacy snapshot should restore: %v", err)
		}
		_ = source.Close()
		if meta.Version != 0 {
			t.Fatalf("expected version 0, got %d", meta.Version)
		}
		cfg := *DefaultConfig()
		cfg.LocalID = "test"
		legacyFSM := newRecoveryTestFSM()
		r := newRestoreTestRaft(t, legacyFSM, store, cfg)
		if err := r.tryRestoreSingleSnapshot(meta); err != nil {
			t.Fatalf("legacy snapshot restore failed: %v", err)
		}
		if got := legacyFSM.Entries(); !equalStrings(got, []string{"one"}) {
			t.Fatalf("legacy snapshot data mismatch: %v", got)
		}
		t.Logf("legacy snapshot accepted: id=%s version=%d size=%d", meta.ID, meta.Version, meta.Size)
	})
}

func TestFileSnapshotStore_UniqueIDsAndTempCleanup(t *testing.T) {
	base := t.TempDir()
	stale := filepath.Join(base, snapPath, "stale"+tmpSuffix)
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileSnapshotStoreWithLogger(base, 3, newTestLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale temporary snapshot was not removed: %v", err)
	}

	_, trans := NewInmemTransport(NewInmemAddr())
	first, err := store.Create(SnapshotVersionMax, 10, 3, Configuration{}, 0, trans)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Cancel() }()
	second, err := store.Create(SnapshotVersionMax, 10, 3, Configuration{}, 0, trans)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Cancel() }()
	if first.ID() == second.ID() {
		t.Fatalf("concurrent snapshot writers received the same ID %q", first.ID())
	}
	t.Logf("snapshot IDs are unique within the same millisecond: %s != %s", first.ID(), second.ID())
}

func TestSnapshotRestore_InterruptedThenCleanRetry(t *testing.T) {
	dir := t.TempDir()
	sourceEntries := make([]string, 0, 256)
	for i := 0; i < 256; i++ {
		sourceEntries = append(sourceEntries, fmt.Sprintf("entry-%03d", i))
	}
	store, id := createRecoverySnapshot(t, dir, newRecoveryTestFSM(sourceEntries...), 128, 4)

	before, err := store.Verify(id)
	if err != nil {
		t.Fatal(err)
	}

	meta, err := store.readMeta(id)
	if err != nil {
		t.Fatal(err)
	}

	var progressMu sync.Mutex
	var progress []SnapshotRestoreProgress
	cfg := *DefaultConfig()
	cfg.LocalID = "test"
	cfg.SnapshotRestoreProgress = func(p SnapshotRestoreProgress) {
		progressMu.Lock()
		defer progressMu.Unlock()
		progress = append(progress, p)
	}

	failingFSM := newRecoveryTestFSM()
	failingFSM.failAfter = 64
	failingRaft := newRestoreTestRaft(t, failingFSM, store, cfg)
	err = failingRaft.tryRestoreSingleSnapshot(&meta.SnapshotMeta)
	if !errors.Is(err, ErrSnapshotRestoreIncomplete) {
		t.Fatalf("expected ErrSnapshotRestoreIncomplete, got %v", err)
	}
	if len(failingFSM.Entries()) != 64 {
		t.Fatalf("expected 64 partial entries, got %d", len(failingFSM.Entries()))
	}

	pending, err := store.PendingSnapshotRestore()
	if err != nil {
		t.Fatal(err)
	}
	if pending == nil || pending.SnapshotID != id || pending.Phase != "failed" {
		t.Fatalf("expected failed restore journal, got %#v", pending)
	}

	after, err := store.Verify(id)
	if err != nil {
		t.Fatalf("original snapshot was not retained: %v", err)
	}
	if before.Checksum != after.Checksum || before.Size != after.Size {
		t.Fatalf("original snapshot changed: before=%+v after=%+v", before, after)
	}

	restoredFSM := newRecoveryTestFSM()
	restoredRaft := newRestoreTestRaft(t, restoredFSM, store, cfg)
	if err := restoredRaft.reportPendingSnapshotRestore(); err != nil {
		t.Fatal(err)
	}
	if err := restoredRaft.tryRestoreSingleSnapshot(&meta.SnapshotMeta); err != nil {
		t.Fatalf("clean retry failed: %v", err)
	}
	pending, err = store.PendingSnapshotRestore()
	if err != nil {
		t.Fatal(err)
	}
	if pending != nil {
		t.Fatalf("restore journal should be cleared, got %#v", pending)
	}
	if got := restoredFSM.Entries(); !equalStrings(got, sourceEntries) {
		t.Fatalf("restored entries mismatch: got %d want %d", len(got), len(sourceEntries))
	}
	hash1 := restoredFSM.StateHash()

	progressMu.Lock()
	progressCopy := append([]SnapshotRestoreProgress(nil), progress...)
	progressMu.Unlock()
	foundComplete := false
	foundVerify := false
	for _, p := range progressCopy {
		if p.Phase == "verify" && p.SnapshotID == id && p.AppliedEntries == 1 && p.TotalEntries == 1 {
			foundVerify = true
		}
		if p.AppliedEntries == 256 && p.TotalEntries == 256 {
			foundComplete = true
		}
	}
	if !foundComplete {
		t.Fatalf("missing fixed entry progress contract: %#v", progressCopy)
	}
	if !foundVerify {
		t.Fatalf("missing integrity-check progress callback: %#v", progressCopy)
	}

	restoredAgainFSM := newRecoveryTestFSM()
	restoredAgainRaft := newRestoreTestRaft(t, restoredAgainFSM, store, cfg)
	if err := restoredAgainRaft.tryRestoreSingleSnapshot(&meta.SnapshotMeta); err != nil {
		t.Fatalf("second full restore failed: %v", err)
	}
	hash2 := restoredAgainFSM.StateHash()
	if hash1 != hash2 {
		t.Fatalf("state hash changed across restores: %s != %s", hash1, hash2)
	}

	t.Logf("snapshot=%s size=%d crc64-ecma=%s", id, before.Size, before.Checksum)
	t.Logf("progress applied-entries=%d total-entries=%d", progressCopy[len(progressCopy)-1].AppliedEntries, progressCopy[len(progressCopy)-1].TotalEntries)
	t.Logf("state-hash restore-1=%s restore-2=%s equal=%t", hash1, hash2, hash1 == hash2)
}

func TestSnapshotRestore_RejectsOversizedSnapshot(t *testing.T) {
	dir := t.TempDir()
	store, id := createRecoverySnapshot(t, dir, newRecoveryTestFSM("one", "two", "three"), 10, 2)
	meta, err := store.readMeta(id)
	if err != nil {
		t.Fatal(err)
	}

	cfg := *DefaultConfig()
	cfg.LocalID = "test"
	cfg.SnapshotRestoreMaxBytes = 8
	r := newRestoreTestRaft(t, newRecoveryTestFSM(), store, cfg)
	err = r.tryRestoreSingleSnapshot(&meta.SnapshotMeta)
	if !errors.Is(err, ErrSnapshotTooLarge) {
		t.Fatalf("expected ErrSnapshotTooLarge, got %v", err)
	}
	t.Logf("oversized snapshot rejected: %v", err)
	if pending, err := store.PendingSnapshotRestore(); err != nil || pending != nil {
		t.Fatalf("oversized restore should be rejected before FSM restore: pending=%#v err=%v", pending, err)
	}
}

func TestRaft_RestoreLogHandoffAndMembership(t *testing.T) {
	conf := inmemConfig(t)
	conf.TrailingLogs = 100
	conf.RestoreCommittedLogs = true
	var progressMu sync.Mutex
	var progress []SnapshotRestoreProgress
	conf.SnapshotRestoreProgress = func(p SnapshotRestoreProgress) {
		progressMu.Lock()
		defer progressMu.Unlock()
		progress = append(progress, p)
	}
	opts := &MakeClusterOpts{
		Peers:              1,
		Bootstrap:          true,
		Conf:               conf,
		CommitTrackingLogs: true,
	}
	c := MakeClusterCustom(t, opts)
	defer c.Close()

	leader := c.Leader()
	for i := 0; i < 10; i++ {
		future := leader.Apply([]byte(fmt.Sprintf("pre-%02d", i)), 0)
		if err := future.Error(); err != nil {
			t.Fatal(err)
		}
	}
	if err := leader.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}

	for i := 10; i < 20; i++ {
		future := leader.Apply([]byte(fmt.Sprintf("post-%02d", i)), 0)
		if err := future.Error(); err != nil {
			t.Fatal(err)
		}
	}

	const newID = ServerID("new-node")
	newMember := leader.AddNonvoter(newID, ServerAddress("10.20.30.40:8300"), 0, 0)
	if err := newMember.Error(); err != nil {
		t.Fatal(err)
	}
	configIndex := newMember.Index()

	if err := leader.Shutdown().Error(); err != nil {
		t.Fatal(err)
	}

	freshFSM := &MockFSM{}
	cfg := leader.config()
	_, trans := NewInmemTransport(leader.localAddr)
	restarted, err := NewRaft(&cfg, freshFSM, leader.logs, leader.stable, leader.snapshots, trans)
	if err != nil {
		t.Fatalf("restart after snapshot failed: %v", err)
	}
	c.rafts[0] = restarted

	entries := freshFSM.Logs()
	if len(entries) != 20 {
		t.Fatalf("replayed FSM has %d entries, want 20", len(entries))
	}
	for i, entry := range entries {
		prefix := "pre"
		if i >= 10 {
			prefix = "post"
		}
		want := fmt.Sprintf("%s-%02d", prefix, i)
		if string(entry) != want {
			t.Fatalf("entry %d = %q, want %q", i, entry, want)
		}
	}
	if restarted.getLastApplied() < configIndex {
		t.Fatalf("last applied %d is behind membership change %d", restarted.getLastApplied(), configIndex)
	}
	progressMu.Lock()
	progressCopy := append([]SnapshotRestoreProgress(nil), progress...)
	progressMu.Unlock()
	sawLogReplayComplete := false
	for _, p := range progressCopy {
		if p.Phase == "apply-logs" && p.TotalEntries > 0 && p.AppliedEntries == p.TotalEntries {
			sawLogReplayComplete = true
		}
	}
	if !sawLogReplayComplete {
		t.Fatalf("missing completed log replay progress: %#v", progressCopy)
	}

	future := restarted.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, server := range future.Configuration().Servers {
		if server.ID == newID {
			found = true
		}
	}
	if !found {
		t.Fatalf("membership change was lost after restore: %+v", future.Configuration())
	}
	t.Logf("replayed commands=%d last-applied=%d configuration=%+v", len(entries), restarted.getLastApplied(), future.Configuration())

	if err := restarted.Snapshot().Error(); err != nil {
		t.Fatalf("snapshot after restore should include replayed state: %v", err)
	}
}

func TestRaft_RestoreLogGapRefusesStartup(t *testing.T) {
	conf := inmemConfig(t)
	conf.TrailingLogs = 100
	conf.RestoreCommittedLogs = true
	opts := &MakeClusterOpts{
		Peers:              1,
		Bootstrap:          true,
		Conf:               conf,
		CommitTrackingLogs: true,
	}
	c := MakeClusterCustom(t, opts)
	defer c.Close()

	leader := c.Leader()
	for i := 0; i < 5; i++ {
		if err := leader.Apply([]byte(fmt.Sprintf("pre-%d", i)), 0).Error(); err != nil {
			t.Fatal(err)
		}
	}
	if err := leader.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	snaps, err := leader.snapshots.List()
	if err != nil {
		t.Fatal(err)
	}
	snapshotIndex := snaps[0].Index

	for i := 0; i < 5; i++ {
		if err := leader.Apply([]byte(fmt.Sprintf("post-%d", i)), 0).Error(); err != nil {
			t.Fatal(err)
		}
	}
	gapIndex := snapshotIndex + 2
	if err := leader.logs.DeleteRange(gapIndex, gapIndex); err != nil {
		t.Fatal(err)
	}

	if err := leader.Shutdown().Error(); err != nil {
		t.Fatal(err)
	}

	cfg := leader.config()
	_, trans := NewInmemTransport(leader.localAddr)
	_, err = NewRaft(&cfg, &MockFSM{}, leader.logs, leader.stable, leader.snapshots, trans)
	if !errors.Is(err, ErrLogRangeGap) {
		t.Fatalf("expected ErrLogRangeGap, got %v", err)
	}
	var gapErr *LogRangeGapError
	if !errors.As(err, &gapErr) || gapErr.Start != gapIndex || gapErr.End != gapIndex {
		t.Fatalf("unexpected gap details: %#v", gapErr)
	}
	t.Logf("startup refused: %v", err)
}

func createRecoverySnapshot(t *testing.T, dir string, fsm FSM, index, term uint64) (*FileSnapshotStore, string) {
	t.Helper()
	store, err := NewFileSnapshotStoreWithLogger(dir, 3, newTestLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	store.noSync = true
	_, trans := NewInmemTransport(NewInmemAddr())
	sink, err := store.Create(SnapshotVersionMax, index, term, Configuration{}, 0, trans)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	if err := snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}
	return store, sink.ID()
}

func newRestoreTestRaft(t *testing.T, fsm FSM, store SnapshotStore, cfg Config) *Raft {
	t.Helper()
	r := &Raft{
		fsm:       fsm,
		snapshots: store,
		logger:    newTestLogger(t),
	}
	r.conf.Store(cfg)
	return r
}

func rewriteSnapshotMeta(t *testing.T, dir, id string, mutate func(*fileSnapshotMeta)) {
	t.Helper()
	path := filepath.Join(dir, snapPath, id, metaFilePath)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var meta fileSnapshotMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatal(err)
	}
	mutate(&meta)
	data, err = json.Marshal(&meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var _ FSMRestoreProgress = (*recoveryTestFSM)(nil)
var _ FSMStateHasher = (*recoveryTestFSM)(nil)
