// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

const (
	testPath      = "permTest"
	snapPath      = "snapshots"
	metaFilePath  = "meta.json"
	stateFilePath = "state.bin"
	tmpSuffix     = ".tmp"

	restoreStateFile    = ".restore-state.json"
	restoreStateTmpFile = ".restore-state.json.tmp"
	snapshotHashBufSize = 64 * 1024
	maxSnapshotMetaSize = 4 * 1024 * 1024
)

// FileSnapshotStore implements the SnapshotStore interface and allows
// snapshots to be made on the local disk.
type FileSnapshotStore struct {
	path   string
	retain int
	logger hclog.Logger
	// createMu makes snapshot ID selection and temporary directory creation
	// atomic when the snapshot thread and an install-snapshot RPC race.
	createMu sync.Mutex
	// restoreMu serializes the small restore journal written outside snapshot
	// directories. Snapshot state files themselves are never modified.
	restoreMu sync.Mutex
	// progressMu protects the optional restore progress callback.
	progressMu sync.RWMutex
	progressFn func(SnapshotRestoreProgress)

	// noSync, if true, skips crash-safe file fsync api calls.
	// It's a private field, only used in testing
	noSync bool
}

type snapMetaSlice []*fileSnapshotMeta

// FileSnapshotSink implements SnapshotSink with a file.
type FileSnapshotSink struct {
	store     *FileSnapshotStore
	logger    hclog.Logger
	dir       string
	parentDir string
	meta      fileSnapshotMeta

	noSync bool

	stateFile *os.File
	stateHash hash.Hash64
	buffered  *bufio.Writer

	closed bool
}

// fileSnapshotMeta is stored on disk. We also put a CRC
// on disk so that we can verify the snapshot.
type fileSnapshotMeta struct {
	SnapshotMeta
	CRC []byte
}

// bufferedFile is returned when we open a snapshot. This way
// reads are buffered and the file still gets closed.
type bufferedFile struct {
	bh *bufio.Reader
	fh *os.File
}

func (b *bufferedFile) Read(p []byte) (n int, err error) {
	return b.bh.Read(p)
}

func (b *bufferedFile) Close() error {
	return b.fh.Close()
}

// NewFileSnapshotStoreWithLogger creates a new FileSnapshotStore based
// on a base directory. The `retain` parameter controls how many
// snapshots are retained. Must be at least 1.
func NewFileSnapshotStoreWithLogger(base string, retain int, logger hclog.Logger) (*FileSnapshotStore, error) {
	if retain < 1 {
		return nil, fmt.Errorf("must retain at least one snapshot")
	}
	if logger == nil {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "snapshot",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}

	// Ensure our path exists
	path := filepath.Join(base, snapPath)
	if err := os.MkdirAll(path, 0o755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("snapshot path not accessible: %v", err)
	}

	// Setup the store
	store := &FileSnapshotStore{
		path:   path,
		retain: retain,
		logger: logger,
	}

	// Do a permissions test
	if err := store.testPermissions(); err != nil {
		return nil, fmt.Errorf("permissions test failed: %v", err)
	}
	if err := store.cleanupTemporarySnapshots(); err != nil {
		return nil, fmt.Errorf("failed to clean temporary snapshots: %v", err)
	}
	return store, nil
}

func (f *FileSnapshotStore) cleanupTemporarySnapshots() error {
	entries, err := os.ReadDir(f.path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, tmpSuffix) {
			continue
		}
		path := filepath.Join(f.path, name)
		f.logger.Warn("removing stale temporary snapshot artifact", "path", path)
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	return nil
}

// NewFileSnapshotStore creates a new FileSnapshotStore based
// on a base directory. The `retain` parameter controls how many
// snapshots are retained. Must be at least 1.
func NewFileSnapshotStore(base string, retain int, logOutput io.Writer) (*FileSnapshotStore, error) {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	return NewFileSnapshotStoreWithLogger(base, retain, hclog.New(&hclog.LoggerOptions{
		Name:   "snapshot",
		Output: logOutput,
		Level:  hclog.DefaultLevel,
	}))
}

// testPermissions tries to touch a file in our path to see if it works.
func (f *FileSnapshotStore) testPermissions() error {
	path := filepath.Join(f.path, testPath)
	fh, err := os.Create(path)
	if err != nil {
		return err
	}

	if err = fh.Close(); err != nil {
		return err
	}

	if err = os.Remove(path); err != nil {
		return err
	}
	return nil
}

// snapshotName generates a name for the snapshot.
func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}

func uniqueSnapshotName(path string, term, index uint64) string {
	base := snapshotName(term, index)
	name := base
	for suffix := 1; ; suffix++ {
		_, finalErr := os.Stat(filepath.Join(path, name))
		_, tmpErr := os.Stat(filepath.Join(path, name+tmpSuffix))
		if (finalErr != nil && !errors.Is(finalErr, os.ErrNotExist)) ||
			(tmpErr != nil && !errors.Is(tmpErr, os.ErrNotExist)) {
			return name
		}
		if errors.Is(finalErr, os.ErrNotExist) && errors.Is(tmpErr, os.ErrNotExist) {
			return name
		}
		name = base + "-" + strconv.Itoa(suffix)
	}
}

// Create is used to start a new snapshot
func (f *FileSnapshotStore) Create(version SnapshotVersion, index, term uint64,
	configuration Configuration, configurationIndex uint64, trans Transport) (SnapshotSink, error) {
	// We only support version 1 snapshots at this time.
	if version != 1 {
		return nil, fmt.Errorf("unsupported snapshot version %d", version)
	}

	// Create a new path. Serialize name selection and temporary directory
	// creation so a concurrent snapshot and install-snapshot cannot race to the
	// same ID.
	f.createMu.Lock()
	name := uniqueSnapshotName(f.path, term, index)
	path := filepath.Join(f.path, name+tmpSuffix)
	f.logger.Info("creating new snapshot", "path", path)

	// Make the directory
	if err := os.MkdirAll(path, 0o755); err != nil {
		f.createMu.Unlock()
		f.logger.Error("failed to make snapshot directly", "error", err)
		return nil, err
	}
	f.createMu.Unlock()

	// Create the sink
	sink := &FileSnapshotSink{
		store:     f,
		logger:    f.logger,
		dir:       path,
		parentDir: f.path,
		noSync:    f.noSync,
		meta: fileSnapshotMeta{
			SnapshotMeta: SnapshotMeta{
				Version:            version,
				ID:                 name,
				Index:              index,
				Term:               term,
				Peers:              encodePeers(configuration, trans),
				Configuration:      configuration,
				ConfigurationIndex: configurationIndex,
			},
			CRC: nil,
		},
	}

	// Write out the meta data
	if err := sink.writeMeta(); err != nil {
		f.logger.Error("failed to write metadata", "error", err)
		return nil, err
	}

	// Open the state file
	statePath := filepath.Join(path, stateFilePath)
	fh, err := os.Create(statePath)
	if err != nil {
		f.logger.Error("failed to create state file", "error", err)
		return nil, err
	}
	sink.stateFile = fh

	// Create a CRC64 hash
	sink.stateHash = crc64.New(crc64.MakeTable(crc64.ECMA))

	// Wrap both the hash and file in a MultiWriter with buffering
	multi := io.MultiWriter(sink.stateFile, sink.stateHash)
	sink.buffered = bufio.NewWriter(multi)

	// Done
	return sink, nil
}

// List returns available snapshots in the store.
func (f *FileSnapshotStore) List() ([]*SnapshotMeta, error) {
	// Get the eligible snapshots
	snapshots, err := f.getSnapshots()
	if err != nil {
		f.logger.Error("failed to get snapshots", "error", err)
		return nil, err
	}

	var snapMeta []*SnapshotMeta
	for _, meta := range snapshots {
		snapMeta = append(snapMeta, &meta.SnapshotMeta)
		if len(snapMeta) == f.retain {
			break
		}
	}
	return snapMeta, nil
}

// getSnapshots returns all the known snapshots.
func (f *FileSnapshotStore) getSnapshots() ([]*fileSnapshotMeta, error) {
	// Get the eligible snapshots
	snapshots, err := os.ReadDir(f.path)
	if err != nil {
		f.logger.Error("failed to scan snapshot directory", "error", err)
		return nil, err
	}

	// Populate the metadata
	var snapMeta []*fileSnapshotMeta
	for _, snap := range snapshots {
		// Ignore any files
		if !snap.IsDir() {
			continue
		}

		// Ignore any temporary snapshots
		dirName := snap.Name()
		if strings.HasSuffix(dirName, tmpSuffix) {
			f.logger.Warn("found temporary snapshot", "name", dirName)
			continue
		}

		// Try to read the meta data
		meta, err := f.readMeta(dirName)
		if err != nil {
			return nil, &SnapshotIntegrityError{
				ID:     dirName,
				Reason: "failed to read metadata",
				Err:    err,
			}
		}

		// Make sure we can understand this version.
		if err := validateSnapshotVersion(dirName, meta.Version); err != nil {
			return nil, err
		}
		if meta.Version == 0 {
			f.logger.Warn("found legacy snapshot; it can be restored but the next snapshot will use version 1",
				"name", dirName)
		}

		// Append, but only return up to the retain count
		snapMeta = append(snapMeta, meta)
	}

	// Sort the snapshot, reverse so we get new -> old
	sort.Sort(sort.Reverse(snapMetaSlice(snapMeta)))

	return snapMeta, nil
}

// readMeta is used to read the meta data for a given named backup
func (f *FileSnapshotStore) readMeta(name string) (*fileSnapshotMeta, error) {
	// Open the meta file
	metaPath := filepath.Join(f.path, name, metaFilePath)
	fh, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = fh.Close() }()

	stat, err := fh.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() > maxSnapshotMetaSize {
		return nil, &SnapshotIntegrityError{
			ID:     name,
			Reason: fmt.Sprintf("metadata exceeds the %d byte limit", maxSnapshotMetaSize),
		}
	}

	// Buffer the file IO
	buffered := bufio.NewReader(fh)

	// Read in the JSON
	meta := &fileSnapshotMeta{}
	dec := json.NewDecoder(buffered)
	if err := dec.Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// Open takes a snapshot ID and returns a ReadCloser for that snapshot.
func (f *FileSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	// Get the metadata
	meta, err := f.readMeta(id)
	if err != nil {
		f.logger.Error("failed to get meta data to open snapshot", "error", err)
		return nil, nil, err
	}
	if err := validateSnapshotVersion(id, meta.Version); err != nil {
		return nil, nil, err
	}

	// Open the state file
	statePath := filepath.Join(f.path, id, stateFilePath)
	fh, err := os.Open(statePath)
	if err != nil {
		f.logger.Error("failed to open state file", "error", err)
		return nil, nil, err
	}

	stat, err := fh.Stat()
	if err != nil {
		_ = fh.Close()
		return nil, nil, err
	}
	if stat.Size() != meta.Size {
		_ = fh.Close()
		return nil, nil, &SnapshotIntegrityError{
			ID:           id,
			Reason:       "state file size does not match metadata (file is truncated or has trailing data)",
			ExpectedSize: meta.Size,
			ActualSize:   stat.Size(),
		}
	}

	// Create a CRC64 hash
	stateHash := crc64.New(crc64.MakeTable(crc64.ECMA))

	// Compute the hash
	counted := newCountingReader(fh)
	verifyMonitor := startSnapshotRestoreMonitorWithProgress(
		f.logger, counted, stat.Size(), false, id, "verify", nil, f.getSnapshotRestoreProgress(),
	)
	_, err = io.CopyBuffer(stateHash, counted, make([]byte, snapshotHashBufSize))
	verifyMonitor.StopAndWait()
	if err != nil {
		f.logger.Error("failed to read state file", "error", err)
		_ = fh.Close()
		return nil, nil, err
	}

	// Verify the hash
	computed := stateHash.Sum(nil)
	if !bytes.Equal(meta.CRC, computed) {
		f.logger.Error("CRC checksum failed", "stored", meta.CRC, "computed", computed)
		_ = fh.Close()
		return nil, nil, &SnapshotIntegrityError{
			ID:           id,
			Reason:       "CRC64 checksum does not match metadata",
			ExpectedSize: meta.Size,
			ActualSize:   stat.Size(),
			ExpectedCRC:  hex.EncodeToString(meta.CRC),
			ActualCRC:    hex.EncodeToString(computed),
		}
	}

	// Seek to the start
	if _, err := fh.Seek(0, 0); err != nil {
		f.logger.Error("state file seek failed", "error", err)
		_ = fh.Close()
		return nil, nil, err
	}

	// Return a buffered file
	buffered := &bufferedFile{
		bh: bufio.NewReader(fh),
		fh: fh,
	}

	f.logger.Info("snapshot integrity verified",
		"id", id,
		"version", meta.Version,
		"size-in-bytes", meta.Size,
		"crc64-ecma", hex.EncodeToString(computed),
	)

	return &meta.SnapshotMeta, buffered, nil
}

// Verify performs a read-only size and checksum validation of a snapshot. It
// never modifies or removes the snapshot.
func (f *FileSnapshotStore) Verify(id string) (*SnapshotVerification, error) {
	start := time.Now()
	meta, source, err := f.Open(id)
	if err != nil {
		return nil, err
	}
	if err := source.Close(); err != nil {
		return nil, err
	}

	fh, err := os.Open(filepath.Join(f.path, id, stateFilePath))
	if err != nil {
		return nil, err
	}
	defer func() { _ = fh.Close() }()
	stateHash := crc64.New(crc64.MakeTable(crc64.ECMA))
	if _, err := io.CopyBuffer(stateHash, fh, make([]byte, snapshotHashBufSize)); err != nil {
		return nil, err
	}

	return &SnapshotVerification{
		ID:       id,
		Version:  meta.Version,
		Size:     meta.Size,
		Checksum: hex.EncodeToString(stateHash.Sum(nil)),
		Duration: time.Since(start),
	}, nil
}

// SetSnapshotRestoreProgress configures the callback used for integrity-check
// progress emitted by Open. It may be updated while snapshots are being read.
func (f *FileSnapshotStore) SetSnapshotRestoreProgress(progressFn func(SnapshotRestoreProgress)) {
	f.progressMu.Lock()
	f.progressFn = progressFn
	f.progressMu.Unlock()
}

func (f *FileSnapshotStore) getSnapshotRestoreProgress() func(SnapshotRestoreProgress) {
	f.progressMu.RLock()
	defer f.progressMu.RUnlock()
	return f.progressFn
}

// PendingSnapshotRestore returns the last restore journal entry. A non-nil
// result means a previous process did not finish restoring the FSM and a clean
// rebuild from the original snapshot is required.
func (f *FileSnapshotStore) PendingSnapshotRestore() (*SnapshotRestoreState, error) {
	f.restoreMu.Lock()
	defer f.restoreMu.Unlock()
	return f.readRestoreStateLocked()
}

func (f *FileSnapshotStore) beginSnapshotRestore(meta *SnapshotMeta) error {
	f.restoreMu.Lock()
	defer f.restoreMu.Unlock()

	now := time.Now()
	state := &SnapshotRestoreState{
		SnapshotID: meta.ID,
		Index:      meta.Index,
		Term:       meta.Term,
		Phase:      "fsm-restore",
		StartedAt:  now,
		UpdatedAt:  now,
	}
	return f.writeRestoreStateLocked(state)
}

func (f *FileSnapshotStore) finishSnapshotRestore(id string) error {
	f.restoreMu.Lock()
	defer f.restoreMu.Unlock()

	state, err := f.readRestoreStateLocked()
	if err != nil {
		return err
	}
	if state == nil || state.SnapshotID != id {
		return nil
	}
	err = os.Remove(filepath.Join(f.path, restoreStateFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (f *FileSnapshotStore) failSnapshotRestore(id string, restoreErr error) error {
	f.restoreMu.Lock()
	defer f.restoreMu.Unlock()

	state, err := f.readRestoreStateLocked()
	if err != nil {
		return err
	}
	if state == nil {
		state = &SnapshotRestoreState{
			SnapshotID: id,
			StartedAt:  time.Now(),
		}
	}
	state.Phase = "failed"
	state.UpdatedAt = time.Now()
	if restoreErr != nil {
		state.LastError = restoreErr.Error()
	}
	return f.writeRestoreStateLocked(state)
}

func (f *FileSnapshotStore) readRestoreStateLocked() (*SnapshotRestoreState, error) {
	path := filepath.Join(f.path, restoreStateFile)
	stat, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if stat.Size() > maxSnapshotMetaSize {
		return nil, fmt.Errorf("snapshot restore journal exceeds %d bytes", maxSnapshotMetaSize)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	state := &SnapshotRestoreState{}
	if err := json.Unmarshal(data, state); err != nil {
		return nil, fmt.Errorf("failed to read snapshot restore journal: %w", err)
	}
	return state, nil
}

func (f *FileSnapshotStore) writeRestoreStateLocked(state *SnapshotRestoreState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	tmpPath := filepath.Join(f.path, restoreStateTmpFile)
	finalPath := filepath.Join(f.path, restoreStateFile)
	fh, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := fh.Write(data); err != nil {
		_ = fh.Close()
		return err
	}
	if !f.noSync {
		if err := fh.Sync(); err != nil {
			_ = fh.Close()
			return err
		}
	}
	if err := fh.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}
	if !f.noSync && runtime.GOOS != "windows" {
		dir, err := os.Open(f.path)
		if err != nil {
			return err
		}
		defer func() { _ = dir.Close() }()
		if err := dir.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// ReapSnapshots reaps any snapshots beyond the retain count.
func (f *FileSnapshotStore) ReapSnapshots() error {
	snapshots, err := f.getSnapshots()
	if err != nil {
		f.logger.Error("failed to get snapshots", "error", err)
		return err
	}

	for i := f.retain; i < len(snapshots); i++ {
		path := filepath.Join(f.path, snapshots[i].ID)
		f.logger.Info("reaping snapshot", "path", path)
		if err := os.RemoveAll(path); err != nil {
			f.logger.Error("failed to reap snapshot", "path", path, "error", err)
			return err
		}
	}
	return nil
}

// ID returns the ID of the snapshot, can be used with Open()
// after the snapshot is finalized.
func (s *FileSnapshotSink) ID() string {
	return s.meta.ID
}

// Write is used to append to the state file. We write to the
// buffered IO object to reduce the amount of context switches.
func (s *FileSnapshotSink) Write(b []byte) (int, error) {
	return s.buffered.Write(b)
}

// Close is used to indicate a successful end.
func (s *FileSnapshotSink) Close() error {
	// Make sure close is idempotent
	if s.closed {
		return nil
	}
	s.closed = true

	// Close the open handles
	if err := s.finalize(); err != nil {
		s.logger.Error("failed to finalize snapshot", "error", err)
		if delErr := os.RemoveAll(s.dir); delErr != nil {
			s.logger.Error("failed to delete temporary snapshot directory", "path", s.dir, "error", delErr)
			return delErr
		}
		return err
	}

	// Write out the meta data
	if err := s.writeMeta(); err != nil {
		s.logger.Error("failed to write metadata", "error", err)
		return err
	}

	// Move the directory into place
	newPath := strings.TrimSuffix(s.dir, tmpSuffix)
	if err := os.Rename(s.dir, newPath); err != nil {
		s.logger.Error("failed to move snapshot into place", "error", err)
		return err
	}

	if !s.noSync && runtime.GOOS != "windows" { // skipping fsync for directory entry edits on Windows, only needed for *nix style file systems
		parentFH, err := os.Open(s.parentDir)
		if err != nil {
			s.logger.Error("failed to open snapshot parent directory", "path", s.parentDir, "error", err)
			return err
		}
		defer func() { _ = parentFH.Close() }()

		if err = parentFH.Sync(); err != nil {
			s.logger.Error("failed syncing parent directory", "path", s.parentDir, "error", err)
			return err
		}
	}

	// Reap any old snapshots
	if err := s.store.ReapSnapshots(); err != nil {
		return err
	}

	return nil
}

// Cancel is used to indicate an unsuccessful end.
func (s *FileSnapshotSink) Cancel() error {
	// Make sure close is idempotent
	if s.closed {
		return nil
	}
	s.closed = true

	// Close the open handles
	if err := s.finalize(); err != nil {
		s.logger.Error("failed to finalize snapshot", "error", err)
		return err
	}

	// Attempt to remove all artifacts
	return os.RemoveAll(s.dir)
}

// finalize is used to close all of our resources.
func (s *FileSnapshotSink) finalize() error {
	// Flush any remaining data
	if err := s.buffered.Flush(); err != nil {
		return err
	}

	// Sync to force fsync to disk
	if !s.noSync {
		if err := s.stateFile.Sync(); err != nil {
			return err
		}
	}

	// Get the file size
	stat, statErr := s.stateFile.Stat()

	// Close the file
	if err := s.stateFile.Close(); err != nil {
		return err
	}

	// Set the file size, check after we close
	if statErr != nil {
		return statErr
	}
	s.meta.Size = stat.Size()

	// Set the CRC
	s.meta.CRC = s.stateHash.Sum(nil)
	return nil
}

// writeMeta is used to write out the metadata we have.
func (s *FileSnapshotSink) writeMeta() error {
	var err error
	// Open the meta file
	metaPath := filepath.Join(s.dir, metaFilePath)
	var fh *os.File
	fh, err = os.Create(metaPath)
	if err != nil {
		return err
	}
	defer func() { _ = fh.Close() }()

	// Buffer the file IO
	buffered := bufio.NewWriter(fh)

	// Write out as JSON
	enc := json.NewEncoder(buffered)
	if err = enc.Encode(&s.meta); err != nil {
		return err
	}

	if err = buffered.Flush(); err != nil {
		return err
	}

	if !s.noSync {
		if err = fh.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// Implement the sort interface for []*fileSnapshotMeta.
func (s snapMetaSlice) Len() int {
	return len(s)
}

func (s snapMetaSlice) Less(i, j int) bool {
	if s[i].Term != s[j].Term {
		return s[i].Term < s[j].Term
	}
	if s[i].Index != s[j].Index {
		return s[i].Index < s[j].Index
	}
	return s[i].ID < s[j].ID
}

func (s snapMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
