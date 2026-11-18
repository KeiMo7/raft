// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrSnapshotCorrupt is returned when a snapshot's size or checksum does
	// not match the metadata written when the snapshot was created.
	ErrSnapshotCorrupt = errors.New("snapshot integrity validation failed")

	// ErrSnapshotVersion is returned when a snapshot version cannot be
	// understood by this Raft build.
	ErrSnapshotVersion = errors.New("unsupported snapshot version")

	// ErrSnapshotTooLarge is returned when a snapshot exceeds the configured
	// restore size limit.
	ErrSnapshotTooLarge = errors.New("snapshot exceeds restore size limit")

	// ErrSnapshotRestoreIncomplete is returned after an FSM restore fails
	// part-way through. The original snapshot is not modified and can be
	// retried after restarting with a fresh FSM.
	ErrSnapshotRestoreIncomplete = errors.New("snapshot restore was interrupted")

	// ErrLogRangeGap is returned when the log entries needed to advance from
	// a restored snapshot (or from the beginning of the log) are not present.
	ErrLogRangeGap = errors.New("required log range has a gap")
)

// SnapshotIntegrityError describes a snapshot that failed size or checksum
// validation. The snapshot is left in place so the operator can inspect or
// retry it.
type SnapshotIntegrityError struct {
	ID           string
	Reason       string
	ExpectedSize int64
	ActualSize   int64
	ExpectedCRC  string
	ActualCRC    string
	Err          error
}

func (e *SnapshotIntegrityError) Error() string {
	msg := fmt.Sprintf("snapshot %q integrity check failed: %s", e.ID, e.Reason)
	if e.ExpectedSize != 0 || e.ActualSize != 0 {
		msg += fmt.Sprintf(" (expected-size=%d actual-size=%d)", e.ExpectedSize, e.ActualSize)
	}
	if e.ExpectedCRC != "" || e.ActualCRC != "" {
		msg += fmt.Sprintf(" (expected-crc=%s actual-crc=%s)", e.ExpectedCRC, e.ActualCRC)
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *SnapshotIntegrityError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrSnapshotCorrupt}
	}
	return []error{ErrSnapshotCorrupt, e.Err}
}

// SnapshotVersionError describes a snapshot version that is not understood by
// this build, along with the supported range and an upgrade/downgrade hint.
type SnapshotVersionError struct {
	ID      string
	Version SnapshotVersion
	Min     SnapshotVersion
	Max     SnapshotVersion
}

func (e *SnapshotVersionError) Error() string {
	guidance := "upgrade the Raft library to a version that supports this snapshot"
	if e.Version < e.Min {
		guidance = "create a snapshot with a Raft version that supports this snapshot before downgrading"
	}
	return fmt.Sprintf(
		"snapshot %q has unsupported version %d (supported versions %d-%d); %s",
		e.ID, e.Version, e.Min, e.Max, guidance,
	)
}

func (e *SnapshotVersionError) Unwrap() error {
	return ErrSnapshotVersion
}

// SnapshotTooLargeError describes a snapshot rejected by the configured
// restore limit. It is checked before the snapshot is copied or loaded.
type SnapshotTooLargeError struct {
	ID    string
	Size  int64
	Limit int64
}

func (e *SnapshotTooLargeError) Error() string {
	return fmt.Sprintf("snapshot %q requires %d bytes, exceeding the restore limit of %d bytes", e.ID, e.Size, e.Limit)
}

func (e *SnapshotTooLargeError) Unwrap() error {
	return ErrSnapshotTooLarge
}

// LogRangeGapError reports the inclusive range of log entries that are missing
// and prevent a safe recovery. Both endpoints are one-based Raft log indexes.
type LogRangeGapError struct {
	Start uint64
	End   uint64
}

func (e *LogRangeGapError) Error() string {
	if e.Start == e.End {
		return fmt.Sprintf("required log range has a gap: missing index %d", e.Start)
	}
	return fmt.Sprintf("required log range has a gap: missing indexes %d-%d", e.Start, e.End)
}

func (e *LogRangeGapError) Unwrap() error {
	return ErrLogRangeGap
}

// SnapshotVerification contains the result of a read-only snapshot integrity
// check. Checksum is the CRC64/ECMA value stored in the existing meta.json.
type SnapshotVerification struct {
	ID       string
	Version  SnapshotVersion
	Size     int64
	Checksum string
	Duration time.Duration
}

// SnapshotRestoreState is persisted outside of the snapshot directory while an
// FSM restore is in progress. Its presence after a restart means the previous
// process stopped before the FSM restore completed, so recovery must begin
// again from the original, immutable snapshot.
type SnapshotRestoreState struct {
	SnapshotID string    `json:"snapshot_id"`
	Index      uint64    `json:"index"`
	Term       uint64    `json:"term"`
	Phase      string    `json:"phase"`
	StartedAt  time.Time `json:"started_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	LastError  string    `json:"last_error,omitempty"`
}

// SnapshotRestoreProgress is the stable progress contract reported by the
// restore path. AppliedEntries and TotalEntries count snapshot entries when
// the FSM implements FSMRestoreProgress, and count Raft log entries while
// replaying logs after a snapshot. If the FSM only exposes a byte stream, the
// snapshot body is counted as one entry so the fields remain meaningful.
type SnapshotRestoreProgress struct {
	SnapshotID     string
	Phase          string
	AppliedEntries uint64
	TotalEntries   uint64
	ReadBytes      int64
	TotalBytes     int64
	Elapsed        time.Duration
}

// FSMRestoreProgress is an optional interface an FSM can implement to expose
// entry-level progress while its existing Restore method is running. It is
// polled concurrently with Restore and must be safe for concurrent use.
type FSMRestoreProgress interface {
	RestoreProgress() (appliedEntries, totalEntries uint64)
}

// FSMStateHasher is an optional interface used to emit a deterministic state
// hash after a successful restore. Repeated restores of the same snapshot
// should produce the same hash for a deterministic FSM.
type FSMStateHasher interface {
	StateHash() string
}

type snapshotRestoreJournal interface {
	beginSnapshotRestore(meta *SnapshotMeta) error
	finishSnapshotRestore(id string) error
	failSnapshotRestore(id string, err error) error
	PendingSnapshotRestore() (*SnapshotRestoreState, error)
}

func configureSnapshotRestoreProgress(snaps SnapshotStore, progressFn func(SnapshotRestoreProgress)) {
	if setter, ok := snaps.(interface {
		SetSnapshotRestoreProgress(func(SnapshotRestoreProgress))
	}); ok {
		setter.SetSnapshotRestoreProgress(progressFn)
	}
}

func validateSnapshotVersion(id string, version SnapshotVersion) error {
	if version < SnapshotVersionMin || version > SnapshotVersionMax {
		return &SnapshotVersionError{
			ID:      id,
			Version: version,
			Min:     SnapshotVersionMin,
			Max:     SnapshotVersionMax,
		}
	}
	return nil
}

func checkSnapshotRestoreLimit(id string, size, limit int64) error {
	if size < 0 {
		return &SnapshotIntegrityError{
			ID:     id,
			Reason: "snapshot metadata contains a negative size",
		}
	}
	if limit > 0 && size > limit {
		return &SnapshotTooLargeError{ID: id, Size: size, Limit: limit}
	}
	return nil
}
