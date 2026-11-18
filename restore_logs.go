// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"fmt"
	"time"
)

const restoreLogProgressInterval = time.Second

// validateLogRange verifies that every index in the inclusive range can be
// read before recovery starts applying entries. Reporting a missing range is
// safer than discovering a gap after a partial FSM rebuild.
func (r *Raft) validateLogRange(start, end uint64) error {
	return validateLogRange(r.logs, start, end)
}

func validateLogRange(logs LogStore, start, end uint64) error {
	if start > end {
		return nil
	}

	first, err := logs.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %w", err)
	}
	if first == 0 || first > start {
		gapEnd := end
		if first > 0 && first-1 < gapEnd {
			gapEnd = first - 1
		}
		return &LogRangeGapError{Start: start, End: gapEnd}
	}

	for index := start; index <= end; index++ {
		var entry Log
		err := logs.GetLog(index, &entry)
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrLogNotFound) {
			return fmt.Errorf("failed to read log at index %d: %w", index, err)
		}

		gapEnd := index
		for gapEnd < end {
			var next Log
			if err := logs.GetLog(gapEnd+1, &next); err == nil {
				break
			} else if !errors.Is(err, ErrLogNotFound) {
				return fmt.Errorf("failed to read log at index %d: %w", gapEnd+1, err)
			}
			gapEnd++
		}
		return &LogRangeGapError{Start: index, End: gapEnd}
	}
	return nil
}

func (r *Raft) replayCommittedLogs(start, end uint64) error {
	if start > end {
		return nil
	}
	if err := r.validateLogRange(start, end); err != nil {
		return err
	}

	cfg := r.config()
	maxBatch := cfg.MaxAppendEntries
	if maxBatch <= 0 {
		maxBatch = 1
	}

	total := end - start + 1
	applied := uint64(0)
	lastProgress := time.Now()
	replayStart := time.Now()

	reportProgress := func(force bool) {
		if !force && time.Since(lastProgress) < restoreLogProgressInterval {
			return
		}
		lastProgress = time.Now()
		progress := SnapshotRestoreProgress{
			SnapshotID:     r.restoredSnapshotID,
			Phase:          "apply-logs",
			AppliedEntries: applied,
			TotalEntries:   total,
			Elapsed:        time.Since(replayStart),
		}
		if cfg.SnapshotRestoreProgress != nil {
			cfg.SnapshotRestoreProgress(progress)
		}
		r.logger.Info("snapshot restore log replay progress",
			"snapshot-id", progress.SnapshotID,
			"phase", progress.Phase,
			"applied-entries", progress.AppliedEntries,
			"total-entries", progress.TotalEntries,
			"elapsed", progress.Elapsed,
		)
	}
	reportProgress(true)

	batch := make([]*Log, 0, maxBatch)
	flush := func(lastIndex uint64) error {
		if len(batch) == 0 {
			return nil
		}
		if err := r.applyRecoveryBatch(batch); err != nil {
			return err
		}
		batch = batch[:0]
		r.setLastApplied(lastIndex)
		applied = lastIndex - start + 1
		reportProgress(false)
		return nil
	}

	for index := start; index <= end; index++ {
		var entry Log
		if err := r.logs.GetLog(index, &entry); err != nil {
			if errors.Is(err, ErrLogNotFound) {
				return &LogRangeGapError{Start: index, End: index}
			}
			return fmt.Errorf("failed to read log at index %d: %w", index, err)
		}

		if err := r.processConfigurationLogEntry(&entry); err != nil {
			return fmt.Errorf("failed to recover configuration at index %d: %w", index, err)
		}

		switch entry.Type {
		case LogCommand, LogConfiguration:
			batch = append(batch, &entry)
		}
		if len(batch) >= maxBatch {
			if err := flush(index); err != nil {
				return err
			}
		}
	}
	if err := flush(end); err != nil {
		return err
	}
	r.setLastApplied(end)
	applied = total
	reportProgress(true)
	r.logger.Info("snapshot restore log replay complete",
		"snapshot-id", r.restoredSnapshotID,
		"from-index", start,
		"to-index", end,
		"applied-entries", total,
		"total-entries", total,
		"duration", time.Since(replayStart),
	)
	return nil
}

func (r *Raft) applyRecoveryBatch(logs []*Log) error {
	if batchingFSM, ok := r.fsm.(BatchingFSM); ok {
		sendLogs := make([]*Log, 0, len(logs))
		for _, entry := range logs {
			switch entry.Type {
			case LogCommand, LogConfiguration:
				sendLogs = append(sendLogs, entry)
			}
		}
		if len(sendLogs) == 0 {
			return nil
		}
		responses := batchingFSM.ApplyBatch(sendLogs)
		if len(responses) != len(sendLogs) {
			return fmt.Errorf("invalid number of responses from ApplyBatch: got %d, want %d", len(responses), len(sendLogs))
		}
		return nil
	}

	configStore, hasConfigStore := r.fsm.(ConfigurationStore)
	for _, entry := range logs {
		switch entry.Type {
		case LogCommand:
			_ = r.fsm.Apply(entry)
		case LogConfiguration:
			if hasConfigStore {
				configStore.StoreConfiguration(entry.Index, DecodeConfiguration(entry.Data))
			}
		}
	}
	return nil
}

func (r *Raft) scanConfigurationLogs(start, end uint64) error {
	if start > end {
		return nil
	}
	if err := r.validateLogRange(start, end); err != nil {
		return err
	}
	for index := start; index <= end; index++ {
		var entry Log
		if err := r.logs.GetLog(index, &entry); err != nil {
			if errors.Is(err, ErrLogNotFound) {
				return &LogRangeGapError{Start: index, End: index}
			}
			return fmt.Errorf("failed to read log at index %d: %w", index, err)
		}
		if err := r.processConfigurationLogEntry(&entry); err != nil {
			return fmt.Errorf("failed to process configuration at index %d: %w", index, err)
		}
	}
	return nil
}
