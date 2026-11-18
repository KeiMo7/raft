// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"io"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

const (
	snapshotRestoreMonitorInterval = 10 * time.Second
)

type snapshotRestoreMonitor struct {
	logger          hclog.Logger
	cr              CountingReader
	size            int64
	networkTransfer bool
	snapshotID      string
	phase           string
	entryProgress   FSMRestoreProgress
	progressFn      func(SnapshotRestoreProgress)
	started         time.Time

	once   sync.Once
	cancel func()
	doneCh chan struct{}
}

func startSnapshotRestoreMonitor(
	logger hclog.Logger,
	cr CountingReader,
	size int64,
	networkTransfer bool,
) *snapshotRestoreMonitor {
	return startSnapshotRestoreMonitorWithProgress(
		logger, cr, size, networkTransfer, "", "", nil, nil,
	)
}

func startSnapshotRestoreMonitorWithProgress(
	logger hclog.Logger,
	cr CountingReader,
	size int64,
	networkTransfer bool,
	snapshotID string,
	phase string,
	entryProgress FSMRestoreProgress,
	progressFn func(SnapshotRestoreProgress),
) *snapshotRestoreMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	m := &snapshotRestoreMonitor{
		logger:          logger,
		cr:              cr,
		size:            size,
		networkTransfer: networkTransfer,
		snapshotID:      snapshotID,
		phase:           phase,
		entryProgress:   entryProgress,
		progressFn:      progressFn,
		started:         time.Now(),
		cancel:          cancel,
		doneCh:          make(chan struct{}),
	}
	go m.run(ctx)
	m.runOnce()
	return m
}

func (m *snapshotRestoreMonitor) run(ctx context.Context) {
	defer close(m.doneCh)

	ticker := time.NewTicker(snapshotRestoreMonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runOnce()
		}
	}
}

func (m *snapshotRestoreMonitor) runOnce() {
	readBytes := m.cr.Count()
	totalBytes := m.size
	pct := float64(100)
	if totalBytes > 0 {
		pct = float64(100*readBytes) / float64(totalBytes)
		if pct > 100 {
			pct = 100
		}
	}

	appliedEntries, totalEntries := uint64(0), uint64(1)
	if m.entryProgress != nil {
		appliedEntries, totalEntries = m.entryProgress.RestoreProgress()
		if totalEntries == 0 {
			totalEntries = 1
		}
	} else if totalBytes == 0 || readBytes >= totalBytes {
		appliedEntries = 1
	}
	elapsed := time.Since(m.started)

	progress := SnapshotRestoreProgress{
		SnapshotID:     m.snapshotID,
		Phase:          m.phase,
		AppliedEntries: appliedEntries,
		TotalEntries:   totalEntries,
		ReadBytes:      readBytes,
		TotalBytes:     totalBytes,
		Elapsed:        elapsed,
	}
	if m.progressFn != nil {
		m.progressFn(progress)
	}

	message := "snapshot restore progress"
	if m.networkTransfer {
		message = "snapshot network transfer progress"
	}

	m.logger.Info(message,
		"snapshot-id", m.snapshotID,
		"phase", m.phase,
		"read-bytes", readBytes,
		"total-bytes", totalBytes,
		"applied-entries", appliedEntries,
		"total-entries", totalEntries,
		"percent-complete", hclog.Fmt("%0.2f%%", pct),
		"elapsed", elapsed,
	)
}

func (m *snapshotRestoreMonitor) StopAndWait() {
	m.once.Do(func() {
		m.cancel()
		<-m.doneCh
		m.runOnce()
	})
}

type CountingReader interface {
	io.Reader
	Count() int64
}

type countingReader struct {
	reader io.Reader

	mu    sync.Mutex
	bytes int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.mu.Lock()
	r.bytes += int64(n)
	r.mu.Unlock()
	return n, err
}

func (r *countingReader) Count() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bytes
}

func newCountingReader(r io.Reader) *countingReader {
	return &countingReader{reader: r}
}

type countingReadCloser struct {
	*countingReader
	readCloser io.ReadCloser
}

func newCountingReadCloser(rc io.ReadCloser) *countingReadCloser {
	return &countingReadCloser{
		countingReader: newCountingReader(rc),
		readCloser:     rc,
	}
}

func (c countingReadCloser) Close() error {
	return c.readCloser.Close()
}

func (c countingReadCloser) WrappedReadCloser() io.ReadCloser {
	return c.readCloser
}

// ReadCloserWrapper allows access to an underlying ReadCloser from a wrapper.
type ReadCloserWrapper interface {
	io.ReadCloser
	WrappedReadCloser() io.ReadCloser
}

var _ ReadCloserWrapper = &countingReadCloser{}
