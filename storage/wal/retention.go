// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal

import "fmt"

// RetentionMark declares the oldest WAL segment still needed by a participant.
type RetentionMark struct {
	FirstSegment uint32
}

// RetentionFunc reports a participant's current WAL retention mark.
type RetentionFunc func() RetentionMark

// RegisterRetention registers a named WAL retention participant.
func (m *Manager) RegisterRetention(name string, fn RetentionFunc) error {
	if m == nil {
		return fmt.Errorf("wal: manager is nil")
	}
	if name == "" {
		return fmt.Errorf("wal: retention participant name required")
	}
	if fn == nil {
		return fmt.Errorf("wal: retention participant %q is nil", name)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.retention == nil {
		m.retention = make(map[string]RetentionFunc, 4)
	}
	m.retention[name] = fn
	return nil
}

// UnregisterRetention removes a WAL retention participant.
func (m *Manager) UnregisterRetention(name string) {
	if m == nil || name == "" {
		return
	}
	m.mu.Lock()
	delete(m.retention, name)
	m.mu.Unlock()
}

// CanRemoveSegment reports whether every retention participant has released id.
func (m *Manager) CanRemoveSegment(id uint32) bool {
	if m == nil || id == 0 {
		return false
	}
	m.mu.Lock()
	activeID := m.activeID
	participants := make([]RetentionFunc, 0, len(m.retention))
	for _, fn := range m.retention {
		participants = append(participants, fn)
	}
	m.mu.Unlock()

	if activeID > 0 && id >= activeID {
		return false
	}
	for _, fn := range participants {
		mark := fn()
		if mark.FirstSegment > 0 && id >= mark.FirstSegment {
			return false
		}
	}
	return true
}
