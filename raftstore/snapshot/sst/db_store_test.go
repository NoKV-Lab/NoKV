// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package sst_test

import (
	"testing"

	local "github.com/feichai0017/NoKV/local"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	snapshot "github.com/feichai0017/NoKV/raftstore/snapshot/sst"
	"github.com/stretchr/testify/require"
)

func TestDBStoreRequiresOpenDB(t *testing.T) {
	var nilDB *local.DB
	_, err := snapshot.NewDBStore(nilDB).ExportSnapshot(localmeta.RegionMeta{})
	require.ErrorContains(t, err, "requires open db")

	db := openSnapshotDB(t)
	require.NoError(t, db.Close())
	_, err = snapshot.NewDBStore(db).ImportSnapshot([]byte("not-a-real-payload"))
	require.ErrorContains(t, err, "requires open db")
}
