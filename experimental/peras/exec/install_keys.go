// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
)

// PerasSegmentInstallKeys returns the exact metadata keys written by a segment
// install command. The raftstore adapter uses this set as the dependency
// surface for prepared-MVCC installs, so malformed or mismatched routing state
// fails closed instead of producing a partial key set.
func PerasSegmentInstallKeys(segment PerasSegment, routingKey []byte, materialize bool) ([][]byte, error) {
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	if materialize {
		return perasSegmentMaterializeInstallKeys(segment)
	}
	return perasSegmentCatalogInstallKeys(segment, routingKey)
}

// PerasSegmentInstallPlan materializes the compiler install header for a sealed
// segment. Raftstore uses this header for route selection and apply dependency
// planning, so the planner does not have to decode the segment payload.
func PerasSegmentInstallPlan(segment PerasSegment, materialize bool) (compile.InstallPlan, error) {
	if err := validatePerasSegmentPayload(segment); err != nil {
		return compile.InstallPlan{}, err
	}
	if materialize {
		return perasSegmentMaterializeInstallPlan(segment)
	}
	return perasSegmentCatalogInstallPlan(segment)
}

// PerasSegmentCatalogRouteInstallKeys returns the conservative dependency
// surface for one catalog-only install route without decoding the full segment
// payload. The route always writes the bucket-local index key. The route object
// key is included as a false dependency for non-canonical routes so routing and
// apply admission serialize every route through the same visible key surface.
func PerasSegmentCatalogRouteInstallKeys(root [32]byte, routingKey []byte) ([][]byte, error) {
	if root == ([32]byte{}) || len(routingKey) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	parts, ok := layout.InspectKey(routingKey)
	if !ok || parts.Kind != layout.KeyKindSegment || parts.SegmentRecord != layout.SegmentRecordObject || parts.SegmentRoot != root {
		return nil, ErrInvalidPerasSegment
	}
	indexKey, err := layout.EncodeSegmentCatalogIndexKey(parts.MountKeyID, parts.Bucket, root)
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, 2)
	keys = appendUniquePerasInstallKey(keys, routingKey)
	keys = appendUniquePerasInstallKey(keys, indexKey)
	return keys, nil
}

func perasSegmentMaterializeInstallPlan(segment PerasSegment) (compile.InstallPlan, error) {
	routingKey, err := segment.FirstKey()
	if err != nil {
		return compile.InstallPlan{}, err
	}
	dependencyKeys, err := perasSegmentMaterializeInstallKeys(segment)
	if err != nil {
		return compile.InstallPlan{}, err
	}
	materializedKeys := make([][]byte, 0, len(segment.entries))
	if err := segment.ForEachEntry(func(entry SegmentKV) error {
		if len(entry.Key) == 0 {
			return ErrInvalidPerasSegment
		}
		materializedKeys = appendUniquePerasInstallKey(materializedKeys, entry.Key)
		return nil
	}); err != nil {
		return compile.InstallPlan{}, err
	}
	catalogKeys, canonicalObjectKey, err := perasSegmentCatalogHeaderKeys(segment)
	if err != nil {
		return compile.InstallPlan{}, err
	}
	return compile.InstallPlan{
		Mode:               compile.SegmentInstallSingleBucket,
		Materialize:        true,
		RoutingKeys:        [][]byte{routingKey},
		DependencyKeys:     dependencyKeys,
		CatalogKeys:        catalogKeys,
		MaterializedKeys:   materializedKeys,
		CanonicalObjectKey: canonicalObjectKey,
	}, nil
}

func perasSegmentCatalogInstallPlan(segment PerasSegment) (compile.InstallPlan, error) {
	routingKeys, err := PerasSegmentCatalogObjectKeys(segment)
	if err != nil {
		return compile.InstallPlan{}, err
	}
	catalogKeys, canonicalObjectKey, err := perasSegmentCatalogHeaderKeys(segment)
	if err != nil {
		return compile.InstallPlan{}, err
	}
	return compile.InstallPlan{
		Mode:               compile.SegmentInstallCatalog,
		Materialize:        false,
		RoutingKeys:        routingKeys,
		DependencyKeys:     catalogKeys,
		CatalogKeys:        catalogKeys,
		CanonicalObjectKey: canonicalObjectKey,
	}, nil
}

func perasSegmentCatalogHeaderKeys(segment PerasSegment) ([][]byte, []byte, error) {
	canonicalObjectKey, err := PerasSegmentObjectKey(segment)
	if err != nil {
		return nil, nil, err
	}
	catalogKeys := make([][]byte, 0, 4)
	catalogKeys = appendUniquePerasInstallKey(catalogKeys, canonicalObjectKey)
	indexKeys, err := PerasSegmentCatalogIndexKeys(segment)
	if err != nil {
		return nil, nil, err
	}
	for _, key := range indexKeys {
		catalogKeys = appendUniquePerasInstallKey(catalogKeys, key)
	}
	return catalogKeys, canonicalObjectKey, nil
}

func perasSegmentMaterializeInstallKeys(segment PerasSegment) ([][]byte, error) {
	keys := make([][]byte, 0, len(segment.entries)+2)
	err := segment.ForEachEntry(func(entry SegmentKV) error {
		if len(entry.Key) == 0 {
			return ErrInvalidPerasSegment
		}
		keys = appendUniquePerasInstallKey(keys, entry.Key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	objectKey, err := PerasSegmentObjectKey(segment)
	if err != nil {
		return nil, err
	}
	keys = appendUniquePerasInstallKey(keys, objectKey)
	indexKeys, err := PerasSegmentCatalogIndexKeys(segment)
	if err != nil {
		return nil, err
	}
	for _, key := range indexKeys {
		keys = appendUniquePerasInstallKey(keys, key)
	}
	return keys, nil
}

func perasSegmentCatalogInstallKeys(segment PerasSegment, routingKey []byte) ([][]byte, error) {
	if len(routingKey) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	if !perasSegmentCatalogObjectKeyMatches(segment, routingKey) {
		return nil, ErrInvalidPerasSegment
	}
	objectKey, err := PerasSegmentObjectKey(segment)
	if err != nil {
		return nil, err
	}
	parts, ok := layout.InspectKey(routingKey)
	if !ok || parts.Kind != layout.KeyKindSegment || parts.SegmentRecord != layout.SegmentRecordObject || parts.SegmentRoot != segment.Root {
		return nil, ErrInvalidPerasSegment
	}
	indexKey, err := layout.EncodeSegmentCatalogIndexKey(parts.MountKeyID, parts.Bucket, segment.Root)
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, 2)
	keys = appendUniquePerasInstallKey(keys, objectKey)
	keys = appendUniquePerasInstallKey(keys, indexKey)
	return keys, nil
}

func perasSegmentCatalogObjectKeyMatches(segment PerasSegment, key []byte) bool {
	objectKeys, err := PerasSegmentCatalogObjectKeys(segment)
	if err != nil {
		return false
	}
	for _, objectKey := range objectKeys {
		if bytes.Equal(objectKey, key) {
			return true
		}
	}
	return false
}

func appendUniquePerasInstallKey(keys [][]byte, key []byte) [][]byte {
	if len(key) == 0 {
		return keys
	}
	for _, existing := range keys {
		if bytes.Equal(existing, key) {
			return keys
		}
	}
	return append(keys, append([]byte(nil), key...))
}
