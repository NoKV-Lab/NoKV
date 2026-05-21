// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package artifact

import (
	"encoding/json"
	"fmt"

	"github.com/feichai0017/NoKV/fsmeta"
)

const opaqueSchemaArtifactV1 = "nokv.artifact.v1"

type opaqueAttrsV1 struct {
	Schema string  `json:"schema"`
	Body   BodyRef `json:"body"`
}

func encodeArtifactOpaqueAttrs(ref BodyRef) ([]byte, error) {
	if err := validateBodyRef(ref); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(opaqueAttrsV1{
		Schema: opaqueSchemaArtifactV1,
		Body:   ref,
	})
	if err != nil {
		return nil, err
	}
	if len(payload) > fsmeta.MaxInodeOpaqueAttrsBytes {
		return nil, fmt.Errorf("%w: opaque attrs exceed fsmeta limit", ErrInvalidArtifactMetadata)
	}
	return payload, nil
}

func decodeArtifactOpaqueAttrs(payload []byte) (BodyRef, error) {
	if len(payload) == 0 {
		return BodyRef{}, fmt.Errorf("%w: missing opaque attrs", ErrInvalidArtifactMetadata)
	}
	var attrs opaqueAttrsV1
	if err := json.Unmarshal(payload, &attrs); err != nil {
		return BodyRef{}, fmt.Errorf("%w: %v", ErrInvalidArtifactMetadata, err)
	}
	if attrs.Schema != opaqueSchemaArtifactV1 {
		return BodyRef{}, fmt.Errorf("%w: unsupported schema %q", ErrInvalidArtifactMetadata, attrs.Schema)
	}
	if err := validateBodyRef(attrs.Body); err != nil {
		return BodyRef{}, fmt.Errorf("%w: %v", ErrInvalidArtifactMetadata, err)
	}
	return attrs.Body, nil
}
