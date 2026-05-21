// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package artifact

import (
	"fmt"
	"path"
	"strings"
)

func splitArtifactPath(artifactPath string, allowEmpty bool) ([]string, error) {
	if artifactPath == "" {
		if allowEmpty {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: empty path", ErrInvalidArtifactPath)
	}
	if strings.ContainsAny(artifactPath, "\\\x00") || strings.HasPrefix(artifactPath, "/") {
		return nil, fmt.Errorf("%w: %q", ErrInvalidArtifactPath, artifactPath)
	}
	if path.Clean(artifactPath) != artifactPath {
		return nil, fmt.Errorf("%w: %q", ErrInvalidArtifactPath, artifactPath)
	}
	parts := strings.Split(artifactPath, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, fmt.Errorf("%w: %q", ErrInvalidArtifactPath, artifactPath)
		}
	}
	return parts, nil
}

func normalizeArtifactPath(parts []string) string {
	return strings.Join(parts, "/")
}

func validateBodyRef(ref BodyRef) error {
	if ref.Store == "" || ref.Key == "" {
		return ErrInvalidBodyRef
	}
	return nil
}
