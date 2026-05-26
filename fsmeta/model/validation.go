// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package model

func ValidateMountID(id MountID) error {
	if id == "" {
		return ErrInvalidMountID
	}
	return nil
}

func ValidateMountKeyID(id MountKeyID) error {
	if id == 0 {
		return ErrInvalidMountID
	}
	return nil
}

func ValidateMountIdentity(identity MountIdentity) error {
	if err := ValidateMountID(identity.MountID); err != nil {
		return err
	}
	return ValidateMountKeyID(identity.MountKeyID)
}

func ValidateMountIdentityForRequest(identity MountIdentity, mount MountID) error {
	if err := ValidateMountIdentity(identity); err != nil {
		return err
	}
	if mount != "" && mount != identity.MountID {
		return ErrInvalidMountID
	}
	return nil
}

func ValidateInodeID(id InodeID) error {
	if id == 0 {
		return ErrInvalidInodeID
	}
	return nil
}

func ValidateName(name string) error {
	if name == "" || name == "." || name == ".." {
		return ErrInvalidName
	}
	for i := 0; i < len(name); i++ {
		switch name[i] {
		case '/', 0:
			return ErrInvalidName
		}
	}
	return nil
}

func ValidateSessionID(id SessionID) error {
	if id == "" {
		return ErrInvalidSession
	}
	for i := 0; i < len(id); i++ {
		if id[i] == 0 {
			return ErrInvalidSession
		}
	}
	return nil
}

func ValidateInodeType(typ InodeType) error {
	switch typ {
	case "", InodeTypeFile, InodeTypeDirectory:
		return nil
	default:
		return ErrInvalidValue
	}
}

func ValidateCreateAttrs(attrs CreateAttrs) error {
	if err := ValidateInodeType(attrs.Type); err != nil {
		return err
	}
	if len(attrs.OpaqueAttrs) > MaxInodeOpaqueAttrsBytes {
		return ErrInvalidValue
	}
	return nil
}

func ValidateCreateRequest(req CreateRequest) error {
	if err := ValidateMountID(req.Mount); err != nil {
		return err
	}
	if err := ValidateInodeID(req.Parent); err != nil {
		return err
	}
	if err := ValidateName(req.Name); err != nil {
		return err
	}
	return ValidateCreateAttrs(req.Attrs)
}

func ValidateRenameRequest(req RenameRequest) error {
	if err := ValidateMountID(req.Mount); err != nil {
		return err
	}
	if err := ValidateInodeID(req.FromParent); err != nil {
		return err
	}
	if err := ValidateInodeID(req.ToParent); err != nil {
		return err
	}
	if err := ValidateName(req.FromName); err != nil {
		return err
	}
	if err := ValidateName(req.ToName); err != nil {
		return err
	}
	if req.FromParent == req.ToParent && req.FromName == req.ToName {
		return ErrInvalidRequest
	}
	return nil
}

func ValidateRenameReplaceRequest(req RenameReplaceRequest) error {
	return ValidateRenameRequest(RenameRequest(req))
}

func NormalizeReadDirLimit(limit uint32) (uint32, error) {
	if limit == 0 {
		return DefaultReadDirLimit, nil
	}
	if limit > MaxReadDirLimit {
		return 0, ErrInvalidPageSize
	}
	return limit, nil
}

func NormalizeSessionExpireLimit(limit uint32) (uint32, error) {
	if limit == 0 {
		return DefaultSessionExpireLimit, nil
	}
	if limit > MaxSessionExpireLimit {
		return 0, ErrInvalidPageSize
	}
	return limit, nil
}

func ValidateSnapshotValue(token SnapshotSubtreeToken) error {
	if err := ValidateMountID(token.Mount); err != nil {
		return err
	}
	if err := ValidateMountKeyID(token.MountKeyID); err != nil {
		return err
	}
	if err := ValidateInodeID(token.RootInode); err != nil {
		return err
	}
	if token.ReadVersion == 0 {
		return ErrInvalidValue
	}
	for _, ref := range token.RuntimeEvidence {
		if !ref.Valid() {
			return ErrInvalidValue
		}
	}
	return nil
}
