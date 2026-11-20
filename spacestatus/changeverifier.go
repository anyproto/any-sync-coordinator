package spacestatus

import (
	"fmt"

	"github.com/anyproto/any-sync/commonspace/object/acl/aclrecordproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/settings"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
)

type ChangeVerifier interface {
	Verify(change StatusChange) (err error)
}

var getChangeVerifier = newChangeVerifier

func newChangeVerifier() ChangeVerifier {
	return &changeVerifier{}
}

type changeVerifier struct {
}

func (c *changeVerifier) Verify(change StatusChange) (err error) {
	switch change.DeletionPayloadType {
	case coordinatorproto.DeletionPayloadType_Tree:
		rawDelete := &treechangeproto.RawTreeChangeWithId{
			RawChange: change.DeletionPayload,
			Id:        change.DeletionPayloadId,
		}
		return settings.VerifyDeleteChange(rawDelete, change.Identity, change.PeerId)
	case coordinatorproto.DeletionPayloadType_Confirm:
		var confirmSig = new(coordinatorproto.DeletionConfirmPayloadWithSignature)
		if err = confirmSig.UnmarshalVT(change.DeletionPayload); err != nil {
			return err
		}
		return coordinatorproto.ValidateDeleteConfirmation(change.Identity, change.SpaceId, change.NetworkId, confirmSig)
	case coordinatorproto.DeletionPayloadType_Account:
		var confirmSig = new(coordinatorproto.DeletionConfirmPayloadWithSignature)
		if err = confirmSig.UnmarshalVT(change.DeletionPayload); err != nil {
			return err
		}
		return coordinatorproto.ValidateAccountDeleteConfirmation(change.Identity, change.SpaceId, change.NetworkId, confirmSig)
	}
	return coordinatorproto.ErrUnexpected
}

const (
	regularSpaceType  = "anytype.space"
	techSpaceType     = "anytype.techspace"
	chatSpaceType     = "anytype.chatspace"
	oneToOneSpaceType = "anytype.onetoone"
	// any.* types are the `any` product's counterparts of the anytype
	// types above; same coordinator treatment, but they require
	// fileproto v2 in the header. The 1-1 variant derives a different
	// space id than anytype's, so 1-1s never pair across products.
	anySpaceType         = "any.space"
	anyTechSpaceType     = "any.techspace"
	anyOneToOneSpaceType = "any.onetoone"
)

// canNest reports whether a space of this class and header type can be a parent or a child space.
// Only the regular space of the `any` product can: every other type, and a derived space, which
// carries the same header type, cannot.
func canNest(spaceType SpaceType, headerType string) bool {
	return spaceType == SpaceTypeRegular && headerType == anySpaceType
}

// CheckParent tells whether a space can be the parent of a space that is being signed: it is not
// deleted, and it is a top-level space that can be nested.
func CheckParent(parent StatusEntry) error {
	if parent.Status != SpaceStatusCreated {
		return coordinatorproto.ErrParentSpaceNotAvailable
	}
	if parent.ParentSpaceId != "" || !canNest(parent.Type, parent.HeaderType) {
		return coordinatorproto.ErrParentSpaceInvalid
	}
	return nil
}

// HeaderParent returns the id of the parent space the header names, empty for a top-level space.
// spaceType and headerType are what VerifySpaceHeader returned for the header. A header that names a
// parent is refused unless it is a v1 header of a space that can be nested.
func HeaderParent(headerBytes []byte, spaceType SpaceType, headerType string) (parentSpaceId string, err error) {
	rawHeader := &spacesyncproto.RawSpaceHeader{}
	if err = rawHeader.UnmarshalVT(headerBytes); err != nil {
		return
	}
	header := &spacesyncproto.SpaceHeader{}
	if err = header.UnmarshalVT(rawHeader.SpaceHeader); err != nil {
		return
	}
	if header.ParentSpaceId == "" {
		return "", nil
	}
	if header.Version != spacesyncproto.SpaceHeaderVersion_SpaceHeaderVersion1 || !canNest(spaceType, headerType) {
		return "", coordinatorproto.ErrParentSpaceInvalid
	}
	return header.ParentSpaceId, nil
}

func verifyHeaderSignatureOneToOne(identity crypto.PubKey, rawHeader *spacesyncproto.RawSpaceHeader) (err error) {
	var header spacesyncproto.SpaceHeader
	err = header.UnmarshalVT(rawHeader.SpaceHeader)
	if err != nil {
		return
	}

	var oneToOneInfo aclrecordproto.AclOneToOneInfo
	err = oneToOneInfo.UnmarshalVT(header.SpaceHeaderPayload)
	if err != nil {
		return
	}

	ownerIdentity, err := crypto.UnmarshalEd25519PublicKeyProto(oneToOneInfo.Owner)
	if err != nil {
		return
	}

	// oneToOne space is signed by sharedSk, Owner of the space
	ok, err := ownerIdentity.Verify(rawHeader.SpaceHeader, rawHeader.Signature)
	if err != nil {
		return
	}
	if !ok {
		return fmt.Errorf("space header signature mismatched")
	}

	if len(oneToOneInfo.Writers) != 2 {
		return fmt.Errorf("verify oneToOne signature check: oneToOne space should have exactly two writers")
	}

	// check if identity is one of the writers.
	// first, unmarshal both to check if they are pubkeys
	writer0, err := crypto.UnmarshalEd25519PublicKeyProto(oneToOneInfo.Writers[0])
	if err != nil {
		return
	}
	writer1, err := crypto.UnmarshalEd25519PublicKeyProto(oneToOneInfo.Writers[1])
	if err != nil {
		return
	}

	if !identity.Equals(writer0) && !identity.Equals(writer1) {
		return fmt.Errorf("verify oneToOne signature check: identity must be in one of the writers")
	}

	return nil
}

func verifyHeaderSignature(identity crypto.PubKey, rawHeader *spacesyncproto.RawSpaceHeader) (err error) {
	ok, err := identity.Verify(rawHeader.SpaceHeader, rawHeader.Signature)
	if err != nil {
		return
	}
	if !ok {
		return fmt.Errorf("space header signature mismatched")
	}

	return nil

}
// VerifySpaceHeader checks the header signature and maps the header's
// space type string to the stored SpaceType enum. headerType is the raw
// string from the header, returned for persistence and logging.
func VerifySpaceHeader(identity crypto.PubKey, headerBytes []byte) (spaceType SpaceType, headerType string, err error) {
	rawHeader := &spacesyncproto.RawSpaceHeader{}
	if err = rawHeader.UnmarshalVT(headerBytes); err != nil {
		return
	}

	header := &spacesyncproto.SpaceHeader{}
	if err = header.UnmarshalVT(rawHeader.SpaceHeader); err != nil {
		return
	}
	headerType = header.SpaceType

	if header.SpaceType == oneToOneSpaceType || header.SpaceType == anyOneToOneSpaceType {
		err = verifyHeaderSignatureOneToOne(identity, rawHeader)
		if err != nil {
			return
		}
	} else {
		err = verifyHeaderSignature(identity, rawHeader)
		if err != nil {
			return
		}

	}

	switch header.SpaceType {
	case anySpaceType, anyTechSpaceType, anyOneToOneSpaceType:
		// any.* spaces are files-v2 only
		if header.FileprotoVersion != spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2 {
			err = fmt.Errorf("space type %s requires fileproto version %d, got %d",
				header.SpaceType, spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2, header.FileprotoVersion)
			return
		}
	}

	switch header.SpaceType {
	case techSpaceType, anyTechSpaceType:
		return SpaceTypeTech, headerType, nil
	case chatSpaceType:
		return SpaceTypeRegular, headerType, nil
	case oneToOneSpaceType, anyOneToOneSpaceType:
		return SpaceTypeOneToOne, headerType, nil
	case "", regularSpaceType, anySpaceType:
		if header.Timestamp == 0 {
			return SpaceTypePersonal, headerType, nil
		}
		return SpaceTypeRegular, headerType, nil
	default:
		return 0, headerType, fmt.Errorf("unknown space type: %s", header.SpaceType)
	}

}
