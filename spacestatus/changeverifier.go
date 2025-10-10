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
)

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
func VerifySpaceHeader(identity crypto.PubKey, headerBytes []byte) (spaceType SpaceType, err error) {
	rawHeader := &spacesyncproto.RawSpaceHeader{}
	if err = rawHeader.UnmarshalVT(headerBytes); err != nil {
		return
	}

	header := &spacesyncproto.SpaceHeader{}
	if err = header.UnmarshalVT(rawHeader.SpaceHeader); err != nil {
		return
	}

	if header.SpaceType == oneToOneSpaceType {
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
	case techSpaceType:
		return SpaceTypeTech, nil
	case chatSpaceType:
		return SpaceTypeRegular, nil
	case oneToOneSpaceType:
		return SpaceTypeOneToOne, nil
	case "", regularSpaceType:
		if header.Timestamp == 0 {
			return SpaceTypePersonal, nil
		}
		return SpaceTypeRegular, nil
	default:
		return 0, fmt.Errorf("unknown space type: %s", header.SpaceType)
	}

}
