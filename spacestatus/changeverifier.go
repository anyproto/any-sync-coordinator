package spacestatus

import (
	"fmt"

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

func VerifySpaceHeader(identity crypto.PubKey, headerBytes []byte) (spaceType SpaceType, err error) {
	rawHeader := &spacesyncproto.RawSpaceHeader{}
	if err = rawHeader.UnmarshalVT(headerBytes); err != nil {
		return
	}

	header := &spacesyncproto.SpaceHeader{}
	if err = header.UnmarshalVT(rawHeader.SpaceHeader); err != nil {
		return
	}

	// todo: check signate for onetoone
	if header.SpaceType != oneToOneSpaceType {
		ok, err := identity.Verify(rawHeader.SpaceHeader, rawHeader.Signature)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("space header signature mismatched")
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
