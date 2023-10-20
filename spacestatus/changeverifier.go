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
		if err = confirmSig.Unmarshal(change.DeletionPayload); err != nil {
			return err
		}
		return coordinatorproto.ValidateDeleteConfirmation(change.Identity, change.SpaceId, change.NetworkId, confirmSig)
	case coordinatorproto.DeletionPayloadType_Account:
		var confirmSig = new(coordinatorproto.DeletionConfirmPayloadWithSignature)
		if err = confirmSig.Unmarshal(change.DeletionPayload); err != nil {
			return err
		}
		return coordinatorproto.ValidateAccountDeleteConfirmation(change.Identity, change.SpaceId, change.NetworkId, confirmSig)
	}
	return coordinatorproto.ErrUnexpected
}

const techSpaceType = "anytype.techspace"

func VerifySpaceHeader(identity crypto.PubKey, headerBytes []byte) (spaceType SpaceType, err error) {
	rawHeader := &spacesyncproto.RawSpaceHeader{}
	if err = rawHeader.Unmarshal(headerBytes); err != nil {
		return
	}

	ok, err := identity.Verify(rawHeader.SpaceHeader, rawHeader.Signature)
	if err != nil {
		return
	}
	if !ok {
		return 0, fmt.Errorf("space header signature mismatched")
	}

	header := &spacesyncproto.SpaceHeader{}
	if err = header.Unmarshal(rawHeader.SpaceHeader); err != nil {
		return
	}
	if header.SpaceType == techSpaceType {
		return SpaceTypeTech, nil
	}
	if header.Timestamp == 0 {
		return SpaceTypePersonal, nil
	}
	return SpaceTypeRegular, nil
}
