package spacestatus

import (
	"fmt"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/settings"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
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
	if change.DeletionPayloadType == coordinatorproto.DeletionPayloadType_Tree {
		var rawDelete = new(treechangeproto.RawTreeChangeWithId)
		if err = rawDelete.Unmarshal(change.DeletionPayload); err != nil {
			return err
		}
		return settings.VerifyDeleteChange(rawDelete, change.Identity, change.PeerId)
	}
	if change.DeletionPayloadType == coordinatorproto.DeletionPayloadType_Confirm {
		var confirmSig = new(coordinatorproto.DeletionConfirmPayloadWithSignature)
		if err = confirmSig.Unmarshal(change.DeletionPayload); err != nil {
			return err
		}

		var ok bool
		ok, err = change.Identity.Verify(confirmSig.DeletionPayload, confirmSig.Signature)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("invalid payload signature")
		}
		var confirm = new(coordinatorproto.DeletionConfirmPayload)
		if err = confirm.Unmarshal(confirmSig.DeletionPayload); err != nil {
			return err
		}

		// TODO: validate confirm payload
	}
	return
}
