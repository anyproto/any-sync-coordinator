package spacestatus

import (
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/settings"
	"github.com/anyproto/any-sync/util/crypto"
)

type ChangeVerifier interface {
	Verify(rawDelete *treechangeproto.RawTreeChangeWithId, identity crypto.PubKey, peerId string) (err error)
}

var getChangeVerifier = newChangeVerifier

func newChangeVerifier() ChangeVerifier {
	return &changeVerifier{}
}

type changeVerifier struct {
}

func (c *changeVerifier) Verify(rawDelete *treechangeproto.RawTreeChangeWithId, identity crypto.PubKey, peerId string) (err error) {
	return settings.VerifyDeleteChange(rawDelete, identity, peerId)
}
