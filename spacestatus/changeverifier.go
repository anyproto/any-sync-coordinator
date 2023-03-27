package spacestatus

import (
	"github.com/anytypeio/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anytypeio/any-sync/commonspace/settings"
	"github.com/anytypeio/any-sync/util/crypto"
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
