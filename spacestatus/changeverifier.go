package spacestatus

import (
	"github.com/anytypeio/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anytypeio/any-sync/commonspace/settings"
)

type ChangeVerifier interface {
	Verify(rawDelete *treechangeproto.RawTreeChangeWithId, identity []byte, peerId string) (err error)
}

var getChangeVerifier = newChangeVerifier

func newChangeVerifier() ChangeVerifier {
	return &changeVerifier{}
}

type changeVerifier struct {
}

func (c *changeVerifier) Verify(rawDelete *treechangeproto.RawTreeChangeWithId, identity []byte, peerId string) (err error) {
	return settings.VerifyDeleteChange(rawDelete, identity, peerId)
}
