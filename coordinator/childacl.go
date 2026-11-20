package coordinator

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
)

// parentPermissions holds what each account may do in the parent of a child space.
type parentPermissions map[string]list.AclPermissions

func (p parentPermissions) of(account crypto.PubKey) list.AclPermissions {
	return p[string(account.Storage())]
}

// childRecord checks one acl record of a child space against the parent of the space.
type childRecord struct {
	// parent could not be read when parentErr is set. Only a record that depends on the parent is
	// refused for it, so the members of a child can still be removed and its keys rotated.
	parent    parentPermissions
	parentErr error
	// author signed the record
	author crypto.PubKey
}

// newChildRecord reads the permissions in the parent before the acl of the child is locked for the
// record. Under that lock the read could wait for the consensus stream to load the parent, while the
// stream waits for the lock to deliver a record of the child.
func (c *coordinator) newChildRecord(ctx context.Context, parentSpaceId string, rec *consensusproto.RawRecord) (*childRecord, error) {
	author, err := recordAuthor(rec)
	if err != nil {
		return nil, err
	}
	r := &childRecord{author: author}
	r.parentErr = c.acl.ReadState(ctx, parentSpaceId, func(parent *list.AclState) error {
		accounts := parent.CurrentAccounts()
		r.parent = make(parentPermissions, len(accounts))
		for _, account := range accounts {
			r.parent[string(account.PubKey.Storage())] = account.Permissions
		}
		return nil
	})
	if errors.Is(r.parentErr, consensuserr.ErrLogNotFound) {
		r.parentErr = coordinatorproto.ErrParentSpaceNotAvailable
	}
	return r, nil
}

// recordAuthor returns the account that signed the record. The validation of the record verifies the
// signature against it.
func recordAuthor(rec *consensusproto.RawRecord) (crypto.PubKey, error) {
	record := &consensusproto.Record{}
	if err := record.UnmarshalVT(rec.Payload); err != nil {
		return nil, err
	}
	return crypto.UnmarshalEd25519PublicKeyProto(record.Identity)
}

// afterValid refuses a valid record that the parent does not allow.
func (r *childRecord) afterValid(before, after *list.AclState) error {
	return r.checkAdmission(before, after)
}

// isMember tells a member of a space from the identity of a guest key and from an account without
// permissions.
func isMember(permissions list.AclPermissions) bool {
	return !permissions.NoPermissions() && !permissions.IsGuest()
}

// checkAdmission refuses a record that makes an account a member of the child, or posts its request
// to join, when the account is not a member of the parent. A request is checked when it is posted and
// again when it is accepted, as the account can leave the parent in between. The identity of a guest
// key is a member of neither space: being in the parent it opens no child, and it gets into a child
// from outside the parent only when the owner of the child lets it in.
func (r *childRecord) checkAdmission(before, after *list.AclState) error {
	byOwner := before.Permissions(r.author).IsOwner()
	var entering []crypto.PubKey
	for _, account := range after.CurrentAccounts() {
		was := before.Permissions(account.PubKey)
		switch {
		case isMember(account.Permissions) && !isMember(was):
			entering = append(entering, account.PubKey)
		case account.Permissions.IsGuest() && was.NoPermissions() && !byOwner:
			entering = append(entering, account.PubKey)
		}
	}
	requests, err := after.JoinRecords(false)
	if err != nil {
		return err
	}
	for _, request := range requests {
		if pending, err := before.Record(request.RequestIdentity); err != nil || pending.RecordId != request.RecordId {
			entering = append(entering, request.RequestIdentity)
		}
	}
	for _, account := range entering {
		if r.parentErr != nil {
			return r.parentErr
		}
		if !isMember(r.parent.of(account)) {
			return coordinatorproto.ErrNotParentMember
		}
	}
	return nil
}
