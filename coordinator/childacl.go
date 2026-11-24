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
	// byParent is set when the record leans on the authority of the parent: its author does in the
	// child what no permissions in the child allow
	byParent bool
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

// authorize tells what the parent lets the author of the record do in the child: an Admin or the
// Owner of the parent removes members, the Owner changes the owner.
func (r *childRecord) authorize(author crypto.PubKey) (list.ParentAuthority, error) {
	if r.parentErr != nil {
		return list.ParentAuthority{}, r.parentErr
	}
	r.byParent = true
	permissions := r.parent.of(author)
	return list.ParentAuthority{
		RemoveMembers: permissions.CanManageAccounts(),
		ChangeOwner:   permissions.IsOwner(),
	}, nil
}

// afterValid refuses a valid record that the parent does not allow.
func (r *childRecord) afterValid(before, after *list.AclState) error {
	if err := r.checkAdmission(before, after); err != nil {
		return err
	}
	if err := r.checkNewOwner(before, after); err != nil {
		return err
	}
	if r.byParent {
		return r.checkRemovalRank(before, after)
	}
	return nil
}

// refusal names the error of a record that failed validation. A record on the authority of the
// parent that lacks permissions answers with the error of the parent: its author's role there is too
// low. The Owner of the parent has every authority, so its record lacks them for another reason.
func (r *childRecord) refusal(err error) error {
	if r.byParent && !r.parent.of(r.author).IsOwner() && errors.Is(err, list.ErrInsufficientPermissions) {
		return coordinatorproto.ErrParentPermissionDenied
	}
	return err
}

// checkRemovalRank refuses a removal on the authority of the parent of an account the author could
// not remove from the parent itself: there only the Owner removes an Admin, and nobody the Owner.
func (r *childRecord) checkRemovalRank(before, after *list.AclState) error {
	if r.parent.of(r.author).IsOwner() {
		return nil
	}
	for _, account := range before.CurrentAccounts() {
		if account.Permissions.NoPermissions() || !after.Permissions(account.PubKey).NoPermissions() {
			continue
		}
		if removed := r.parent.of(account.PubKey); removed.IsOwner() || removed.IsAdmin() {
			return coordinatorproto.ErrParentPermissionDenied
		}
	}
	return nil
}

// checkNewOwner refuses a record that makes an account the owner of the child when the account is
// not a member of the parent, whoever writes it: the owner of the child or the Owner of the parent.
func (r *childRecord) checkNewOwner(before, after *list.AclState) error {
	previous, err := before.OwnerPubKey()
	if err != nil {
		return err
	}
	owner, err := after.OwnerPubKey()
	if err != nil {
		return err
	}
	if owner.Equals(previous) {
		return nil
	}
	if r.parentErr != nil {
		return r.parentErr
	}
	if !isMember(r.parent.of(owner)) {
		return coordinatorproto.ErrNotParentMember
	}
	return nil
}

// isMember tells a member of a space from the identity of a guest key and from an account without
// permissions.
func isMember(permissions list.AclPermissions) bool {
	return !permissions.NoPermissions() && !permissions.IsGuest()
}

// checkAdmission refuses a record that makes an account a member of the child, or posts its request
// to join, when the account is not a member of the parent. A request is checked when it is posted and
// again when it is accepted, as the account can leave the parent in between.
func (r *childRecord) checkAdmission(before, after *list.AclState) error {
	var entering []crypto.PubKey
	for _, account := range after.CurrentAccounts() {
		if isMember(account.Permissions) && !isMember(before.Permissions(account.PubKey)) {
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
