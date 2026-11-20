package coordinator

import (
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-coordinator/spacestatus"
)

const testChildId = "child.id"

// family is a parent space and one child of it, with the coordinator on the real acl service
type family struct {
	*fixture
	// accounts of the parent by their role in it
	parentOwner, parentAdmin, otherAdmin, parentWriter, parentReader *accountdata.AccountKeys
	// childOwner is a Writer of the parent
	childOwner *accountdata.AccountKeys
	// outsider is in neither space
	outsider *accountdata.AccountKeys
}

func newFamily(t *testing.T) *family {
	f := &family{
		fixture:      newAclFixture(t),
		parentOwner:  newAccount(t),
		parentAdmin:  newAccount(t),
		otherAdmin:   newAccount(t),
		parentWriter: newAccount(t),
		parentReader: newAccount(t),
		childOwner:   newAccount(t),
		outsider:     newAccount(t),
	}
	t.Cleanup(func() { f.finish(t) })

	f.newAcl(t, testParentId, f.parentOwner)
	f.putMembers(t, testParentId, f.parentOwner, list.AclPermissionsAdmin, f.parentAdmin, f.otherAdmin)
	f.putMembers(t, testParentId, f.parentOwner, list.AclPermissionsWriter, f.parentWriter, f.childOwner)
	f.putMembers(t, testParentId, f.parentOwner, list.AclPermissionsReader, f.parentReader)
	f.newAcl(t, testChildId, f.childOwner)

	f.expectSpace(spacestatus.StatusEntry{
		SpaceId:       testChildId,
		Identity:      f.childOwner.SignKey.GetPublic().Account(),
		Type:          spacestatus.SpaceTypeRegular,
		HeaderType:    testAnySpaceType,
		IsShareable:   true,
		ParentSpaceId: testParentId,
	})
	f.aclEventLog.EXPECT().AddLog(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return f
}

// childBuilder builds the next record of the child as the account
func (f *family) childBuilder(t *testing.T, account *accountdata.AccountKeys) list.AclRecordBuilder {
	return f.aclOf(t, testChildId, account).RecordBuilder()
}

// addToChild sends a record of the child to the coordinator as the author
func (f *family) addToChild(t *testing.T, author *accountdata.AccountKeys, rec *consensusproto.RawRecord, buildErr error) error {
	_, err := f.addRecord(t, testChildId, author, rec, buildErr)
	return err
}

// add adds accounts to the child directly, as its owner
func (f *family) add(t *testing.T, permissions list.AclPermissions, accounts ...*accountdata.AccountKeys) error {
	return f.addAs(t, f.childOwner, permissions, accounts...)
}

// addAs adds accounts to the child directly, as the author
func (f *family) addAs(t *testing.T, author *accountdata.AccountKeys, permissions list.AclPermissions, accounts ...*accountdata.AccountKeys) error {
	additions := make([]list.AccountAdd, 0, len(accounts))
	for _, account := range accounts {
		additions = append(additions, list.AccountAdd{
			Identity:    account.SignKey.GetPublic(),
			Permissions: permissions,
			Metadata:    []byte("metadata"),
		})
	}
	rec, err := f.childBuilder(t, author).BuildAccountsAdd(list.AccountsAddPayload{Additions: additions})
	return f.addToChild(t, author, rec, err)
}

// remove removes accounts from the child with a read key change, as its owner
func (f *family) remove(t *testing.T, accounts ...*accountdata.AccountKeys) error {
	rec, err := f.childBuilder(t, f.childOwner).BuildAccountRemove(removePayload(t, accounts...))
	return f.addToChild(t, f.childOwner, rec, err)
}

// removeAsParent removes accounts from the child without a read key change, as an account that acts
// on the authority of the parent
func (f *family) removeAsParent(t *testing.T, author *accountdata.AccountKeys, accounts ...*accountdata.AccountKeys) error {
	rec, err := f.childBuilder(t, author).BuildAccountRemoveNoRotate(pubKeys(accounts...))
	return f.addToChild(t, author, rec, err)
}

// changeOwnerAsParent hands the child to one of its members, as an account that acts on the authority
// of the parent
func (f *family) changeOwnerAsParent(t *testing.T, author, newOwner *accountdata.AccountKeys) error {
	rec, err := f.childBuilder(t, author).BuildOwnershipChangeByParent(list.OwnershipChangePayload{
		NewOwner:            newOwner.SignKey.GetPublic(),
		OldOwnerPermissions: list.AclPermissionsAdmin,
	})
	return f.addToChild(t, author, rec, err)
}

func (f *family) childPermissions(t *testing.T, account *accountdata.AccountKeys) list.AclPermissions {
	return permissionsOf(f.aclOf(t, testChildId, f.childOwner), account)
}

func TestCoordinator_ChildAdmission(t *testing.T) {
	t.Run("a member of the parent is added, whatever its role there", func(t *testing.T) {
		f := newFamily(t)
		require.NoError(t, f.add(t, list.AclPermissionsWriter, f.parentReader, f.parentAdmin))
		require.NoError(t, f.add(t, list.AclPermissionsReader, f.parentOwner))
		assert.Equal(t, list.AclPermissionsWriter, f.childPermissions(t, f.parentReader))
		assert.Equal(t, list.AclPermissionsReader, f.childPermissions(t, f.parentOwner))
	})
	t.Run("an account that is not in the parent is not added", func(t *testing.T) {
		f := newFamily(t)
		require.ErrorIs(t, f.add(t, list.AclPermissionsReader, f.outsider), coordinatorproto.ErrNotParentMember)
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, f.outsider))
	})
	t.Run("one outsider refuses the whole record", func(t *testing.T) {
		f := newFamily(t)
		require.ErrorIs(t, f.add(t, list.AclPermissionsReader, f.parentReader, f.outsider), coordinatorproto.ErrNotParentMember)
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, f.parentReader))
	})
	t.Run("a batch that removes one account and adds another takes only a member of the parent", func(t *testing.T) {
		f := newFamily(t)
		require.NoError(t, f.add(t, list.AclPermissionsReader, f.parentReader))
		batch := func(added *accountdata.AccountKeys) error {
			res, err := f.childBuilder(t, f.childOwner).BuildBatchRequest(list.BatchRequestPayload{
				Removals: removePayload(t, f.parentReader),
				Additions: []list.AccountAdd{{
					Identity:    added.SignKey.GetPublic(),
					Permissions: list.AclPermissionsWriter,
					Metadata:    []byte("metadata"),
				}},
			})
			return f.addToChild(t, f.childOwner, res.Rec, err)
		}
		require.ErrorIs(t, batch(f.outsider), coordinatorproto.ErrNotParentMember)
		assert.Equal(t, list.AclPermissionsReader, f.childPermissions(t, f.parentReader))
		require.NoError(t, batch(f.parentWriter))
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, f.parentReader))
		assert.Equal(t, list.AclPermissionsWriter, f.childPermissions(t, f.parentWriter))
	})
	t.Run("the identity of a guest key is added though it is not in the parent", func(t *testing.T) {
		f := newFamily(t)
		guest := newAccount(t)
		require.NoError(t, f.add(t, list.AclPermissionsGuest, guest))
		assert.Equal(t, list.AclPermissionsGuest, f.childPermissions(t, guest))
	})
	t.Run("only the owner of the child lets the identity of a guest key in from outside the parent", func(t *testing.T) {
		f := newFamily(t)
		require.NoError(t, f.add(t, list.AclPermissionsAdmin, f.parentAdmin))
		guest := newAccount(t)

		// the acl of the child lets its Admin add any identity as a guest
		require.ErrorIs(t, f.addAs(t, f.parentAdmin, list.AclPermissionsGuest, guest), coordinatorproto.ErrNotParentMember)
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, guest))
		// a member of the parent is let in as a guest by an Admin of the child
		require.NoError(t, f.addAs(t, f.parentAdmin, list.AclPermissionsGuest, f.parentReader))
		assert.Equal(t, list.AclPermissionsGuest, f.childPermissions(t, f.parentReader))

		require.NoError(t, f.add(t, list.AclPermissionsGuest, guest))
		assert.Equal(t, list.AclPermissionsGuest, f.childPermissions(t, guest))
	})
	t.Run("the identity of a guest key of the parent does not become a member of the child", func(t *testing.T) {
		f := newFamily(t)
		guest := newAccount(t)
		f.putMembers(t, testParentId, f.parentOwner, list.AclPermissionsGuest, guest)
		require.ErrorIs(t, f.add(t, list.AclPermissionsReader, guest), coordinatorproto.ErrNotParentMember)
		require.NoError(t, f.add(t, list.AclPermissionsGuest, guest))
	})
	t.Run("a join request is taken and accepted only from a member of the parent", func(t *testing.T) {
		f := newFamily(t)
		invite, err := f.childBuilder(t, f.childOwner).BuildInvite()
		require.NoError(t, f.addToChild(t, f.childOwner, invite.InviteRec, err))

		// request posts the request of the joiner and returns its record
		request := func(joiner *accountdata.AccountKeys) (*consensusproto.RawRecordWithId, error) {
			rec, err := f.childBuilder(t, joiner).BuildRequestJoin(list.RequestJoinPayload{
				InviteKey: invite.InviteKey,
				Metadata:  []byte("metadata"),
			})
			return f.addRecord(t, testChildId, joiner, rec, err)
		}
		accept := func(requestId string) error {
			rec, err := f.childBuilder(t, f.childOwner).BuildRequestAccept(list.RequestAcceptPayload{
				RequestRecordId: requestId,
				Permissions:     list.AclPermissionsWriter,
			})
			return f.addToChild(t, f.childOwner, rec, err)
		}
		pending := func() int {
			requests, err := f.aclOf(t, testChildId, f.childOwner).AclState().JoinRecords(false)
			require.NoError(t, err)
			return len(requests)
		}

		// the request of an account that is not in the parent is refused when it is posted
		_, err = request(f.outsider)
		require.ErrorIs(t, err, coordinatorproto.ErrNotParentMember)
		assert.Zero(t, pending())

		// a member of the parent posts its request and is accepted
		posted, err := request(f.parentReader)
		require.NoError(t, err)
		assert.Equal(t, 1, pending())
		require.NoError(t, accept(posted.Id))
		assert.Equal(t, list.AclPermissionsWriter, f.childPermissions(t, f.parentReader))

		// an account that left the parent after it posted its request is not accepted
		posted, err = request(f.parentWriter)
		require.NoError(t, err)
		f.putRemoval(t, testParentId, f.parentOwner, f.parentWriter)
		require.ErrorIs(t, accept(posted.Id), coordinatorproto.ErrNotParentMember)
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, f.parentWriter))

		// its pending request does not hold up the records of others
		require.NoError(t, f.add(t, list.AclPermissionsReader, f.parentAdmin))
	})
	t.Run("an invite is joined only by a member of the parent", func(t *testing.T) {
		f := newFamily(t)
		invite, err := f.childBuilder(t, f.childOwner).BuildInviteAnyone(list.AclPermissionsWriter)
		require.NoError(t, f.addToChild(t, f.childOwner, invite.InviteRec, err))

		join := func(joiner *accountdata.AccountKeys) error {
			rec, err := f.childBuilder(t, joiner).BuildInviteJoinWithoutApprove(list.InviteJoinPayload{
				InviteKey: invite.InviteKey,
				Metadata:  []byte("metadata"),
			})
			return f.addToChild(t, joiner, rec, err)
		}
		require.ErrorIs(t, join(f.outsider), coordinatorproto.ErrNotParentMember)
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, f.outsider))
		require.NoError(t, join(f.parentReader))
		assert.Equal(t, list.AclPermissionsWriter, f.childPermissions(t, f.parentReader))
	})
	t.Run("a member of the child stays one after it left the parent", func(t *testing.T) {
		f := newFamily(t)
		require.NoError(t, f.add(t, list.AclPermissionsReader, f.parentReader))
		f.putRemoval(t, testParentId, f.parentOwner, f.parentReader)

		rec, err := f.childBuilder(t, f.childOwner).BuildPermissionChange(list.PermissionChangePayload{
			Identity:    f.parentReader.SignKey.GetPublic(),
			Permissions: list.AclPermissionsWriter,
		})
		require.NoError(t, f.addToChild(t, f.childOwner, rec, err))
		assert.Equal(t, list.AclPermissionsWriter, f.childPermissions(t, f.parentReader))
		// it is not added again
		require.NoError(t, f.remove(t, f.parentReader))
		require.ErrorIs(t, f.add(t, list.AclPermissionsReader, f.parentReader), coordinatorproto.ErrNotParentMember)
	})
	t.Run("a top-level space takes any account", func(t *testing.T) {
		f := newFamily(t)
		f.expectSpace(spacestatus.StatusEntry{
			SpaceId:     testParentId,
			Identity:    f.parentOwner.SignKey.GetPublic().Account(),
			Type:        spacestatus.SpaceTypeRegular,
			HeaderType:  testAnySpaceType,
			IsShareable: true,
		})
		rec, err := f.aclOf(t, testParentId, f.parentOwner).RecordBuilder().BuildAccountsAdd(list.AccountsAddPayload{Additions: []list.AccountAdd{{
			Identity:    f.outsider.SignKey.GetPublic(),
			Permissions: list.AclPermissionsReader,
			Metadata:    []byte("metadata"),
		}}})
		_, err = f.addRecord(t, testParentId, f.parentOwner, rec, err)
		require.NoError(t, err)
	})
	t.Run("without the acl of the parent nobody becomes a member, and the rest goes on", func(t *testing.T) {
		fx := newAclFixture(t)
		defer fx.finish(t)
		owner, member, added := newAccount(t), newAccount(t), newAccount(t)
		fx.newAcl(t, testChildId, owner)
		fx.putMembers(t, testChildId, owner, list.AclPermissionsReader, member)
		fx.expectSpace(spacestatus.StatusEntry{
			SpaceId:       testChildId,
			Identity:      owner.SignKey.GetPublic().Account(),
			IsShareable:   true,
			ParentSpaceId: "lost.parent.id",
		})
		fx.aclEventLog.EXPECT().AddLog(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		add := func(permissions list.AclPermissions) error {
			rec, err := fx.aclOf(t, testChildId, owner).RecordBuilder().BuildAccountsAdd(list.AccountsAddPayload{Additions: []list.AccountAdd{{
				Identity:    added.SignKey.GetPublic(),
				Permissions: permissions,
				Metadata:    []byte("metadata"),
			}}})
			_, err = fx.addRecord(t, testChildId, owner, rec, err)
			return err
		}

		require.ErrorIs(t, add(list.AclPermissionsReader), coordinatorproto.ErrParentSpaceNotAvailable)
		require.NoError(t, add(list.AclPermissionsGuest))
		// nobody has the authority of a parent whose acl cannot be read
		keyless, err := fx.aclOf(t, testChildId, added).RecordBuilder().BuildAccountRemoveNoRotate(pubKeys(member))
		_, err = fx.addRecord(t, testChildId, added, keyless, err)
		require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceNotAvailable)
		rec, err := fx.aclOf(t, testChildId, owner).RecordBuilder().BuildAccountRemove(removePayload(t, member))
		_, err = fx.addRecord(t, testChildId, owner, rec, err)
		require.NoError(t, err)
		assert.Equal(t, list.AclPermissionsNone, permissionsOf(fx.aclOf(t, testChildId, owner), member))
	})
}

func TestCoordinator_ChildRemovalByParent(t *testing.T) {
	// the child has members of every role of the parent
	newFamilyWithMembers := func(t *testing.T) *family {
		f := newFamily(t)
		require.NoError(t, f.add(t, list.AclPermissionsWriter, f.parentWriter, f.parentReader, f.parentAdmin, f.otherAdmin, f.parentOwner))
		return f
	}

	t.Run("the Owner of the parent removes a member without the read key", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		require.NoError(t, f.removeAsParent(t, f.parentOwner, f.parentWriter))

		child := f.aclOf(t, testChildId, f.childOwner)
		assert.Equal(t, list.AclPermissionsNone, permissionsOf(child, f.parentWriter))
		assert.True(t, child.AclState().RotationPending())

		// the owner of the child, who holds the read key, ends the pending rotation
		change := removePayload(t).Change
		rec, err := child.RecordBuilder().BuildReadKeyChange(change)
		require.NoError(t, f.addToChild(t, f.childOwner, rec, err))
		assert.False(t, f.aclOf(t, testChildId, f.childOwner).AclState().RotationPending())
	})
	t.Run("an Admin of the parent removes a member without the read key", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		require.NoError(t, f.removeAsParent(t, f.parentAdmin, f.parentWriter, f.parentReader))
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, f.parentWriter))
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, f.parentReader))
	})
	t.Run("an account below Admin in the parent removes nobody", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		for name, author := range map[string]*accountdata.AccountKeys{
			"a Writer of the parent": f.parentWriter,
			"a Reader of the parent": f.parentReader,
			"an outsider":            f.outsider,
			// its permissions in the child give it no such removal
			"the owner of the child": f.childOwner,
		} {
			removed := f.parentReader
			if author == f.parentReader {
				removed = f.parentWriter
			}
			require.ErrorIs(t, f.removeAsParent(t, author, removed), coordinatorproto.ErrParentPermissionDenied, name)
			assert.Equal(t, list.AclPermissionsWriter, f.childPermissions(t, removed), name)
		}
	})
	t.Run("nobody removes the owner of the child this way", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		// the builder refuses the record; one sent past that check fares no better
		rec, err := f.childBuilder(t, f.parentOwner).BuildAccountRemoveNoRotate(pubKeys(f.childOwner))
		require.ErrorIs(t, err, list.ErrInsufficientPermissions)
		err = f.addToChild(t, f.parentOwner, rec, nil)
		require.ErrorIs(t, err, list.ErrInsufficientPermissions)
		require.NotErrorIs(t, err, coordinatorproto.ErrParentPermissionDenied)
		assert.Equal(t, list.AclPermissionsOwner, f.childPermissions(t, f.childOwner))
	})
	t.Run("a top-level space has no such removal", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		f.expectSpace(spacestatus.StatusEntry{
			SpaceId:     testParentId,
			Identity:    f.parentOwner.SignKey.GetPublic().Account(),
			IsShareable: true,
		})
		rec, err := f.aclOf(t, testParentId, f.parentOwner).RecordBuilder().BuildAccountRemoveNoRotate(pubKeys(f.parentReader))
		_, err = f.addRecord(t, testParentId, f.parentOwner, rec, err)
		require.ErrorIs(t, err, list.ErrInsufficientPermissions)
		require.NotErrorIs(t, err, coordinatorproto.ErrParentPermissionDenied)
	})

	t.Run("an Admin of the parent does not remove the Owner or an Admin of the parent", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		require.ErrorIs(t, f.removeAsParent(t, f.parentAdmin, f.otherAdmin), coordinatorproto.ErrParentPermissionDenied)
		require.ErrorIs(t, f.removeAsParent(t, f.parentAdmin, f.parentOwner), coordinatorproto.ErrParentPermissionDenied)
		// one such account refuses the whole record
		require.ErrorIs(t, f.removeAsParent(t, f.parentAdmin, f.parentWriter, f.otherAdmin), coordinatorproto.ErrParentPermissionDenied)
		for _, kept := range []*accountdata.AccountKeys{f.otherAdmin, f.parentOwner, f.parentWriter} {
			assert.Equal(t, list.AclPermissionsWriter, f.childPermissions(t, kept))
		}
	})
	t.Run("an Admin of the parent removes an account that left the parent", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		f.putRemoval(t, testParentId, f.parentOwner, f.otherAdmin)
		require.NoError(t, f.removeAsParent(t, f.parentAdmin, f.otherAdmin))
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, f.otherAdmin))
	})
	t.Run("the Owner of the parent removes an Admin of the parent", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		require.NoError(t, f.removeAsParent(t, f.parentOwner, f.parentAdmin, f.otherAdmin))
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, f.parentAdmin))
	})
	t.Run("the owner of the child removes with the read key whoever the child's acl lets it", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		require.NoError(t, f.remove(t, f.parentOwner, f.parentAdmin))
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, f.parentOwner))
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, f.parentAdmin))
	})
}

func TestCoordinator_ChildOwnerChange(t *testing.T) {
	newFamilyWithMembers := func(t *testing.T) *family {
		f := newFamily(t)
		require.NoError(t, f.add(t, list.AclPermissionsWriter, f.parentWriter, f.parentAdmin))
		return f
	}
	// expectOwner expects the document of the child to take the new owner
	expectOwner := func(f *family, newOwner *accountdata.AccountKeys) {
		f.spaceStatus.EXPECT().ChangeOwner(gomock.Any(), testChildId, newOwner.SignKey.GetPublic().Account()).Return(nil)
		f.deletionLog.EXPECT().AddOwnershipChange(gomock.Any(), testChildId, f.childOwner.SignKey.GetPublic().Account(), gomock.Any()).Return("logId", nil)
	}

	t.Run("the Owner of the parent hands the child to one of its members", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		expectOwner(f, f.parentWriter)
		require.NoError(t, f.changeOwnerAsParent(t, f.parentOwner, f.parentWriter))
		assert.Equal(t, list.AclPermissionsOwner, f.childPermissions(t, f.parentWriter))
		assert.Equal(t, list.AclPermissionsAdmin, f.childPermissions(t, f.childOwner))
		// the Owner of the parent got nothing in the child
		assert.Equal(t, list.AclPermissionsNone, f.childPermissions(t, f.parentOwner))
	})
	t.Run("the owner of the child hands it over itself", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		expectOwner(f, f.parentWriter)
		rec, err := f.childBuilder(t, f.childOwner).BuildOwnershipChange(list.OwnershipChangePayload{
			NewOwner:            f.parentWriter.SignKey.GetPublic(),
			OldOwnerPermissions: list.AclPermissionsWriter,
		})
		require.NoError(t, f.addToChild(t, f.childOwner, rec, err))
		assert.Equal(t, list.AclPermissionsOwner, f.childPermissions(t, f.parentWriter))
	})
	t.Run("an account that is not the Owner of the parent changes no owner", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		for name, author := range map[string]*accountdata.AccountKeys{
			"an Admin of the parent":                      f.parentAdmin,
			"an Admin of the parent outside of the child": f.otherAdmin,
			"a Writer of the parent":                      f.parentWriter,
			"an outsider":                                 f.outsider,
		} {
			require.ErrorIs(t, f.changeOwnerAsParent(t, author, f.parentWriter), coordinatorproto.ErrParentPermissionDenied, name)
		}
		assert.Equal(t, list.AclPermissionsOwner, f.childPermissions(t, f.childOwner))
	})
	t.Run("the new owner is a member of the parent, whoever hands the child over", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		// the member of the child left the parent
		f.putRemoval(t, testParentId, f.parentOwner, f.parentWriter)

		require.ErrorIs(t, f.changeOwnerAsParent(t, f.parentOwner, f.parentWriter), coordinatorproto.ErrNotParentMember)
		rec, err := f.childBuilder(t, f.childOwner).BuildOwnershipChange(list.OwnershipChangePayload{
			NewOwner:            f.parentWriter.SignKey.GetPublic(),
			OldOwnerPermissions: list.AclPermissionsAdmin,
		})
		require.ErrorIs(t, f.addToChild(t, f.childOwner, rec, err), coordinatorproto.ErrNotParentMember)
		assert.Equal(t, list.AclPermissionsOwner, f.childPermissions(t, f.childOwner))
		assert.Equal(t, list.AclPermissionsWriter, f.childPermissions(t, f.parentWriter))

		// a member of the child that is in the parent takes it
		expectOwner(f, f.parentAdmin)
		require.NoError(t, f.changeOwnerAsParent(t, f.parentOwner, f.parentAdmin))
	})
	t.Run("the new owner is a member of the child", func(t *testing.T) {
		f := newFamilyWithMembers(t)
		// the builder refuses the record; one sent past that check fares no better
		rec, err := f.childBuilder(t, f.parentOwner).BuildOwnershipChangeByParent(list.OwnershipChangePayload{
			NewOwner:            f.parentReader.SignKey.GetPublic(),
			OldOwnerPermissions: list.AclPermissionsAdmin,
		})
		require.Error(t, err)
		err = f.addToChild(t, f.parentOwner, rec, nil)
		require.Error(t, err)
		require.NotErrorIs(t, err, coordinatorproto.ErrParentPermissionDenied)
		assert.Equal(t, list.AclPermissionsOwner, f.childPermissions(t, f.childOwner))
	})
}

// The refusals that come from the parent reach the client as registered coordinator errors.
func TestRpcHandler_AclAddRecordChild(t *testing.T) {
	f := newFamily(t)
	require.NoError(t, f.add(t, list.AclPermissionsWriter, f.parentWriter, f.parentReader, f.otherAdmin))

	// send sends the record of the child through the rpc handler and returns the code of its error
	send := func(author *accountdata.AccountKeys, rec *consensusproto.RawRecord, buildErr error) uint64 {
		require.NoError(t, buildErr)
		payload, err := rec.MarshalVT()
		require.NoError(t, err)
		resp, err := f.drpcHandler.AclAddRecord(accountCtx(t, author.SignKey.GetPublic()), &coordinatorproto.AclAddRecordRequest{
			SpaceId: testChildId,
			Payload: payload,
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		return rpcerr.Code(err)
	}
	code := func(c coordinatorproto.ErrorCodes) uint64 {
		return uint64(coordinatorproto.ErrorCodes_ErrorOffset) + uint64(c)
	}
	removal := func(author, removed *accountdata.AccountKeys) uint64 {
		rec, err := f.childBuilder(t, author).BuildAccountRemoveNoRotate(pubKeys(removed))
		return send(author, rec, err)
	}
	ownerChange := func(author *accountdata.AccountKeys) uint64 {
		rec, err := f.childBuilder(t, author).BuildOwnershipChangeByParent(list.OwnershipChangePayload{
			NewOwner:            f.parentWriter.SignKey.GetPublic(),
			OldOwnerPermissions: list.AclPermissionsAdmin,
		})
		return send(author, rec, err)
	}
	denied := code(coordinatorproto.ErrorCodes_ParentPermissionDenied)

	assert.Equal(t, denied, removal(f.parentWriter, f.parentReader), "a removal by a Writer of the parent")
	assert.Equal(t, denied, removal(f.outsider, f.parentReader), "a removal by an account that is not in the parent")
	assert.Equal(t, denied, removal(f.parentAdmin, f.otherAdmin), "a removal of an Admin of the parent by another")
	assert.Equal(t, denied, ownerChange(f.parentAdmin), "an owner change by an Admin of the parent")
	assert.Equal(t, denied, ownerChange(f.outsider), "an owner change by an account that is not in the parent")

	rec, err := f.childBuilder(t, f.childOwner).BuildAccountsAdd(list.AccountsAddPayload{Additions: []list.AccountAdd{{
		Identity:    f.outsider.SignKey.GetPublic(),
		Permissions: list.AclPermissionsReader,
		Metadata:    []byte("metadata"),
	}}})
	assert.Equal(t, code(coordinatorproto.ErrorCodes_NotParentMember), send(f.childOwner, rec, err), "an account that is not in the parent added to the child")
}
