package coordinator

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/aclrecordproto"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
)

// A childSpacesLimit that a limits document stores as 0, or does not store, is the configured default,
// and an update that carries 0 keeps the stored value.
func TestChildScenario_ChildSpacesLimitDocument(t *testing.T) {
	n := newNetwork(t, accountlimit.SpaceLimits{SharedSpacesLimit: 10, ChildSpacesLimit: 2})
	o := n.newOrg()
	assert.Equal(t, uint32(2), n.accountLimits(o.owner).ChildSpacesLimit)

	// the payment node sets limits of the owner, and leaves out the child spaces
	n.setLimits(o.owner, &coordinatorproto.AccountLimitsSetRequest{SharedSpacesLimit: 5, SpaceMembersRead: 100, SpaceMembersWrite: 100})
	assert.Equal(t, uint32(2), n.accountLimits(o.owner).ChildSpacesLimit)
	n.mustCreateChild(o.writer, o.parent.id)
	n.mustCreateChild(o.writer, o.parent.id)
	_, err := n.createChild(o.writer, o.parent.id)
	require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)

	n.setLimits(o.owner, &coordinatorproto.AccountLimitsSetRequest{SharedSpacesLimit: 5, SpaceMembersRead: 100, SpaceMembersWrite: 100, ChildSpacesLimit: 3})
	assert.Equal(t, uint32(3), n.accountLimits(o.owner).ChildSpacesLimit)
	n.mustCreateChild(o.writer, o.parent.id)

	n.setLimits(o.owner, &coordinatorproto.AccountLimitsSetRequest{SharedSpacesLimit: 6, SpaceMembersRead: 100, SpaceMembersWrite: 100})
	assert.Equal(t, uint32(6), n.accountLimits(o.owner).SharedSpacesLimit)
	assert.Equal(t, uint32(3), n.accountLimits(o.owner).ChildSpacesLimit)

	// a limits document written before the child spaces were counted
	other := n.newOrg()
	_, err = n.db.Db().Collection("accountLimit").InsertOne(context.Background(), bson.M{
		"_id":               accountOf(other.owner),
		"spaceMembersRead":  100,
		"spaceMembersWrite": 100,
		"sharedSpacesLimit": 5,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(2), n.accountLimits(other.owner).ChildSpacesLimit)
	n.mustCreateChild(other.writer, other.parent.id)
}

// finalisingStatus is the space status of the coordinator, with the deleter making the deletion of one
// space final right after the coordinator read its status once
type finalisingStatus struct {
	spacestatus.SpaceStatus
	spaceId  string
	armed    atomic.Bool
	finalise func()
}

func (s *finalisingStatus) Status(ctx context.Context, spaceId string) (spacestatus.StatusEntry, error) {
	entry, err := s.SpaceStatus.Status(ctx, spaceId)
	if spaceId == s.spaceId && s.armed.CompareAndSwap(true, false) {
		s.finalise()
	}
	return entry, err
}

// A forced sign that takes back a finally deleted child is checked as a first sign, also when the
// deletion becomes final while the sign is under way.
func TestChildScenario_RevivalRace(t *testing.T) {
	n := newNetwork(t, roomyLimits)
	o := n.newOrg()
	child := n.mustCreateChild(o.writer, o.parent.id)
	require.NoError(t, n.deleteSpace(o.writer, child.id))
	require.NoError(t, n.setPermissions(o.parent.id, o.owner, o.writer, list.AclPermissionsReader))

	status := &finalisingStatus{SpaceStatus: n.c.spaceStatus, spaceId: child.id, finalise: func() { n.endDeletionPeriod(child.id) }}
	status.armed.Store(true)
	n.c.spaceStatus = status

	require.ErrorIs(t, n.sign(o.writer, child, true), coordinatorproto.ErrParentPermissionDenied)
	assert.False(t, status.armed.Load(), "the deletion did not become final during the sign")
	assert.Equal(t, spacestatus.SpaceStatusDeleted, n.status(child.id).Status)
}

// An account that owns a space with a child of another account keeps its account: deleting it would
// delete the parent and leave the child without one.
func TestChildScenario_AccountDeletion(t *testing.T) {
	t.Run("the owner of a parent with a child of another account does not delete its account", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		require.NoError(t, n.sign(o.owner, newTechSpace(t, o.owner), false))
		child := n.mustCreateChild(o.writer, o.parent.id)

		require.ErrorIs(t, n.deleteAccount(o.owner), coordinatorproto.ErrSpaceHasChildren)
		assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(o.parent.id).Status)

		// a child whose deletion is pending still holds the account
		require.NoError(t, n.deleteSpace(o.writer, child.id))
		require.ErrorIs(t, n.deleteAccount(o.owner), coordinatorproto.ErrSpaceHasChildren)
		n.endDeletionPeriod(child.id)
		require.NoError(t, n.deleteAccount(o.owner))
		assert.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(o.parent.id).Status)
	})
	t.Run("children of the owner itself go with its account", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		require.NoError(t, n.sign(o.owner, newTechSpace(t, o.owner), false))
		child := n.mustCreateChild(o.owner, o.parent.id)

		require.NoError(t, n.deleteAccount(o.owner))
		assert.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(o.parent.id).Status)
		assert.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(child.id).Status)
	})
}

// The owner of a child does not take back a deletion that the Owner of the parent made.
func TestChildScenario_DeletionByParentOwner(t *testing.T) {
	// deletedByParent is a child that the Owner of the parent deleted; its owner has a tech space
	deletedByParent := func(t *testing.T) (*network, org, space) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		require.NoError(t, n.sign(o.writer, newTechSpace(t, o.writer), false))
		child := n.mustCreateChild(o.writer, o.parent.id)
		require.NoError(t, n.deleteSpace(o.owner, child.id))
		require.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(child.id).Status)
		return n, o, child
	}

	t.Run("not by a forced sign", func(t *testing.T) {
		n, o, child := deletedByParent(t)
		require.Error(t, n.sign(o.writer, child, true))
		assert.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(child.id).Status)
	})
	t.Run("not by the deprecated status change", func(t *testing.T) {
		n, o, child := deletedByParent(t)
		require.Error(t, n.revertDeletion(o.writer, child.id))
		assert.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(child.id).Status)
	})
	t.Run("not by reverting the deletion of its account", func(t *testing.T) {
		n, o, child := deletedByParent(t)
		own := n.newSharedSpace(o.writer)
		require.NoError(t, n.deleteAccount(o.writer))
		require.NoError(t, n.revertAccountDeletion(o.writer))
		assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(own.id).Status)
		assert.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(child.id).Status)
	})
}

// The Owner of the parent deletes a child that is in deletion by its owner. The deletion is then the one
// of the Owner of the parent, with its period, and the owner of the child does not take it back.
func TestChildScenario_DeletionByParentOwnerOfChildInDeletion(t *testing.T) {
	for name, byAccount := range map[string]bool{"the owner deleted the child": false, "the owner deleted its account": true} {
		t.Run(name, func(t *testing.T) {
			n := newNetwork(t, roomyLimits)
			o := n.newOrg()
			require.NoError(t, n.sign(o.writer, newTechSpace(t, o.writer), false))
			child := n.mustCreateChild(o.writer, o.parent.id)
			if byAccount {
				require.NoError(t, n.deleteAccount(o.writer))
			} else {
				require.NoError(t, n.deleteSpace(o.writer, child.id))
			}
			byOwner := n.status(child.id)
			require.Equal(t, spacestatus.SpaceStatusDeletionPending, byOwner.Status)

			payload := []byte("payload of the Owner of the parent")
			resp, err := n.c.drpcHandler.SpaceDelete(n.ctx(o.owner), &coordinatorproto.SpaceDeleteRequest{
				SpaceId:           child.id,
				DeletionPayload:   payload,
				DeletionPayloadId: "payload.id",
				DeletionDuration:  120,
			})
			require.NoError(t, err)
			entry := n.status(child.id)
			assert.Equal(t, spacestatus.SpaceStatusDeletionPending, entry.Status)
			assert.True(t, entry.DeletedByParentOwner)
			assert.Equal(t, accountOf(o.writer), entry.Identity)
			assert.Equal(t, payload, entry.DeletionPayload)
			assert.Equal(t, int(coordinatorproto.DeletionPayloadType_Confirm), entry.DeletionPayloadType)
			assert.Equal(t, resp.ToBeDeletedTimestamp, entry.ToBeDeletedTimestamp)
			assert.NotEqual(t, byOwner.ToBeDeletedTimestamp, entry.ToBeDeletedTimestamp)

			// the owner of the child takes its account back, and the child by none of its ways
			if !byAccount {
				require.NoError(t, n.deleteAccount(o.writer))
			}
			require.NoError(t, n.revertAccountDeletion(o.writer))
			require.ErrorIs(t, n.sign(o.writer, child, true), coordinatorproto.ErrSpaceIsDeleted)
			require.ErrorIs(t, n.revertDeletion(o.writer, child.id), coordinatorproto.ErrParentPermissionDenied)
			assert.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(child.id).Status)

			// a deletion that is its own already answers the Owner of the parent as a repeated one
			require.ErrorIs(t, n.deleteSpace(o.owner, child.id), coordinatorproto.ErrSpaceDeletionPending)
			assert.Equal(t, entry.ToBeDeletedTimestamp, n.status(child.id).ToBeDeletedTimestamp)
		})
	}
}

// guestEntry is one way into the guest role of a space: a record that gives the account the role
type guestEntry struct {
	name    string
	account *accountdata.AccountKeys
	// before is the role of the account when the record is sent
	before list.AclPermissions
	// with is an account that the same record adds as a Reader
	with *accountdata.AccountKeys
	// send builds the record as the author and sends it to the coordinator
	send func() error
}

// guestEntries prepares every way into the guest role of the space, each with the author as the one
// who writes the record. newMember gives an account that the space takes as a member.
func (n *network) guestEntries(spaceId string, author *accountdata.AccountKeys, newMember func() *accountdata.AccountKeys) []guestEntry {
	key, added, addedWith, joiner, reader := newAccount(n.t), newMember(), newMember(), newMember(), newMember()

	invite, err := n.builder(spaceId, author).BuildInvite()
	require.NoError(n.t, n.sendErr(spaceId, author, invite.InviteRec, err))
	request, err := n.builder(spaceId, joiner).BuildRequestJoin(list.RequestJoinPayload{InviteKey: invite.InviteKey, Metadata: []byte("metadata")})
	posted, err := n.send(spaceId, joiner, request, err)
	require.NoError(n.t, err)
	require.NoError(n.t, n.add(spaceId, author, list.AclPermissionsReader, reader))

	addition := func(account *accountdata.AccountKeys, permissions list.AclPermissions) list.AccountAdd {
		return list.AccountAdd{Identity: account.SignKey.GetPublic(), Permissions: permissions, Metadata: []byte("metadata")}
	}
	return []guestEntry{
		{name: "a guest key: the identity of a new key is added as a guest", account: key, send: func() error {
			return n.add(spaceId, author, list.AclPermissionsGuest, key)
		}},
		{name: "a direct add: an account is added as a guest, another as a Reader with it", account: added, with: addedWith, send: func() error {
			rec, err := n.builder(spaceId, author).BuildAccountsAdd(list.AccountsAddPayload{Additions: []list.AccountAdd{
				addition(addedWith, list.AclPermissionsReader),
				addition(added, list.AclPermissionsGuest),
			}})
			return n.sendErr(spaceId, author, rec, err)
		}},
		{name: "an accepted request: the request to join is accepted as a guest", account: joiner, send: func() error {
			rec, err := n.builder(spaceId, author).BuildRequestAccept(list.RequestAcceptPayload{RequestRecordId: posted.RecordId, Permissions: list.AclPermissionsGuest})
			return n.sendErr(spaceId, author, rec, err)
		}},
		{name: "a permission change: a Reader is made a guest", account: reader, before: list.AclPermissionsReader, send: func() error {
			return n.setPermissions(spaceId, author, reader, list.AclPermissionsGuest)
		}},
	}
}

// An organization and its spaces have no guest role: no record gives an account the role, whoever
// writes it. A regular any.space has the role.
func TestChildScenario_GuestRole(t *testing.T) {
	// refused sends every way into the guest role as the author and expects each to change nothing
	refused := func(t *testing.T, n *network, spaceId string, author *accountdata.AccountKeys, newMember func() *accountdata.AccountKeys) {
		for _, entry := range n.guestEntries(spaceId, author, newMember) {
			records := len(n.consensus.log(spaceId))
			require.ErrorIs(t, entry.send(), coordinatorproto.ErrGuestRoleNotAllowed, entry.name)
			assert.Len(t, n.consensus.log(spaceId), records, entry.name)
			assert.Equal(t, entry.before, n.permissions(spaceId, entry.account), entry.name)
			if entry.with != nil {
				assert.Equal(t, list.AclPermissionsNone, n.permissions(spaceId, entry.with), entry.name)
			}
		}
	}

	t.Run("nobody enters an organization as a guest", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		anyAccount := func() *accountdata.AccountKeys { return newAccount(t) }
		refused(t, n, o.parent.id, o.owner, anyAccount)
		refused(t, n, o.parent.id, o.admin, anyAccount)
	})
	t.Run("nobody enters a space of an organization as a guest", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		child := n.mustCreateChild(o.writer, o.parent.id)
		require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsAdmin, o.admin))
		// the identity of the guest key is not in the parent, the other accounts are its members
		parentMember := func() *accountdata.AccountKeys { return n.newMembers(o, list.AclPermissionsReader, 1)[0] }
		refused(t, n, child.id, o.writer, parentMember)
		refused(t, n, child.id, o.admin, parentMember)
	})
	t.Run("a regular any.space has the guest role", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		owner := newAccount(t)
		s := n.newSharedSpace(owner)
		for _, entry := range n.guestEntries(s.id, owner, func() *accountdata.AccountKeys { return newAccount(t) }) {
			require.NoError(t, entry.send(), entry.name)
			assert.Equal(t, list.AclPermissionsGuest, n.permissions(s.id, entry.account), entry.name)
			if entry.with != nil {
				assert.Equal(t, list.AclPermissionsReader, n.permissions(s.id, entry.with), entry.name)
			}
		}
	})
}

// The new owner of a child is a member of the parent when the ownership changes.
func TestChildScenario_NewOwnerInParent(t *testing.T) {
	n := newNetwork(t, roomyLimits)
	o := n.newOrg()
	leaver := n.newMembers(o, list.AclPermissionsWriter, 1)[0]
	child := n.mustCreateChild(o.writer, o.parent.id)
	require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsWriter, leaver))
	require.NoError(t, n.remove(o.parent.id, o.owner, leaver))

	require.ErrorIs(t, n.changeOwnerAsParent(child.id, o.owner, leaver, list.AclPermissionsAdmin), coordinatorproto.ErrNotParentMember)
	require.ErrorIs(t, n.transfer(child.id, o.writer, leaver, list.AclPermissionsAdmin), coordinatorproto.ErrNotParentMember)
	assert.Equal(t, list.AclPermissionsOwner, n.permissions(child.id, o.writer))
	assert.Equal(t, accountOf(o.writer), n.status(child.id).Identity)
}

// A derived space carries no timestamp. With the type of a regular space of the SDK it is no parent,
// and it is not signed with the type of an organization.
func TestChildScenario_DerivedParent(t *testing.T) {
	n := newNetwork(t, roomyLimits)
	owner := newAccount(t)
	derive := func(spaceType string) space {
		masterKey, _, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		payload, err := spacepayloads.StoragePayloadForSpaceDeriveV1(spacepayloads.SpaceDerivePayload{
			SigningKey:       owner.SignKey,
			MasterKey:        masterKey,
			SpaceType:        spaceType,
			FileProtoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
		})
		require.NoError(t, err)
		return space{id: payload.SpaceHeaderWithId.Id, header: payload.SpaceHeaderWithId.RawHeader, root: payload.AclWithId}
	}
	derivedOrganization := derive(testOrganizationType)
	require.ErrorContains(t, n.sign(owner, derivedOrganization, false), "requires a timestamp")
	assert.False(t, n.registered(derivedOrganization.id))

	derived := derive(testAnySpaceType)
	require.NoError(t, n.sign(owner, derived, false))
	entry := n.status(derived.id)
	assert.Equal(t, spacestatus.SpaceTypePersonal, entry.Type)
	assert.Equal(t, testAnySpaceType, entry.HeaderType)

	child, err := n.createChild(owner, derived.id)
	require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceInvalid)
	assert.False(t, n.registered(child.id))
}

// The limit of an owner holds across its parents: of two children that ask for its last place at the
// same instant, one under each parent, one is signed.
func TestChildScenario_LimitAcrossParents(t *testing.T) {
	n := newNetwork(t, accountlimit.SpaceLimits{SharedSpacesLimit: 10, ChildSpacesLimit: 3})
	for round := 0; round < 5; round++ {
		o := n.newOrg()
		second := n.newOrganization(o.owner)
		secondWriter := newAccount(t)
		require.NoError(t, n.add(second.id, o.owner, list.AclPermissionsWriter, secondWriter))
		n.mustCreateChild(o.writer, o.parent.id)
		n.mustCreateChild(o.writer, o.parent.id)

		creators := []*accountdata.AccountKeys{o.writer, secondWriter}
		children := []space{
			newSpace(t, o.writer, testOrganizationSpaceType, o.parent.id),
			newSpace(t, secondWriter, testOrganizationSpaceType, second.id),
		}
		errs := make([]error, len(children))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range children {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				errs[i] = n.signOnly(creators[i], children[i], false)
			}()
		}
		close(start)
		wg.Wait()

		var signed int
		for i, err := range errs {
			if err == nil {
				signed++
				continue
			}
			require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)
			assert.False(t, n.registered(children[i].id))
		}
		assert.Equal(t, 1, signed, "round %d", round)
		listed, err := n.children(o.owner, o.parent.id)
		require.NoError(t, err)
		listedSecond, err := n.children(o.owner, second.id)
		require.NoError(t, err)
		assert.Equal(t, 3, len(listed)+len(listedSecond), "round %d", round)
	}
}

// The children of a space are listed once its acl is on the network: until the push that follows the
// sign, the coordinator cannot tell the role of the caller.
func TestChildScenario_ChildrenBeforePush(t *testing.T) {
	n := newNetwork(t, roomyLimits)
	owner := newAccount(t)
	parent := newSpace(t, owner, testOrganizationType, "")
	require.NoError(t, n.signOnly(owner, parent, false))

	_, err := n.children(owner, parent.id)
	require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceNotAvailable)

	n.push(parent)
	listed, err := n.children(owner, parent.id)
	require.NoError(t, err)
	assert.Empty(t, listed)
}

// withRoot is a space whose creator signed a header that binds the given acl root
func withRoot(t *testing.T, creator *accountdata.AccountKeys, spaceType, parentSpaceId string, root *consensusproto.RawRecordWithId) space {
	spaceId, header := newRawHeader(t, creator, func(h *spacesyncproto.SpaceHeader) {
		h.SpaceType, h.ParentSpaceId, h.AclPayload = spaceType, parentSpaceId, root.Payload
	})
	return space{id: spaceId, header: header, root: root}
}

// The creator of an organization and of a space of an organization is its first owner: the v1 header
// binds an acl root that the account that signed the header wrote, and that is not a one-to-one root.
func TestChildScenario_AclRoot(t *testing.T) {
	const refusal = "requires the v1 header with the acl root of its creator"
	n := newNetwork(t, roomyLimits)
	o := n.newOrg()
	outsider := newAccount(t)
	key := func(account *accountdata.AccountKeys) []byte {
		raw, err := account.SignKey.GetPublic().Marshall()
		require.NoError(t, err)
		return raw
	}
	// the creator builds the one-to-one space with the outsider alone
	oneToOne, err := spacepayloads.StoragePayloadForOneToOneSpaceWithType(o.writer.SignKey, outsider.SignKey.GetPublic(), spacepayloads.SpaceTypeOneToOneAny)
	require.NoError(t, err)
	// a one-to-one root names the accounts of the space itself, whoever signs it
	named, err := list.NewAclRecordBuilder("", crypto.NewKeyStorage(), nil, recordverifier.NewValidateFull()).BuildOneToOneRoot(
		list.RootContent{PrivKey: o.writer.SignKey, MasterKey: o.writer.SignKey},
		&aclrecordproto.AclOneToOneInfo{Owner: key(outsider), Writers: [][]byte{key(o.writer), key(outsider)}},
	)
	require.NoError(t, err)
	roots := map[string]*consensusproto.RawRecordWithId{
		"the root of an outsider":                  newAclRoot(t, outsider.SignKey),
		"the root of a one-to-one space":           oneToOne.AclWithId,
		"a one-to-one root that the creator signs": named,
	}
	refused := func(t *testing.T, s space, name string) {
		err := n.sign(o.writer, s, false)
		require.ErrorContains(t, err, refusal, name)
		// the error is not one of the coordinator's
		assert.Zero(t, rpcerr.Code(err), name)
		assert.False(t, n.registered(s.id), name)
	}

	t.Run("a space of an organization with a root that is not its creator's own", func(t *testing.T) {
		for name, root := range roots {
			refused(t, withRoot(t, o.writer, testOrganizationSpaceType, o.parent.id, root), name)
		}
		listed, err := n.children(o.owner, o.parent.id)
		require.NoError(t, err)
		assert.Empty(t, listed)
	})
	t.Run("an organization with a root that is not its creator's own", func(t *testing.T) {
		for name, root := range roots {
			refused(t, withRoot(t, o.writer, testOrganizationType, "", root), name)
		}
	})
	t.Run("a header that binds no root", func(t *testing.T) {
		for name, change := range map[string]func(h *spacesyncproto.SpaceHeader){
			"a space of an organization without a root": func(h *spacesyncproto.SpaceHeader) { h.ParentSpaceId, h.AclPayload = o.parent.id, nil },
			"an organization without a root": func(h *spacesyncproto.SpaceHeader) {
				h.SpaceType, h.ParentSpaceId, h.AclPayload = testOrganizationType, "", nil
			},
			"an organization with a v0 header": func(h *spacesyncproto.SpaceHeader) {
				h.SpaceType, h.ParentSpaceId, h.Version = testOrganizationType, "", spacesyncproto.SpaceHeaderVersion_SpaceHeaderVersion0
			},
		} {
			spaceId, header := newRawHeader(t, o.writer, change)
			refused(t, space{id: spaceId, header: header}, name)
		}
	})
	t.Run("the root of the creator", func(t *testing.T) {
		organization := withRoot(t, o.writer, testOrganizationType, "", newAclRoot(t, o.writer.SignKey))
		require.NoError(t, n.sign(o.writer, organization, false))
		child := withRoot(t, o.writer, testOrganizationSpaceType, o.parent.id, newAclRoot(t, o.writer.SignKey))
		require.NoError(t, n.sign(o.writer, child, false))
		assert.Equal(t, list.AclPermissionsOwner, n.permissions(child.id, o.writer))
	})
	t.Run("a space of another type keeps the root it is given", func(t *testing.T) {
		regular := withRoot(t, o.writer, testAnySpaceType, "", roots["the root of an outsider"])
		require.NoError(t, n.sign(o.writer, regular, false))
		owner, err := n.c.acl.OwnerPubKey(context.Background(), regular.id)
		require.NoError(t, err)
		assert.True(t, owner.Equals(outsider.SignKey.GetPublic()))

		// a one-to-one space is signed, and its acl holds both accounts as writers
		s := space{id: oneToOne.SpaceHeaderWithId.Id, header: oneToOne.SpaceHeaderWithId.RawHeader, root: oneToOne.AclWithId}
		require.NoError(t, n.sign(o.writer, s, false))
		for _, account := range []*accountdata.AccountKeys{o.writer, outsider} {
			permissions, err := n.c.acl.Permissions(context.Background(), account.SignKey.GetPublic(), s.id)
			require.NoError(t, err)
			assert.Equal(t, list.AclPermissionsWriter, permissions)
		}
	})
}
