package coordinator

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
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

// Only the owner of a child lets an account that is not in the parent in, as the identity of a guest
// key.
func TestChildScenario_GuestByChildAdmin(t *testing.T) {
	n := newNetwork(t, roomyLimits)
	o := n.newOrg()
	child := n.mustCreateChild(o.writer, o.parent.id)
	require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsAdmin, o.admin))

	guest := newAccount(t)
	require.ErrorIs(t, n.add(child.id, o.admin, list.AclPermissionsGuest, guest), coordinatorproto.ErrNotParentMember)
	assert.Equal(t, list.AclPermissionsNone, n.permissions(child.id, guest))
	// a member of the parent is let in as a guest by an Admin of the child
	require.NoError(t, n.add(child.id, o.admin, list.AclPermissionsGuest, o.reader))
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
