package coordinator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/acl"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/peer/mock_peer"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/pool/mock_pool"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/anyproto/any-sync/testutil/accounttest"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/acleventlog"
	"github.com/anyproto/any-sync-coordinator/config"
	"github.com/anyproto/any-sync-coordinator/coordinatorlog"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/deletionlog"
	"github.com/anyproto/any-sync-coordinator/fileusage"
	"github.com/anyproto/any-sync-coordinator/fileusage/mock_fileusage"
	"github.com/anyproto/any-sync-coordinator/inbox"
	"github.com/anyproto/any-sync-coordinator/inbox/mock_inbox"
	"github.com/anyproto/any-sync-coordinator/invitestore"
	"github.com/anyproto/any-sync-coordinator/invitestore/mock_invitestore"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
	"github.com/anyproto/any-sync-coordinator/subscribe"
	"github.com/anyproto/any-sync-coordinator/subscribe/mock_subscribe"
)

// The scenarios below run child spaces through the rpc handlers of a coordinator whose space status,
// account limits and logs live in mongo and whose acl service reads acl logs from an in-memory
// consensus. Every account acts with its own key, and every space is made as a device makes it.

// paymentPeerId is the peer of the payment node, the one peer that sets account limits
const paymentPeerId = "payment.peer"

// roomyLimits hold none of the scenarios back
var roomyLimits = accountlimit.SpaceLimits{SharedSpacesLimit: 10, ChildSpacesLimit: 10}

// network is the coordinator of the scenarios with the components it runs with
type network struct {
	t           *testing.T
	c           *coordinator
	consensus   *testConsensus
	db          db.Database
	spaceStatus spacestatus.SpaceStatus
}

// newNetwork starts a coordinator that gives an account without a limits document these limits
func newNetwork(t *testing.T, limits accountlimit.SpaceLimits) *network {
	ctrl := gomock.NewController(t)
	nodeConf := mock_nodeconf.NewMockService(ctrl)
	inboxService := mock_inbox.NewMockInboxService(ctrl)
	subscribeService := mock_subscribe.NewMockSubscribeService(ctrl)
	fileUsage := mock_fileusage.NewMockFileUsage(ctrl)
	peerPool := mock_pool.NewMockService(ctrl)
	inviteStore := mock_invitestore.NewMockInviteStore(ctrl)
	anymock.ExpectComp(nodeConf.EXPECT(), nodeconf.CName)
	anymock.ExpectComp(inboxService.EXPECT(), inbox.CName)
	anymock.ExpectComp(subscribeService.EXPECT(), subscribe.CName)
	anymock.ExpectComp(fileUsage.EXPECT(), fileusage.CName)
	anymock.ExpectComp(peerPool.EXPECT(), pool.CName)
	anymock.ExpectComp(inviteStore.EXPECT(), invitestore.CName)
	// without a network id the acl service asks for no acceptor signature on the records
	nodeConf.EXPECT().Configuration().Return(nodeconf.Configuration{}).AnyTimes()
	// the payment node sets limits, and a file node takes the storage limit of each
	nodeConf.EXPECT().NodeTypes(paymentPeerId).Return([]nodeconf.NodeType{nodeconf.NodeTypePaymentProcessingNode}).AnyTimes()
	nodeConf.EXPECT().FilePeers().Return([]string{"file.peer"}).AnyTimes()
	filePeer := mock_peer.NewMockPeer(ctrl)
	filePeer.EXPECT().DoDrpc(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	peerPool.EXPECT().GetOneOf(gomock.Any(), []string{"file.peer"}).Return(filePeer, nil).AnyTimes()

	limits.SpaceMembersRead, limits.SpaceMembersWrite = 100, 100
	n := &network{
		t:           t,
		c:           New().(*coordinator),
		consensus:   newTestConsensus(),
		db:          db.New(),
		spaceStatus: spacestatus.New(),
	}
	a := new(app.App)
	// the account goes first: the coordinator takes the key that signs receipts from it in its Init
	a.Register(&accounttest.AccountTestService{}).
		Register(n.c).
		Register(nodeConf).
		Register(&config.Config{
			Mongo: db.Mongo{
				Connect:  "mongodb://localhost:27017",
				Database: fmt.Sprintf("coordinator_unittest_children_%d", time.Now().UnixNano()),
			},
			// the deleter looks every second for spaces whose deletion period is over
			SpaceStatus:   spacestatus.Config{RunSeconds: 1, DeletionPeriodDays: 1},
			AccountLimits: limits,
		}).
		Register(n.db).
		Register(n.spaceStatus).
		Register(coordinatorlog.New()).
		Register(acleventlog.New()).
		Register(metric.New()).
		Register(deletionlog.New()).
		Register(inboxService).
		Register(subscribeService).
		Register(accountlimit.New()).
		Register(fileUsage).
		Register(peerPool).
		Register(inviteStore).
		Register(rpctest.NewTestServer()).
		Register(acl.New()).
		Register(n.consensus)
	require.NoError(t, a.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, a.Close(context.Background()))
		require.NoError(t, n.db.Db().Drop(context.Background()))
	})
	return n
}

// ctx is the context of a request the account makes from its device
func (n *network) ctx(account *accountdata.AccountKeys) context.Context {
	return accountCtx(n.t, account.SignKey.GetPublic())
}

// space is a space as the device of its creator made it: the signed header and the root of its acl
type space struct {
	id     string
	header []byte
	root   *consensusproto.RawRecordWithId
}

// newSpace creates a space on the device of the creator, under a parent when parentSpaceId is set
func newSpace(t *testing.T, creator *accountdata.AccountKeys, spaceType, parentSpaceId string) space {
	masterKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	metadataKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	readKey, err := crypto.NewRandomAES()
	require.NoError(t, err)
	payload, err := spacepayloads.StoragePayloadForSpaceCreateV1(spacepayloads.SpaceCreatePayload{
		SigningKey:       creator.SignKey,
		SpaceType:        spaceType,
		ReplicationKey:   42,
		MasterKey:        masterKey,
		ReadKey:          readKey,
		MetadataKey:      metadataKey,
		Metadata:         []byte("metadata"),
		FileProtoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
		ParentSpaceId:    parentSpaceId,
	})
	require.NoError(t, err)
	return space{id: payload.SpaceHeaderWithId.Id, header: payload.SpaceHeaderWithId.RawHeader, root: payload.AclWithId}
}

// newTechSpace derives the tech space of the account
func newTechSpace(t *testing.T, account *accountdata.AccountKeys) space {
	masterKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	payload, err := spacepayloads.StoragePayloadForSpaceDeriveV1(spacepayloads.SpaceDerivePayload{
		SigningKey:       account.SignKey,
		MasterKey:        masterKey,
		SpaceType:        "any.techspace",
		FileProtoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
	})
	require.NoError(t, err)
	return space{id: payload.SpaceHeaderWithId.Id, header: payload.SpaceHeaderWithId.RawHeader, root: payload.AclWithId}
}

// signOnly asks the coordinator to sign the space for the account
func (n *network) signOnly(account *accountdata.AccountKeys, s space, force bool) error {
	resp, err := n.c.drpcHandler.SpaceSign(n.ctx(account), &coordinatorproto.SpaceSignRequest{
		SpaceId:      s.id,
		Header:       s.header,
		ForceRequest: force,
	})
	if err == nil && resp.Receipt == nil {
		return errors.New("a sign without a receipt")
	}
	return err
}

// push puts the acl of a signed space on the network, as the push of the space does
func (n *network) push(s space) {
	if s.root == nil {
		return
	}
	if err := n.consensus.AddLog(context.Background(), s.id, s.root); err != nil {
		require.ErrorIs(n.t, err, consensuserr.ErrLogExists)
	}
}

// sign signs the space for the account and pushes it
func (n *network) sign(account *accountdata.AccountKeys, s space, force bool) error {
	if err := n.signOnly(account, s, force); err != nil {
		return err
	}
	n.push(s)
	return nil
}

func (n *network) makeShareable(account *accountdata.AccountKeys, spaceId string) error {
	_, err := n.c.drpcHandler.SpaceMakeShareable(n.ctx(account), &coordinatorproto.SpaceMakeShareableRequest{SpaceId: spaceId})
	return err
}

// newSharedSpace creates a top-level any.space of the owner and shares it
func (n *network) newSharedSpace(owner *accountdata.AccountKeys) space {
	s := newSpace(n.t, owner, testAnySpaceType, "")
	require.NoError(n.t, n.sign(owner, s, false))
	require.NoError(n.t, n.makeShareable(owner, s.id))
	return s
}

// createChild creates a child of the parent on the device of the creator and pushes it
func (n *network) createChild(creator *accountdata.AccountKeys, parentSpaceId string) (space, error) {
	child := newSpace(n.t, creator, testAnySpaceType, parentSpaceId)
	return child, n.sign(creator, child, false)
}

func (n *network) mustCreateChild(creator *accountdata.AccountKeys, parentSpaceId string) space {
	child, err := n.createChild(creator, parentSpaceId)
	require.NoError(n.t, err)
	return child
}

// aclList builds the acl of the space from the network, as the device of the account does
func (n *network) aclList(spaceId string, account *accountdata.AccountKeys) list.AclList {
	storage, err := list.NewInMemoryStorage(spaceId, n.consensus.log(spaceId))
	require.NoError(n.t, err)
	l, err := list.BuildAclListWithIdentity(account, storage, recordverifier.NewValidateFull())
	require.NoError(n.t, err)
	return l
}

func (n *network) builder(spaceId string, account *accountdata.AccountKeys) list.AclRecordBuilder {
	return n.aclList(spaceId, account).RecordBuilder()
}

func (n *network) permissions(spaceId string, account *accountdata.AccountKeys) list.AclPermissions {
	return n.aclList(spaceId, account).AclState().Permissions(account.SignKey.GetPublic())
}

func (n *network) rotationPending(spaceId string) bool {
	return n.aclList(spaceId, newAccount(n.t)).AclState().RotationPending()
}

// send sends the record the author built to the coordinator, then lets the network deliver what the
// coordinator took. A record that fails the check of the author's device is sent all the same: the
// coordinator is the one that decides.
func (n *network) send(spaceId string, author *accountdata.AccountKeys, rec *consensusproto.RawRecord, buildErr error) (*coordinatorproto.AclAddRecordResponse, error) {
	if rec == nil {
		require.Error(n.t, buildErr)
		return nil, buildErr
	}
	payload, err := rec.MarshalVT()
	require.NoError(n.t, err)
	resp, err := n.c.drpcHandler.AclAddRecord(n.ctx(author), &coordinatorproto.AclAddRecordRequest{SpaceId: spaceId, Payload: payload})
	n.consensus.deliver()
	return resp, err
}

func (n *network) sendErr(spaceId string, author *accountdata.AccountKeys, rec *consensusproto.RawRecord, buildErr error) error {
	_, err := n.send(spaceId, author, rec, buildErr)
	return err
}

// add adds the accounts to the space with the permissions, as the author
func (n *network) add(spaceId string, author *accountdata.AccountKeys, permissions list.AclPermissions, accounts ...*accountdata.AccountKeys) error {
	additions := make([]list.AccountAdd, 0, len(accounts))
	for _, account := range accounts {
		additions = append(additions, list.AccountAdd{
			Identity:    account.SignKey.GetPublic(),
			Permissions: permissions,
			Metadata:    []byte("metadata"),
		})
	}
	rec, err := n.builder(spaceId, author).BuildAccountsAdd(list.AccountsAddPayload{Additions: additions})
	return n.sendErr(spaceId, author, rec, err)
}

// remove removes the accounts from the space and rotates the read key, as the author
func (n *network) remove(spaceId string, author *accountdata.AccountKeys, accounts ...*accountdata.AccountKeys) error {
	rec, err := n.builder(spaceId, author).BuildAccountRemove(removePayload(n.t, accounts...))
	return n.sendErr(spaceId, author, rec, err)
}

// removeKeyless removes the accounts from the space without the read key, as the author
func (n *network) removeKeyless(spaceId string, author *accountdata.AccountKeys, accounts ...*accountdata.AccountKeys) error {
	rec, err := n.builder(spaceId, author).BuildAccountRemoveNoRotate(pubKeys(accounts...))
	return n.sendErr(spaceId, author, rec, err)
}

// rotate changes the read key of the space, as the author
func (n *network) rotate(spaceId string, author *accountdata.AccountKeys) error {
	rec, err := n.builder(spaceId, author).BuildReadKeyChange(removePayload(n.t).Change)
	return n.sendErr(spaceId, author, rec, err)
}

func (n *network) setPermissions(spaceId string, author, account *accountdata.AccountKeys, permissions list.AclPermissions) error {
	rec, err := n.builder(spaceId, author).BuildPermissionChange(list.PermissionChangePayload{
		Identity:    account.SignKey.GetPublic(),
		Permissions: permissions,
	})
	return n.sendErr(spaceId, author, rec, err)
}

// transfer hands the space to a member, as its owner
func (n *network) transfer(spaceId string, owner, newOwner *accountdata.AccountKeys, kept list.AclPermissions) error {
	rec, err := n.builder(spaceId, owner).BuildOwnershipChange(list.OwnershipChangePayload{
		NewOwner:            newOwner.SignKey.GetPublic(),
		OldOwnerPermissions: kept,
	})
	return n.sendErr(spaceId, owner, rec, err)
}

// changeOwnerAsParent hands a child to a member, as an account that acts on the authority of the parent
func (n *network) changeOwnerAsParent(childId string, author, newOwner *accountdata.AccountKeys, kept list.AclPermissions) error {
	rec, err := n.builder(childId, author).BuildOwnershipChangeByParent(list.OwnershipChangePayload{
		NewOwner:            newOwner.SignKey.GetPublic(),
		OldOwnerPermissions: kept,
	})
	return n.sendErr(childId, author, rec, err)
}

// deleteSpace asks the coordinator to delete the space for the account, with an hour to undo it
func (n *network) deleteSpace(account *accountdata.AccountKeys, spaceId string) error {
	_, err := n.c.drpcHandler.SpaceDelete(n.ctx(account), &coordinatorproto.SpaceDeleteRequest{
		SpaceId:           spaceId,
		DeletionPayload:   []byte("payload"),
		DeletionPayloadId: "payload.id",
		DeletionDuration:  60,
	})
	return err
}

// revertDeletion takes back the pending deletion of the space through the deprecated status change
func (n *network) revertDeletion(account *accountdata.AccountKeys, spaceId string) error {
	_, err := n.c.drpcHandler.SpaceStatusChange(n.ctx(account), &coordinatorproto.SpaceStatusChangeRequest{SpaceId: spaceId})
	return err
}

// deleteAccount asks the coordinator to delete the account, which needs its tech space signed
func (n *network) deleteAccount(account *accountdata.AccountKeys) error {
	confirm, err := coordinatorproto.PrepareAccountDeleteConfirmation(account.SignKey, "peer.id", "")
	require.NoError(n.t, err)
	payload, err := confirm.MarshalVT()
	require.NoError(n.t, err)
	_, err = n.c.drpcHandler.AccountDelete(n.ctx(account), &coordinatorproto.AccountDeleteRequest{
		DeletionPayload:   payload,
		DeletionPayloadId: "payload.id",
	})
	return err
}

func (n *network) revertAccountDeletion(account *accountdata.AccountKeys) error {
	_, err := n.c.drpcHandler.AccountRevertDeletion(n.ctx(account), &coordinatorproto.AccountRevertDeletionRequest{})
	return err
}

// endDeletionPeriod lets the deletion period of the space pass and waits for the deleter to make the
// deletion final
func (n *network) endDeletionPeriod(spaceId string) {
	res, err := n.db.Db().Collection("spaces").UpdateOne(context.Background(),
		bson.M{"_id": spaceId, "status": spacestatus.SpaceStatusDeletionPending},
		bson.M{"$set": bson.M{"toBeDeletedTimestamp": time.Now().Add(-time.Second).Unix()}},
	)
	require.NoError(n.t, err)
	require.Equal(n.t, int64(1), res.MatchedCount, "the deletion of the space is not pending")
	require.Eventually(n.t, func() bool {
		return n.status(spaceId).Status == spacestatus.SpaceStatusDeleted
	}, 10*time.Second, 20*time.Millisecond)
}

func (n *network) status(spaceId string) spacestatus.StatusEntry {
	entry, err := n.spaceStatus.Status(context.Background(), spaceId)
	require.NoError(n.t, err)
	return entry
}

func (n *network) registered(spaceId string) bool {
	_, err := n.spaceStatus.Status(context.Background(), spaceId)
	if errors.Is(err, coordinatorproto.ErrSpaceNotExists) {
		return false
	}
	require.NoError(n.t, err)
	return true
}

// statusCheck is what SpaceStatusCheck and SpaceStatusCheckMany tell the account about the space; both
// must tell the same
func (n *network) statusCheck(account *accountdata.AccountKeys, spaceId string) *coordinatorproto.SpaceStatusPayload {
	one, err := n.c.drpcHandler.SpaceStatusCheck(n.ctx(account), &coordinatorproto.SpaceStatusCheckRequest{SpaceId: spaceId})
	require.NoError(n.t, err)
	many, err := n.c.drpcHandler.SpaceStatusCheckMany(n.ctx(account), &coordinatorproto.SpaceStatusCheckManyRequest{SpaceIds: []string{spaceId}})
	require.NoError(n.t, err)
	require.Len(n.t, many.Payloads, 1)
	assert.Equal(n.t, one.Payload.Permissions, many.Payloads[0].Permissions)
	assert.Equal(n.t, one.Payload.Status, many.Payloads[0].Status)
	assert.Equal(n.t, one.Payload.IsShared, many.Payloads[0].IsShared)
	return one.Payload
}

// accountLimits are the limits SpaceStatusCheckMany reports to the account
func (n *network) accountLimits(account *accountdata.AccountKeys) *coordinatorproto.AccountLimits {
	resp, err := n.c.drpcHandler.SpaceStatusCheckMany(n.ctx(account), &coordinatorproto.SpaceStatusCheckManyRequest{})
	require.NoError(n.t, err)
	return resp.AccountLimits
}

// setLimits sets limits of the account, as the payment node does
func (n *network) setLimits(account *accountdata.AccountKeys, req *coordinatorproto.AccountLimitsSetRequest) {
	req.Identity = account.SignKey.GetPublic().Account()
	_, err := n.c.drpcHandler.AccountLimitsSet(peer.CtxWithPeerId(context.Background(), paymentPeerId), req)
	require.NoError(n.t, err)
}

// children lists the children of the parent with their statuses, for the account
func (n *network) children(account *accountdata.AccountKeys, parentSpaceId string) (map[string]coordinatorproto.SpaceStatus, error) {
	resp, err := n.c.drpcHandler.SpaceChildren(n.ctx(account), &coordinatorproto.SpaceChildrenRequest{SpaceId: parentSpaceId})
	if err != nil {
		return nil, err
	}
	children := map[string]coordinatorproto.SpaceStatus{}
	for _, child := range resp.Children {
		children[child.SpaceId] = child.Status
	}
	return children, nil
}

// org is a shared parent space with an account in each role of it
type org struct {
	parent                       space
	owner, admin, writer, reader *accountdata.AccountKeys
}

func (n *network) newOrg() org {
	o := org{owner: newAccount(n.t), admin: newAccount(n.t), writer: newAccount(n.t), reader: newAccount(n.t)}
	o.parent = n.newSharedSpace(o.owner)
	n.join(o, list.AclPermissionsAdmin, o.admin)
	n.join(o, list.AclPermissionsWriter, o.writer)
	n.join(o, list.AclPermissionsReader, o.reader)
	return o
}

// join adds the accounts to the parent, as its owner
func (n *network) join(o org, permissions list.AclPermissions, accounts ...*accountdata.AccountKeys) {
	require.NoError(n.t, n.add(o.parent.id, o.owner, permissions, accounts...))
}

// newMembers makes new accounts members of the parent
func (n *network) newMembers(o org, permissions list.AclPermissions, count int) []*accountdata.AccountKeys {
	accounts := make([]*accountdata.AccountKeys, 0, count)
	for range count {
		accounts = append(accounts, newAccount(n.t))
	}
	n.join(o, permissions, accounts...)
	return accounts
}

func accountOf(account *accountdata.AccountKeys) string {
	return account.SignKey.GetPublic().Account()
}

func TestChildScenario_Creation(t *testing.T) {
	t.Run("a Writer, an Admin and the Owner of the parent create children, nobody below them", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		outsider, removed := newAccount(t), newAccount(t)
		n.join(o, list.AclPermissionsWriter, removed)
		require.NoError(t, n.remove(o.parent.id, o.owner, removed))
		parentLog := len(n.consensus.log(o.parent.id))

		var created []string
		for name, creator := range map[string]*accountdata.AccountKeys{"a Writer": o.writer, "an Admin": o.admin, "the Owner": o.owner} {
			child, err := n.createChild(creator, o.parent.id)
			require.NoError(t, err, name)
			entry := n.status(child.id)
			assert.Equal(t, o.parent.id, entry.ParentSpaceId, name)
			assert.Equal(t, accountOf(creator), entry.Identity, name)
			assert.Equal(t, spacestatus.SpaceStatusCreated, entry.Status, name)
			assert.Equal(t, list.AclPermissionsOwner, n.permissions(child.id, creator), name)
			created = append(created, child.id)
		}
		for name, creator := range map[string]*accountdata.AccountKeys{"a Reader": o.reader, "an outsider": outsider, "a removed member": removed} {
			child, err := n.createChild(creator, o.parent.id)
			require.ErrorIs(t, err, coordinatorproto.ErrParentPermissionDenied, name)
			assert.False(t, n.registered(child.id), name)
		}

		// nothing is written to the parent
		assert.Len(t, n.consensus.log(o.parent.id), parentLog)
		listed, err := n.children(o.owner, o.parent.id)
		require.NoError(t, err)
		assert.ElementsMatch(t, created, slices.Collect(maps.Keys(listed)))
	})

	t.Run("a creator who lost the role is refused a new child, its registered child is signed again", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		first := n.mustCreateChild(o.writer, o.parent.id)

		require.NoError(t, n.setPermissions(o.parent.id, o.owner, o.writer, list.AclPermissionsReader))
		second := newSpace(t, o.writer, testAnySpaceType, o.parent.id)
		require.ErrorIs(t, n.sign(o.writer, second, false), coordinatorproto.ErrParentPermissionDenied)
		assert.False(t, n.registered(second.id))
		require.NoError(t, n.sign(o.writer, first, false))

		// out of the parent altogether
		require.NoError(t, n.remove(o.parent.id, o.owner, o.writer))
		require.NoError(t, n.sign(o.writer, first, false))
		require.NoError(t, n.sign(o.writer, first, true))
		require.ErrorIs(t, n.sign(o.writer, second, false), coordinatorproto.ErrParentPermissionDenied)

		// back as a Writer
		n.join(o, list.AclPermissionsWriter, o.writer)
		require.NoError(t, n.sign(o.writer, second, false))
		assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(first.id).Status)
		assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(second.id).Status)
	})

	t.Run("a space that cannot be a parent", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		creator := newAccount(t)

		notSigned := newSpace(t, creator, testAnySpaceType, "")
		pending := n.newSharedSpace(creator)
		require.NoError(t, n.deleteSpace(creator, pending.id))
		deleted := n.newSharedSpace(creator)
		require.NoError(t, n.deleteSpace(creator, deleted.id))
		n.endDeletionPeriod(deleted.id)
		child := n.mustCreateChild(creator, n.newSharedSpace(creator).id)
		tech := newTechSpace(t, creator)
		require.NoError(t, n.sign(creator, tech, false))
		oneToOnePayload, err := spacepayloads.StoragePayloadForOneToOneSpaceWithType(creator.SignKey, newAccount(t).SignKey.GetPublic(), spacepayloads.SpaceTypeOneToOneAny)
		require.NoError(t, err)
		oneToOne := space{id: oneToOnePayload.SpaceHeaderWithId.Id, header: oneToOnePayload.SpaceHeaderWithId.RawHeader}
		require.NoError(t, n.sign(creator, oneToOne, false))
		anytypeSpace := newSpace(t, creator, "anytype.space", "")
		require.NoError(t, n.sign(creator, anytypeSpace, false))
		require.NoError(t, n.makeShareable(creator, anytypeSpace.id))
		chatSpace := newSpace(t, creator, "anytype.chatspace", "")
		require.NoError(t, n.sign(creator, chatSpace, false))

		for name, tc := range map[string]struct {
			parentSpaceId string
			err           error
		}{
			"a space that was never signed":     {notSigned.id, coordinatorproto.ErrParentSpaceNotAvailable},
			"a space whose deletion is pending": {pending.id, coordinatorproto.ErrParentSpaceNotAvailable},
			"a deleted space":                   {deleted.id, coordinatorproto.ErrParentSpaceNotAvailable},
			"a child":                           {child.id, coordinatorproto.ErrParentSpaceInvalid},
			"a tech space":                      {tech.id, coordinatorproto.ErrParentSpaceInvalid},
			"a 1-1 space":                       {oneToOne.id, coordinatorproto.ErrParentSpaceInvalid},
			"an anytype space":                  {anytypeSpace.id, coordinatorproto.ErrParentSpaceInvalid},
			"an anytype chat space":             {chatSpace.id, coordinatorproto.ErrParentSpaceInvalid},
		} {
			grandchild, err := n.createChild(creator, tc.parentSpaceId)
			require.ErrorIs(t, err, tc.err, name)
			assert.False(t, n.registered(grandchild.id), name)
		}
	})

	t.Run("a child that is not a v1 any.space", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		v0Id, v0Header := newRawHeader(t, o.writer, func(h *spacesyncproto.SpaceHeader) {
			h.ParentSpaceId = o.parent.id
			h.Version = spacesyncproto.SpaceHeaderVersion_SpaceHeaderVersion0
		})
		techId, techHeader := newRawHeader(t, o.writer, func(h *spacesyncproto.SpaceHeader) {
			h.ParentSpaceId = o.parent.id
			h.SpaceType = "any.techspace"
		})
		for name, child := range map[string]space{
			"a v0 header":      {id: v0Id, header: v0Header},
			"an anytype space": newSpace(t, o.writer, "anytype.space", o.parent.id),
			"a chat space":     newSpace(t, o.writer, "anytype.chatspace", o.parent.id),
			"a tech space":     {id: techId, header: techHeader},
		} {
			require.ErrorIs(t, n.sign(o.writer, child, false), coordinatorproto.ErrParentSpaceInvalid, name)
			assert.False(t, n.registered(child.id), name)
		}
	})

	t.Run("a deleted child answers as a deleted top-level space does", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		child := n.mustCreateChild(o.writer, o.parent.id)
		topLevel := n.newSharedSpace(o.writer)

		for _, s := range []space{child, topLevel} {
			require.NoError(t, n.deleteSpace(o.writer, s.id))
		}
		require.ErrorIs(t, n.sign(o.writer, topLevel, false), coordinatorproto.ErrSpaceIsDeleted)
		require.ErrorIs(t, n.sign(o.writer, child, false), coordinatorproto.ErrSpaceIsDeleted)

		for _, s := range []space{child, topLevel} {
			n.endDeletionPeriod(s.id)
		}
		require.ErrorIs(t, n.sign(o.writer, topLevel, false), coordinatorproto.ErrSpaceIsDeleted)
		require.ErrorIs(t, n.sign(o.writer, child, false), coordinatorproto.ErrSpaceIsDeleted)
		assert.Equal(t, spacestatus.SpaceStatusDeleted, n.status(child.id).Status)
	})

}

func TestChildScenario_Limit(t *testing.T) {
	t.Run("the children under every parent of one owner share its limit, whoever creates them", func(t *testing.T) {
		n := newNetwork(t, accountlimit.SpaceLimits{SharedSpacesLimit: 10, ChildSpacesLimit: 3})
		o := n.newOrg()
		second := n.newSharedSpace(o.owner)
		secondWriter := newAccount(t)
		require.NoError(t, n.add(second.id, o.owner, list.AclPermissionsWriter, secondWriter))

		n.mustCreateChild(o.writer, o.parent.id)
		n.mustCreateChild(o.admin, o.parent.id)
		n.mustCreateChild(secondWriter, second.id)
		for name, tc := range map[string]struct {
			creator *accountdata.AccountKeys
			parent  space
		}{
			"the Owner":              {o.owner, second},
			"a Writer of the first":  {o.writer, o.parent},
			"an Admin of the first":  {o.admin, o.parent},
			"a Writer of the second": {secondWriter, second},
		} {
			child, err := n.createChild(tc.creator, tc.parent.id)
			require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached, name)
			assert.False(t, n.registered(child.id), name)
		}
		assert.Equal(t, uint32(3), n.accountLimits(o.owner).ChildSpacesLimit)

		// the limit of another owner is its own
		other := n.newOrg()
		for range 3 {
			n.mustCreateChild(other.writer, other.parent.id)
		}
		_, err := n.createChild(other.writer, other.parent.id)
		require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)
	})

	t.Run("shared spaces take no place of a child and children take none of the shared spaces", func(t *testing.T) {
		n := newNetwork(t, accountlimit.SpaceLimits{SharedSpacesLimit: 2, ChildSpacesLimit: 2})
		o := n.newOrg()
		n.newSharedSpace(o.owner)
		third := newSpace(t, o.owner, testAnySpaceType, "")
		require.NoError(t, n.sign(o.owner, third, false))
		require.ErrorIs(t, n.makeShareable(o.owner, third.id), coordinatorproto.ErrSpaceLimitReached)

		// the owner of the parent is at its shared-spaces limit and has its two children
		first := n.mustCreateChild(o.writer, o.parent.id)
		n.mustCreateChild(o.writer, o.parent.id)
		_, err := n.createChild(o.writer, o.parent.id)
		require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)
		require.ErrorIs(t, n.makeShareable(o.owner, third.id), coordinatorproto.ErrSpaceLimitReached)

		// a child takes members with no step that makes it shareable, and that step changes nothing
		assert.True(t, n.status(first.id).IsShareable)
		require.NoError(t, n.add(first.id, o.writer, list.AclPermissionsReader, o.reader))
		require.NoError(t, n.makeShareable(o.writer, first.id))

		// the creator of two children still shares two spaces of its own
		for range 2 {
			own := newSpace(t, o.writer, testAnySpaceType, "")
			require.NoError(t, n.sign(o.writer, own, false))
			require.NoError(t, n.makeShareable(o.writer, own.id))
		}
		own := newSpace(t, o.writer, testAnySpaceType, "")
		require.NoError(t, n.sign(o.writer, own, false))
		require.ErrorIs(t, n.makeShareable(o.writer, own.id), coordinatorproto.ErrSpaceLimitReached)
	})

	t.Run("a parent handed to another owner takes its children to the count of that owner", func(t *testing.T) {
		n := newNetwork(t, accountlimit.SpaceLimits{SharedSpacesLimit: 10, ChildSpacesLimit: 2})
		a, b := n.newOrg(), n.newOrg()
		first := n.mustCreateChild(a.writer, a.parent.id)
		n.mustCreateChild(a.writer, a.parent.id)
		n.mustCreateChild(b.writer, b.parent.id)
		another := n.newSharedSpace(a.owner)
		_, err := n.createChild(a.owner, another.id)
		require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)

		n.join(a, list.AclPermissionsAdmin, b.owner)
		require.NoError(t, n.transfer(a.parent.id, a.owner, b.owner, list.AclPermissionsAdmin))
		assert.Equal(t, coordinatorproto.SpacePermissions_SpacePermissionsOwner, n.statusCheck(b.owner, a.parent.id).Permissions)

		// the previous owner has room again, the new one has three children against its two
		n.mustCreateChild(a.owner, another.id)
		_, err = n.createChild(b.writer, b.parent.id)
		require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)
		_, err = n.createChild(a.writer, a.parent.id)
		require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)
		// the children keep their own owners
		assert.Equal(t, accountOf(a.writer), n.status(first.id).Identity)
	})

	t.Run("a child holds its place until its deletion is final", func(t *testing.T) {
		n := newNetwork(t, accountlimit.SpaceLimits{SharedSpacesLimit: 10, ChildSpacesLimit: 2})
		o := n.newOrg()
		first := n.mustCreateChild(o.writer, o.parent.id)
		second := n.mustCreateChild(o.admin, o.parent.id)

		require.NoError(t, n.deleteSpace(o.writer, first.id))
		_, err := n.createChild(o.writer, o.parent.id)
		require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)

		n.endDeletionPeriod(first.id)
		n.mustCreateChild(o.writer, o.parent.id)
		_, err = n.createChild(o.admin, o.parent.id)
		require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)

		// a forced sign takes the finally deleted child back only into a free place
		require.ErrorIs(t, n.sign(o.writer, first, true), coordinatorproto.ErrChildSpacesLimitReached)
		assert.Equal(t, spacestatus.SpaceStatusDeleted, n.status(first.id).Status)
		require.NoError(t, n.deleteSpace(o.admin, second.id))
		n.endDeletionPeriod(second.id)
		require.NoError(t, n.sign(o.writer, first, true))
		assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(first.id).Status)
		_, err = n.createChild(o.admin, o.parent.id)
		require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)
	})

	t.Run("a forced sign of a finally deleted child needs a creator who may create one", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		child := n.mustCreateChild(o.writer, o.parent.id)
		require.NoError(t, n.deleteSpace(o.writer, child.id))
		n.endDeletionPeriod(child.id)

		require.NoError(t, n.setPermissions(o.parent.id, o.owner, o.writer, list.AclPermissionsReader))
		require.ErrorIs(t, n.sign(o.writer, child, true), coordinatorproto.ErrParentPermissionDenied)
		assert.Equal(t, spacestatus.SpaceStatusDeleted, n.status(child.id).Status)
		require.NoError(t, n.setPermissions(o.parent.id, o.owner, o.writer, list.AclPermissionsWriter))
		require.NoError(t, n.sign(o.writer, child, true))
		assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(child.id).Status)
	})

	t.Run("signs under one parent at the same time do not pass the limit", func(t *testing.T) {
		n := newNetwork(t, accountlimit.SpaceLimits{SharedSpacesLimit: 10, ChildSpacesLimit: 3})
		o := n.newOrg()
		writers := n.newMembers(o, list.AclPermissionsWriter, 8)
		children := make([]space, len(writers))
		for i, writer := range writers {
			children[i] = newSpace(t, writer, testAnySpaceType, o.parent.id)
		}

		errs := make([]error, len(writers))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, writer := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				errs[i] = n.signOnly(writer, children[i], false)
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
		assert.Equal(t, 3, signed)
		listed, err := n.children(o.owner, o.parent.id)
		require.NoError(t, err)
		assert.Len(t, listed, 3)
	})

	t.Run("an account without a limit has no children", func(t *testing.T) {
		n := newNetwork(t, accountlimit.SpaceLimits{SharedSpacesLimit: 10})
		o := n.newOrg()
		child, err := n.createChild(o.owner, o.parent.id)
		require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)
		assert.False(t, n.registered(child.id))
	})
}

func TestChildScenario_Admission(t *testing.T) {
	// newChild is a child that a Writer of the parent owns
	newChild := func(t *testing.T) (*network, org, space) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		return n, o, n.mustCreateChild(o.writer, o.parent.id)
	}

	t.Run("an Admin or the owner of a child adds a member of the parent, not an outsider", func(t *testing.T) {
		n, o, child := newChild(t)
		outsider := newAccount(t)
		require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsReader, o.reader))
		require.ErrorIs(t, n.add(child.id, o.writer, list.AclPermissionsReader, outsider), coordinatorproto.ErrNotParentMember)

		require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsAdmin, o.admin))
		member := n.newMembers(o, list.AclPermissionsReader, 1)[0]
		require.NoError(t, n.add(child.id, o.admin, list.AclPermissionsWriter, member))
		require.ErrorIs(t, n.add(child.id, o.admin, list.AclPermissionsWriter, outsider), coordinatorproto.ErrNotParentMember)
		// one outsider refuses the whole record
		late := n.newMembers(o, list.AclPermissionsReader, 1)[0]
		rec, err := n.builder(child.id, o.writer).BuildAccountsAdd(list.AccountsAddPayload{Additions: []list.AccountAdd{
			{Identity: late.SignKey.GetPublic(), Permissions: list.AclPermissionsReader, Metadata: []byte("metadata")},
			{Identity: outsider.SignKey.GetPublic(), Permissions: list.AclPermissionsReader, Metadata: []byte("metadata")},
		}})
		require.ErrorIs(t, n.sendErr(child.id, o.writer, rec, err), coordinatorproto.ErrNotParentMember)

		assert.Equal(t, list.AclPermissionsReader, n.permissions(child.id, o.reader))
		assert.Equal(t, list.AclPermissionsWriter, n.permissions(child.id, member))
		assert.Equal(t, list.AclPermissionsNone, n.permissions(child.id, outsider))
		assert.Equal(t, list.AclPermissionsNone, n.permissions(child.id, late))
	})

	t.Run("a join request is posted and accepted only for a member of the parent", func(t *testing.T) {
		n, o, child := newChild(t)
		outsider, leaver := newAccount(t), newAccount(t)
		n.join(o, list.AclPermissionsWriter, leaver)
		invite, err := n.builder(child.id, o.writer).BuildInvite()
		require.NoError(t, n.sendErr(child.id, o.writer, invite.InviteRec, err))
		request := func(joiner *accountdata.AccountKeys) (string, error) {
			rec, err := n.builder(child.id, joiner).BuildRequestJoin(list.RequestJoinPayload{InviteKey: invite.InviteKey, Metadata: []byte("metadata")})
			resp, err := n.send(child.id, joiner, rec, err)
			if err != nil {
				return "", err
			}
			return resp.RecordId, nil
		}
		accept := func(requestId string) error {
			rec, err := n.builder(child.id, o.writer).BuildRequestAccept(list.RequestAcceptPayload{RequestRecordId: requestId, Permissions: list.AclPermissionsWriter})
			return n.sendErr(child.id, o.writer, rec, err)
		}

		_, err = request(outsider)
		require.ErrorIs(t, err, coordinatorproto.ErrNotParentMember)
		requests, err := n.aclList(child.id, o.writer).AclState().JoinRecords(false)
		require.NoError(t, err)
		assert.Empty(t, requests)

		readerRequest, err := request(o.reader)
		require.NoError(t, err)
		leaverRequest, err := request(leaver)
		require.NoError(t, err)
		require.NoError(t, accept(readerRequest))
		assert.Equal(t, list.AclPermissionsWriter, n.permissions(child.id, o.reader))

		// the leaver asks the parent to remove it and is removed before its request is accepted
		rec, err := n.builder(o.parent.id, leaver).BuildRequestRemove()
		require.NoError(t, n.sendErr(o.parent.id, leaver, rec, err))
		require.NoError(t, n.remove(o.parent.id, o.owner, leaver))
		require.ErrorIs(t, accept(leaverRequest), coordinatorproto.ErrNotParentMember)
		assert.Equal(t, list.AclPermissionsNone, n.permissions(child.id, leaver))
	})

	t.Run("an invite is joined only by a member of the parent", func(t *testing.T) {
		n, o, child := newChild(t)
		outsider := newAccount(t)
		invite, err := n.builder(child.id, o.writer).BuildInviteAnyone(list.AclPermissionsWriter)
		require.NoError(t, n.sendErr(child.id, o.writer, invite.InviteRec, err))
		join := func(joiner *accountdata.AccountKeys) error {
			rec, err := n.builder(child.id, joiner).BuildInviteJoinWithoutApprove(list.InviteJoinPayload{InviteKey: invite.InviteKey, Metadata: []byte("metadata")})
			return n.sendErr(child.id, joiner, rec, err)
		}
		require.ErrorIs(t, join(outsider), coordinatorproto.ErrNotParentMember)
		assert.Equal(t, list.AclPermissionsNone, n.permissions(child.id, outsider))
		require.NoError(t, join(o.reader))
		assert.Equal(t, list.AclPermissionsWriter, n.permissions(child.id, o.reader))
	})

	t.Run("the owner of a child gives an outsider read access with a guest key", func(t *testing.T) {
		n, o, child := newChild(t)
		guest := newAccount(t)
		require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsGuest, guest))
		assert.Equal(t, list.AclPermissionsGuest, n.permissions(child.id, guest))
	})
}

func TestChildScenario_Governance(t *testing.T) {
	t.Run("the Owner and an Admin of the parent remove members of a child they are not in", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		childWriter, childReader, otherWriter := newAccount(t), newAccount(t), newAccount(t)
		n.join(o, list.AclPermissionsWriter, childWriter, otherWriter)
		n.join(o, list.AclPermissionsReader, childReader)
		child := n.mustCreateChild(o.writer, o.parent.id)
		// a Reader of the parent administers the child
		require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsAdmin, o.reader))
		require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsWriter, childWriter))
		require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsReader, childReader))
		outsider := newAccount(t)

		require.NoError(t, n.removeKeyless(child.id, o.owner, childReader))
		assert.Equal(t, list.AclPermissionsNone, n.permissions(child.id, childReader))
		assert.True(t, n.rotationPending(child.id))
		// the rotation is for an Admin or the owner of the child
		require.Error(t, n.rotate(child.id, childWriter))
		assert.True(t, n.rotationPending(child.id))
		require.NoError(t, n.rotate(child.id, o.reader))
		assert.False(t, n.rotationPending(child.id))

		require.NoError(t, n.removeKeyless(child.id, o.admin, childWriter))
		assert.Equal(t, list.AclPermissionsNone, n.permissions(child.id, childWriter))
		require.NoError(t, n.rotate(child.id, o.writer))

		// the authority of the parent belongs to its Admins and Owner only
		for name, author := range map[string]*accountdata.AccountKeys{
			"a Writer of the parent": otherWriter,
			"an outsider":            outsider,
			"an Admin of the child, a Reader of the parent": o.reader,
		} {
			require.ErrorIs(t, n.removeKeyless(child.id, author, o.writer), coordinatorproto.ErrParentPermissionDenied, name)
		}
		// nobody removes the owner of the child without the key
		require.Error(t, n.removeKeyless(child.id, o.owner, o.writer))
		require.Error(t, n.removeKeyless(child.id, o.admin, o.writer))
		assert.Equal(t, list.AclPermissionsOwner, n.permissions(child.id, o.writer))
		assert.Equal(t, list.AclPermissionsAdmin, n.permissions(child.id, o.reader))
	})

	t.Run("an Admin of the parent removes from a child only whom it could remove from the parent", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		otherAdmin := n.newMembers(o, list.AclPermissionsAdmin, 1)[0]
		child := n.mustCreateChild(o.writer, o.parent.id)
		require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsWriter, o.owner, otherAdmin, o.reader))

		require.ErrorIs(t, n.removeKeyless(child.id, o.admin, otherAdmin), coordinatorproto.ErrParentPermissionDenied)
		require.ErrorIs(t, n.removeKeyless(child.id, o.admin, o.owner), coordinatorproto.ErrParentPermissionDenied)
		require.ErrorIs(t, n.removeKeyless(child.id, o.admin, o.reader, otherAdmin), coordinatorproto.ErrParentPermissionDenied)
		for _, kept := range []*accountdata.AccountKeys{o.owner, otherAdmin, o.reader} {
			assert.Equal(t, list.AclPermissionsWriter, n.permissions(child.id, kept))
		}
		require.NoError(t, n.removeKeyless(child.id, o.admin, o.reader))
		// the Owner of the parent removes anyone but the owner of the child
		require.NoError(t, n.removeKeyless(child.id, o.owner, otherAdmin))
		assert.Equal(t, list.AclPermissionsNone, n.permissions(child.id, otherAdmin))
	})

	t.Run("the Owner of the parent hands a child to a member, the previous owner keeps the role given", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		third := n.newMembers(o, list.AclPermissionsWriter, 1)[0]
		child := n.mustCreateChild(o.writer, o.parent.id)
		require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsWriter, o.reader, o.admin, third))

		previous := o.writer
		for _, step := range []struct {
			next *accountdata.AccountKeys
			kept list.AclPermissions
		}{
			{o.reader, list.AclPermissionsAdmin},
			{o.admin, list.AclPermissionsWriter},
			{third, list.AclPermissionsReader},
		} {
			require.NoError(t, n.changeOwnerAsParent(child.id, o.owner, step.next, step.kept))
			assert.Equal(t, list.AclPermissionsOwner, n.permissions(child.id, step.next))
			assert.Equal(t, step.kept, n.permissions(child.id, previous))
			assert.Equal(t, accountOf(step.next), n.status(child.id).Identity)
			assert.Equal(t, coordinatorproto.SpacePermissions_SpacePermissionsOwner, n.statusCheck(step.next, child.id).Permissions)
			assert.NotEqual(t, coordinatorproto.SpacePermissions_SpacePermissionsOwner, n.statusCheck(previous, child.id).Permissions)
			previous = step.next
		}
		// the Owner of the parent got nothing in the child
		assert.Equal(t, list.AclPermissionsNone, n.permissions(child.id, o.owner))
		// the new owner acts as the owner of the child
		require.NoError(t, n.add(child.id, third, list.AclPermissionsAdmin, n.newMembers(o, list.AclPermissionsReader, 1)[0]))
	})

	t.Run("an owner change by anyone but the Owner of the parent, or to an account outside the child", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		child := n.mustCreateChild(o.writer, o.parent.id)
		require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsWriter, o.reader, o.admin))
		outsider := newAccount(t)

		require.ErrorIs(t, n.changeOwnerAsParent(child.id, o.admin, o.reader, list.AclPermissionsAdmin), coordinatorproto.ErrParentPermissionDenied)
		require.ErrorIs(t, n.changeOwnerAsParent(child.id, o.reader, o.reader, list.AclPermissionsAdmin), coordinatorproto.ErrParentPermissionDenied)
		require.ErrorIs(t, n.changeOwnerAsParent(child.id, outsider, o.reader, list.AclPermissionsAdmin), coordinatorproto.ErrParentPermissionDenied)

		notInChild := n.newMembers(o, list.AclPermissionsWriter, 1)[0]
		for name, target := range map[string]*accountdata.AccountKeys{"an outsider": outsider, "a member of the parent outside the child": notInChild} {
			require.Error(t, n.changeOwnerAsParent(child.id, o.owner, target, list.AclPermissionsAdmin), name)
		}
		assert.Equal(t, list.AclPermissionsOwner, n.permissions(child.id, o.writer))
		assert.Equal(t, accountOf(o.writer), n.status(child.id).Identity)
	})

	t.Run("a new Owner of the parent governs the children at once, the previous Owner no longer", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		otherAdmin := n.newMembers(o, list.AclPermissionsAdmin, 1)[0]
		child := n.mustCreateChild(o.writer, o.parent.id)
		require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsWriter, o.reader, otherAdmin))

		require.NoError(t, n.transfer(o.parent.id, o.owner, o.admin, list.AclPermissionsAdmin))
		previous, current := o.owner, o.admin

		require.ErrorIs(t, n.changeOwnerAsParent(child.id, previous, o.reader, list.AclPermissionsAdmin), coordinatorproto.ErrParentPermissionDenied)
		require.ErrorIs(t, n.deleteSpace(previous, child.id), coordinatorproto.ErrParentPermissionDenied)
		require.ErrorIs(t, n.removeKeyless(child.id, previous, otherAdmin), coordinatorproto.ErrParentPermissionDenied)
		assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(child.id).Status)

		require.NoError(t, n.removeKeyless(child.id, current, otherAdmin))
		require.NoError(t, n.changeOwnerAsParent(child.id, current, o.reader, list.AclPermissionsWriter))
		assert.Equal(t, list.AclPermissionsOwner, n.permissions(child.id, o.reader))
		require.NoError(t, n.deleteSpace(current, child.id))
		assert.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(child.id).Status)
	})
}

func TestChildScenario_DeletionAndList(t *testing.T) {
	t.Run("the owner of a child and the Owner of the parent delete it, nobody else", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		otherWriter := n.newMembers(o, list.AclPermissionsWriter, 1)[0]
		first := n.mustCreateChild(o.writer, o.parent.id)
		require.NoError(t, n.add(first.id, o.writer, list.AclPermissionsAdmin, o.admin))
		require.NoError(t, n.add(first.id, o.writer, list.AclPermissionsWriter, o.reader))
		second := n.mustCreateChild(o.admin, o.parent.id)

		for name, account := range map[string]*accountdata.AccountKeys{
			"an Admin of the parent and of the child": o.admin,
			"a Writer of the child":                   o.reader,
			"a Writer of the parent":                  otherWriter,
			"an outsider":                             newAccount(t),
		} {
			require.ErrorIs(t, n.deleteSpace(account, first.id), coordinatorproto.ErrParentPermissionDenied, name)
		}
		assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(first.id).Status)

		require.NoError(t, n.deleteSpace(o.writer, first.id))
		assert.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(first.id).Status)
		require.NoError(t, n.deleteSpace(o.owner, second.id))
		assert.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(second.id).Status)
		assert.Equal(t, accountOf(o.admin), n.status(second.id).Identity)
	})

	t.Run("the owner of a child undoes a deletion it made", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		require.NoError(t, n.sign(o.writer, newTechSpace(t, o.writer), false))
		child := n.mustCreateChild(o.writer, o.parent.id)

		require.NoError(t, n.deleteSpace(o.writer, child.id))
		require.NoError(t, n.sign(o.writer, child, true))
		assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(child.id).Status)

		require.NoError(t, n.deleteSpace(o.writer, child.id))
		require.NoError(t, n.revertDeletion(o.writer, child.id))
		assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(child.id).Status)

		require.NoError(t, n.deleteAccount(o.writer))
		assert.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(child.id).Status)
		require.NoError(t, n.revertAccountDeletion(o.writer))
		assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(child.id).Status)
	})

	t.Run("a parent is deleted only once the deletion of all its children is final", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		first := n.mustCreateChild(o.writer, o.parent.id)
		second := n.mustCreateChild(o.admin, o.parent.id)
		refused := func() {
			require.ErrorIs(t, n.deleteSpace(o.owner, o.parent.id), coordinatorproto.ErrSpaceHasChildren)
			assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(o.parent.id).Status)
		}

		refused()
		require.NoError(t, n.deleteSpace(o.writer, first.id))
		refused()
		n.endDeletionPeriod(first.id)
		refused()
		require.NoError(t, n.deleteSpace(o.owner, second.id))
		refused()
		n.endDeletionPeriod(second.id)
		require.NoError(t, n.deleteSpace(o.owner, o.parent.id))
		assert.Equal(t, spacestatus.SpaceStatusDeletionPending, n.status(o.parent.id).Status)
	})

	t.Run("an Admin or the Owner of the parent lists its children until their deletion is final", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		kept := n.mustCreateChild(o.writer, o.parent.id)
		pending := n.mustCreateChild(o.admin, o.parent.id)
		deleted := n.mustCreateChild(o.owner, o.parent.id)
		other := n.newOrg()
		n.mustCreateChild(other.writer, other.parent.id)
		require.NoError(t, n.deleteSpace(o.admin, pending.id))
		require.NoError(t, n.deleteSpace(o.owner, deleted.id))
		n.endDeletionPeriod(deleted.id)

		for _, account := range []*accountdata.AccountKeys{o.owner, o.admin} {
			listed, err := n.children(account, o.parent.id)
			require.NoError(t, err)
			assert.Equal(t, map[string]coordinatorproto.SpaceStatus{
				kept.id:    coordinatorproto.SpaceStatus_SpaceStatusCreated,
				pending.id: coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion,
			}, listed)
		}
		for name, account := range map[string]*accountdata.AccountKeys{
			"a Writer who owns a child": o.writer,
			"a Reader":                  o.reader,
			"an outsider":               newAccount(t),
			"the Owner of another":      other.owner,
		} {
			_, err := n.children(account, o.parent.id)
			require.ErrorIs(t, err, coordinatorproto.ErrParentPermissionDenied, name)
		}
	})

	t.Run("the status of a child tells the caller its own place in the child", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		o := n.newOrg()
		child := n.mustCreateChild(o.writer, o.parent.id)
		require.NoError(t, n.add(child.id, o.writer, list.AclPermissionsWriter, o.reader))
		topLevel := n.newSharedSpace(o.writer)
		require.NoError(t, n.add(topLevel.id, o.writer, list.AclPermissionsWriter, o.reader))

		owner := n.statusCheck(o.writer, child.id)
		assert.Equal(t, coordinatorproto.SpacePermissions_SpacePermissionsOwner, owner.Permissions)
		assert.NotNil(t, owner.Limits)
		assert.True(t, owner.IsShared)
		assert.Equal(t, coordinatorproto.SpaceStatus_SpaceStatusCreated, owner.Status)

		// a member is told what a member of a top-level space is told
		member, memberOfTopLevel := n.statusCheck(o.reader, child.id), n.statusCheck(o.reader, topLevel.id)
		assert.Equal(t, memberOfTopLevel.Permissions, member.Permissions)
		assert.Equal(t, memberOfTopLevel.Limits, member.Limits)
		assert.True(t, member.IsShared)

		// the Owner of the parent is told what an outsider is told
		parentOwner, outsider := n.statusCheck(o.owner, child.id), n.statusCheck(newAccount(t), child.id)
		assert.NotEqual(t, coordinatorproto.SpacePermissions_SpacePermissionsOwner, parentOwner.Permissions)
		assert.Equal(t, outsider.Permissions, parentOwner.Permissions)
		assert.Nil(t, parentOwner.Limits)
	})
}

func TestChildScenario_TopLevel(t *testing.T) {
	t.Run("a top-level space gives nobody the authority of a parent", func(t *testing.T) {
		n := newNetwork(t, roomyLimits)
		owner, admin, writer, outsider := newAccount(t), newAccount(t), newAccount(t), newAccount(t)
		s := n.newSharedSpace(owner)
		require.NoError(t, n.add(s.id, owner, list.AclPermissionsAdmin, admin))
		require.NoError(t, n.add(s.id, owner, list.AclPermissionsWriter, writer))

		require.Error(t, n.removeKeyless(s.id, owner, writer))
		require.Error(t, n.removeKeyless(s.id, admin, writer))
		assert.Equal(t, list.AclPermissionsWriter, n.permissions(s.id, writer))
		require.Error(t, n.changeOwnerAsParent(s.id, admin, writer, list.AclPermissionsAdmin))
		require.Error(t, n.transfer(s.id, admin, writer, list.AclPermissionsAdmin))
		assert.Equal(t, list.AclPermissionsOwner, n.permissions(s.id, owner))
		require.Error(t, n.deleteSpace(admin, s.id))
		assert.Equal(t, spacestatus.SpaceStatusCreated, n.status(s.id).Status)

		// any account is added, and the owner hands the space over itself
		require.NoError(t, n.add(s.id, owner, list.AclPermissionsReader, outsider))
		require.NoError(t, n.transfer(s.id, owner, admin, list.AclPermissionsWriter))
		assert.Equal(t, accountOf(admin), n.status(s.id).Identity)
		require.NoError(t, n.deleteSpace(admin, s.id))
	})

	t.Run("sharing a top-level space and its limit work as before", func(t *testing.T) {
		n := newNetwork(t, accountlimit.SpaceLimits{SharedSpacesLimit: 1, ChildSpacesLimit: 10})
		owner, writer, stranger := newAccount(t), newAccount(t), newAccount(t)
		first := newSpace(t, owner, testAnySpaceType, "")
		require.NoError(t, n.sign(owner, first, false))
		require.ErrorIs(t, n.add(first.id, owner, list.AclPermissionsWriter, writer), coordinatorproto.ErrSpaceNotShareable)
		require.ErrorIs(t, n.makeShareable(stranger, first.id), coordinatorproto.ErrForbidden)
		require.NoError(t, n.makeShareable(owner, first.id))
		require.NoError(t, n.add(first.id, owner, list.AclPermissionsWriter, writer))
		assert.True(t, n.statusCheck(owner, first.id).IsShared)

		second := newSpace(t, owner, testAnySpaceType, "")
		require.NoError(t, n.sign(owner, second, false))
		require.ErrorIs(t, n.makeShareable(owner, second.id), coordinatorproto.ErrSpaceLimitReached)
		// a child of the shared space changes none of it
		n.mustCreateChild(writer, first.id)
		require.ErrorIs(t, n.makeShareable(owner, second.id), coordinatorproto.ErrSpaceLimitReached)
		assert.False(t, n.statusCheck(owner, second.id).IsShared)
		assert.Equal(t, uint32(1), n.accountLimits(owner).SharedSpacesLimit)
	})
}
