package coordinator

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/list/listtest"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/consensus/consensusclient"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
)

// testConsensus keeps acl logs in memory in place of the consensus node. A record it accepts reaches
// the watcher of its log only with deliver: the real node sends a record back some time after the add.
type testConsensus struct {
	mu       sync.Mutex
	logs     map[string][]*consensusproto.RawRecordWithId
	pending  map[string][]*consensusproto.RawRecordWithId
	watchers map[string]consensusclient.Watcher
}

func newTestConsensus() *testConsensus {
	return &testConsensus{
		logs:     map[string][]*consensusproto.RawRecordWithId{},
		pending:  map[string][]*consensusproto.RawRecordWithId{},
		watchers: map[string]consensusclient.Watcher{},
	}
}

func (c *testConsensus) Init(*app.App) error       { return nil }
func (c *testConsensus) Name() string              { return consensusclient.CName }
func (c *testConsensus) Run(context.Context) error { return nil }
func (c *testConsensus) Close(context.Context) error {
	return nil
}

func (c *testConsensus) AddLog(_ context.Context, logId string, rec *consensusproto.RawRecordWithId) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.logs[logId]; ok {
		return consensuserr.ErrLogExists
	}
	c.logs[logId] = []*consensusproto.RawRecordWithId{rec}
	return nil
}

func (c *testConsensus) DeleteLog(_ context.Context, logId string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.logs, logId)
	return nil
}

func (c *testConsensus) AddRecord(_ context.Context, logId string, rec *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	log, ok := c.logs[logId]
	if !ok {
		return nil, consensuserr.ErrLogNotFound
	}
	record := &consensusproto.Record{}
	if err := record.UnmarshalVT(rec.Payload); err != nil {
		return nil, consensuserr.ErrInvalidPayload
	}
	if record.PrevId != log[len(log)-1].Id {
		return nil, consensuserr.ErrConflict
	}
	added := listtest.WrapAclRecord(rec)
	c.logs[logId] = append(log, added)
	if _, watched := c.watchers[logId]; watched {
		c.pending[logId] = append(c.pending[logId], added)
	}
	return added, nil
}

// Watch answers with the whole log, newest record first, as the consensus node does
func (c *testConsensus) Watch(logId string, w consensusclient.Watcher) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	log, ok := c.logs[logId]
	if !ok {
		go w.AddConsensusError(consensuserr.ErrLogNotFound)
		return nil
	}
	c.watchers[logId] = w
	recs := slices.Clone(log)
	slices.Reverse(recs)
	go w.AddConsensusRecords(recs)
	return nil
}

func (c *testConsensus) UnWatch(logId string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.watchers, logId)
	delete(c.pending, logId)
	return nil
}

// deliver sends the watchers the records accepted since the last delivery
func (c *testConsensus) deliver() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for logId, recs := range c.pending {
		slices.Reverse(recs)
		c.watchers[logId].AddConsensusRecords(recs)
	}
	c.pending = map[string][]*consensusproto.RawRecordWithId{}
}

func (c *testConsensus) log(logId string) []*consensusproto.RawRecordWithId {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.logs[logId])
}

func newAccount(t *testing.T) *accountdata.AccountKeys {
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)
	return keys
}

// newAcl puts the acl of a new space of the owner on the network
func (fx *fixture) newAcl(t *testing.T, spaceId string, owner *accountdata.AccountKeys) {
	acl, err := list.NewInMemoryDerivedAcl(spaceId, owner)
	require.NoError(t, err)
	require.NoError(t, fx.consensus.AddLog(context.Background(), spaceId, acl.Root()))
}

// aclOf builds the acl of the space from the records on the network, for the account to read its
// state and to build its next record
func (fx *fixture) aclOf(t *testing.T, spaceId string, account *accountdata.AccountKeys) list.AclList {
	storage, err := list.NewInMemoryStorage(spaceId, fx.consensus.log(spaceId))
	require.NoError(t, err)
	acl, err := list.BuildAclListWithIdentity(account, storage, recordverifier.NewValidateFull())
	require.NoError(t, err)
	return acl
}

// put adds a record to the acl past the coordinator, to set a space up
func (fx *fixture) put(t *testing.T, spaceId string, rec *consensusproto.RawRecord, err error) *consensusproto.RawRecordWithId {
	require.NoError(t, err)
	added, err := fx.consensus.AddRecord(context.Background(), spaceId, rec)
	require.NoError(t, err)
	fx.consensus.deliver()
	return added
}

// putMembers adds accounts to the acl with the given permissions, as its owner does
func (fx *fixture) putMembers(t *testing.T, spaceId string, owner *accountdata.AccountKeys, permissions list.AclPermissions, accounts ...*accountdata.AccountKeys) {
	additions := make([]list.AccountAdd, 0, len(accounts))
	for _, account := range accounts {
		additions = append(additions, list.AccountAdd{
			Identity:    account.SignKey.GetPublic(),
			Permissions: permissions,
			Metadata:    []byte("metadata"),
		})
	}
	rec, err := fx.aclOf(t, spaceId, owner).RecordBuilder().BuildAccountsAdd(list.AccountsAddPayload{Additions: additions})
	fx.put(t, spaceId, rec, err)
}

// addRecord sends the record to the coordinator as the account, then lets the network deliver it
func (fx *fixture) addRecord(t *testing.T, spaceId string, author *accountdata.AccountKeys, rec *consensusproto.RawRecord, buildErr error) (*consensusproto.RawRecordWithId, error) {
	require.NoError(t, buildErr)
	payload, err := rec.MarshalVT()
	require.NoError(t, err)
	added, err := fx.AclAddRecord(accountCtx(t, author.SignKey.GetPublic()), spaceId, payload)
	fx.consensus.deliver()
	return added, err
}

// expectSpace serves the document of a space and its member limits to every acl record added to it
func (fx *fixture) expectSpace(entry spacestatus.StatusEntry) {
	fx.spaceStatus.EXPECT().Status(gomock.Any(), entry.SpaceId).Return(entry, nil).AnyTimes()
	fx.accountLimit.EXPECT().GetLimitsBySpace(gomock.Any(), entry.SpaceId).Return(accountlimit.SpaceLimits{
		SpaceMembersRead:  100,
		SpaceMembersWrite: 100,
	}, nil).AnyTimes()
}

func permissionsOf(acl list.AclList, account *accountdata.AccountKeys) list.AclPermissions {
	return acl.AclState().Permissions(account.SignKey.GetPublic())
}

func pubKeys(accounts ...*accountdata.AccountKeys) []crypto.PubKey {
	keys := make([]crypto.PubKey, 0, len(accounts))
	for _, account := range accounts {
		keys = append(keys, account.SignKey.GetPublic())
	}
	return keys
}
