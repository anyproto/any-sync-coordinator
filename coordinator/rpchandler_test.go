package coordinator

import (
	"testing"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/acleventlog"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
)

func TestRpcHandler_SpaceStatusCheckMany(t *testing.T) {
	_, pubKey, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	pubKeyData, err := pubKey.Marshall()
	require.NoError(t, err)
	ctx = peer.CtxWithIdentity(ctx, pubKeyData)

	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		aLimits := accountlimit.Limits{
			SpaceMembersRead:  4,
			SpaceMembersWrite: 3,
			SharedSpacesLimit: 2,
		}

		fx.accountLimit.EXPECT().GetLimits(ctx, pubKey.Account()).Return(aLimits, nil)
		fx.spaceStatus.EXPECT().Status(ctx, "space1").Return(spacestatus.StatusEntry{
			SpaceId:  "space1",
			Identity: pubKey.Account(),
		}, nil)
		fx.spaceStatus.EXPECT().Status(ctx, "space2").Return(spacestatus.StatusEntry{
			SpaceId:  "space2",
			Identity: "other account",
		}, nil)

		resp, err := fx.drpcHandler.SpaceStatusCheckMany(ctx, &coordinatorproto.SpaceStatusCheckManyRequest{
			SpaceIds: []string{"space1", "space2"},
		})
		require.NoError(t, err)

		require.Len(t, resp.Payloads, 2)
		require.NotNil(t, resp.AccountLimits)

		assert.Equal(t, aLimits.SharedSpacesLimit, resp.AccountLimits.SharedSpacesLimit)
		assert.Equal(t, resp.Payloads[0].GetLimits().ReadMembers, aLimits.SpaceMembersRead)
		assert.Equal(t, resp.Payloads[0].GetLimits().WriteMembers, aLimits.SpaceMembersWrite)
		assert.Nil(t, resp.Payloads[1].GetLimits())
	})
}

func TestRpcHandler_EventLog(t *testing.T) {
	_, pubKey, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	pubKeyData, err := pubKey.Marshall()
	require.NoError(t, err)
	ctx = peer.CtxWithIdentity(ctx, pubKeyData)
	ctx = peer.CtxWithPeerId(ctx, "peer.addr")

	t.Run("success - return all items", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.nodeConf.EXPECT().NodeTypes(gomock.Any()).Return([]nodeconf.NodeType{nodeconf.NodeTypeCoordinator}).AnyTimes()

		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		fx.aclEventLog.EXPECT().GetAfter(ctx, pubKey.Account(), "", uint32(0)).Return([]acleventlog.AclEventLogEntry{
			{
				Id:        &id1,
				Timestamp: 123,
				EntryType: acleventlog.EntryTypeSpaceReceipt,
			},
			{
				Id:          &id2,
				Timestamp:   124,
				EntryType:   acleventlog.EntryTypeSpaceAclAddRecord,
				AclChangeId: "acl1",
			},
		}, false, nil)

		out, err := fx.drpcHandler.AclEventLog(ctx, &coordinatorproto.AclEventLogRequest{
			AccountIdentity: pubKey.Account(),
		})
		require.NoError(t, err)

		require.Equal(t, 2, len(out.Records))
	})

	t.Run("success - return 1 item", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.nodeConf.EXPECT().NodeTypes(gomock.Any()).Return([]nodeconf.NodeType{nodeconf.NodeTypeCoordinator}).AnyTimes()

		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		fx.aclEventLog.EXPECT().GetAfter(ctx, pubKey.Account(), id1.Hex(), uint32(0)).Return([]acleventlog.AclEventLogEntry{
			{
				Id:          &id2,
				Timestamp:   124,
				EntryType:   acleventlog.EntryTypeSpaceAclAddRecord,
				AclChangeId: "acl1",
			},
		}, false, nil)

		out, err := fx.drpcHandler.AclEventLog(ctx, &coordinatorproto.AclEventLogRequest{
			AccountIdentity: pubKey.Account(),
			AfterId:         id1.Hex(),
		})
		require.NoError(t, err)

		require.Equal(t, 1, len(out.Records))
	})

}
