package coordinator

import (
	"testing"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
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
