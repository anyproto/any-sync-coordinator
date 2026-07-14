package coordinator

import (
	"context"
	"errors"
	"testing"

	"github.com/anyproto/any-sync/commonfile/fileproto"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/aclrecordproto"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/list/listtest"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-coordinator/invitestore"
)

type testFileServer struct {
	fileproto.DRPCFileUnimplementedServer
	blocks  map[string][]byte
	deleted []string
}

func (s *testFileServer) BlockPush(ctx context.Context, req *fileproto.BlockPushRequest) (*fileproto.Ok, error) {
	c, err := cid.Cast(req.Cid)
	if err != nil {
		return nil, err
	}
	if s.blocks == nil {
		s.blocks = map[string][]byte{}
	}
	s.blocks[c.String()] = req.Data
	return &fileproto.Ok{}, nil
}

func (s *testFileServer) BlockDeleteUnbound(ctx context.Context, req *fileproto.BlockDeleteUnboundRequest) (*fileproto.Ok, error) {
	c, err := cid.Cast(req.Cid)
	if err != nil {
		return nil, err
	}
	s.deleted = append(s.deleted, c.String())
	return &fileproto.Ok{}, nil
}

func (fx *fixture) expectFilePeer(t *testing.T, fs *testFileServer) {
	ts := rpctest.NewTestServer()
	require.NoError(t, fileproto.DRPCRegisterFile(ts.Mux, fs))
	filePeer, err := ts.Dial("file.peer")
	require.NoError(t, err)
	fx.nodeConf.EXPECT().FilePeers().Return([]string{"file.peer"})
	fx.pool.EXPECT().GetOneOf(gomock.Any(), []string{"file.peer"}).Return(filePeer, nil)
}

// blockOf builds a raw block and its cid, standing in for an invite file. The binding never
// looks inside the file, so the contents do not have to be a real invite here.
func blockOf(t *testing.T, data []byte) cid.Cid {
	prefix := cid.Prefix{Version: 1, Codec: cid.DagProtobuf, MhType: 0x12, MhLength: -1}
	c, err := prefix.Sum(data)
	require.NoError(t, err)
	return c
}

func TestCoordinator_AclUploadInvite(t *testing.T) {
	var spaceId = "space.id"

	ownerKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	ownerPubData, err := ownerKeys.SignKey.GetPublic().Marshall()
	require.NoError(t, err)
	ownerCtx := peer.CtxWithPeerId(peer.CtxWithIdentity(context.Background(), ownerPubData), "peer.addr")

	_, invitePub, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	inviteKeyRaw, err := invitePub.Marshall()
	require.NoError(t, err)

	data := []byte("invite file")
	inviteCid := blockOf(t, data)

	expectCanManage := func(fx *fixture) {
		fx.acl.EXPECT().Permissions(gomock.Any(), gomock.Any(), spaceId).
			Return(list.AclPermissions(aclrecordproto.AclUserPermissions_Owner), nil)
	}

	t.Run("bound upload binds and pushes", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		expectCanManage(fx)
		fs := &testFileServer{blocks: map[string][]byte{}}
		fx.expectFilePeer(t, fs)
		fx.inviteStore.EXPECT().Bind(gomock.Any(), invitestore.Entry{
			Cid:          inviteCid.String(),
			SpaceId:      spaceId,
			InvitePubKey: inviteKeyRaw,
		}).Return(nil)

		require.NoError(t, fx.AclUploadInvite(ownerCtx, &coordinatorproto.AclUploadInviteRequest{
			Cid:          inviteCid.Bytes(),
			Data:         data,
			SpaceId:      spaceId,
			InvitePubKey: inviteKeyRaw,
		}))
		assert.Equal(t, data, fs.blocks[inviteCid.String()])
	})

	t.Run("old client uploads unbound", func(t *testing.T) {
		// no spaceId: nothing to bind, no acl lookup, no auth. The file is pushed as before
		// and can never be collected.
		fx := newFixture(t)
		defer fx.finish(t)
		fs := &testFileServer{blocks: map[string][]byte{}}
		fx.expectFilePeer(t, fs)

		require.NoError(t, fx.AclUploadInvite(ownerCtx, &coordinatorproto.AclUploadInviteRequest{
			Cid:  inviteCid.Bytes(),
			Data: data,
		}))
		assert.Equal(t, data, fs.blocks[inviteCid.String()])
	})

	t.Run("cannot manage accounts", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.acl.EXPECT().Permissions(gomock.Any(), gomock.Any(), spaceId).
			Return(list.AclPermissions(aclrecordproto.AclUserPermissions_Reader), nil)
		err := fx.AclUploadInvite(ownerCtx, &coordinatorproto.AclUploadInviteRequest{
			Cid:          inviteCid.Bytes(),
			Data:         data,
			SpaceId:      spaceId,
			InvitePubKey: inviteKeyRaw,
		})
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})

	t.Run("cid not addressing the data", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		expectCanManage(fx)
		err := fx.AclUploadInvite(ownerCtx, &coordinatorproto.AclUploadInviteRequest{
			Cid:          blockOf(t, []byte("something else")).Bytes(),
			Data:         data,
			SpaceId:      spaceId,
			InvitePubKey: inviteKeyRaw,
		})
		require.Error(t, err)
	})

	t.Run("invite key must be a public key", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		expectCanManage(fx)
		err := fx.AclUploadInvite(ownerCtx, &coordinatorproto.AclUploadInviteRequest{
			Cid:          inviteCid.Bytes(),
			Data:         data,
			SpaceId:      spaceId,
			InvitePubKey: []byte("not a key"),
		})
		require.Error(t, err)
	})

	t.Run("cid already bound elsewhere", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		expectCanManage(fx)
		fx.inviteStore.EXPECT().Bind(gomock.Any(), gomock.Any()).Return(invitestore.ErrCidBound)
		err := fx.AclUploadInvite(ownerCtx, &coordinatorproto.AclUploadInviteRequest{
			Cid:          inviteCid.Bytes(),
			Data:         data,
			SpaceId:      spaceId,
			InvitePubKey: inviteKeyRaw,
		})
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})

	t.Run("failed push unbinds", func(t *testing.T) {
		// a binding whose file never made it would otherwise keep a cid reserved forever
		fx := newFixture(t)
		defer fx.finish(t)
		expectCanManage(fx)
		fx.inviteStore.EXPECT().Bind(gomock.Any(), gomock.Any()).Return(nil)
		fx.nodeConf.EXPECT().FilePeers().Return([]string{"file.peer"})
		fx.pool.EXPECT().GetOneOf(gomock.Any(), []string{"file.peer"}).Return(nil, errors.New("no file peer"))
		fx.inviteStore.EXPECT().Delete(gomock.Any(), inviteCid.String()).Return(nil)

		err := fx.AclUploadInvite(ownerCtx, &coordinatorproto.AclUploadInviteRequest{
			Cid:          inviteCid.Bytes(),
			Data:         data,
			SpaceId:      spaceId,
			InvitePubKey: inviteKeyRaw,
		})
		require.Error(t, err)
	})
}

func TestCoordinator_AclDeleteInvite_Bound(t *testing.T) {
	var spaceId = "space.id"

	ownerKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	ownerPubData, err := ownerKeys.SignKey.GetPublic().Marshall()
	require.NoError(t, err)
	ownerCtx := peer.CtxWithPeerId(peer.CtxWithIdentity(context.Background(), ownerPubData), "peer.addr")

	aclList, err := list.NewInMemoryDerivedAcl(spaceId, ownerKeys)
	require.NoError(t, err)
	inv, err := aclList.RecordBuilder().BuildInvite()
	require.NoError(t, err)
	require.NoError(t, aclList.AddRawRecord(listtest.WrapAclRecord(inv.InviteRec)))

	activeKeyRaw, err := inv.InviteKey.GetPublic().Marshall()
	require.NoError(t, err)

	data := []byte("invite file")
	inviteCid := blockOf(t, data)

	expectCanManage := func(fx *fixture) {
		fx.acl.EXPECT().Permissions(gomock.Any(), gomock.Any(), spaceId).
			Return(list.AclPermissions(aclrecordproto.AclUserPermissions_Owner), nil)
	}
	expectReadState := func(fx *fixture) {
		fx.acl.EXPECT().ReadState(gomock.Any(), spaceId, gomock.Any()).
			DoAndReturn(func(ctx context.Context, spaceId string, f func(s *list.AclState) error) error {
				return f(aclList.AclState())
			})
	}

	t.Run("revoked invite deleted without a file key", func(t *testing.T) {
		// this is the whole point: the invite key is not in the acl any more, so the
		// coordinator deletes the file knowing nothing about its contents
		_, revokedPub, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		revokedKeyRaw, err := revokedPub.Marshall()
		require.NoError(t, err)

		fx := newFixture(t)
		defer fx.finish(t)
		expectCanManage(fx)
		fx.inviteStore.EXPECT().Get(gomock.Any(), inviteCid.String()).Return(invitestore.Entry{
			Cid:          inviteCid.String(),
			SpaceId:      spaceId,
			InvitePubKey: revokedKeyRaw,
		}, nil)
		expectReadState(fx)
		fs := &testFileServer{blocks: map[string][]byte{inviteCid.String(): data}}
		fx.expectFilePeer(t, fs)
		fx.inviteStore.EXPECT().Delete(gomock.Any(), inviteCid.String()).Return(nil)

		require.NoError(t, fx.AclDeleteInvite(ownerCtx, &coordinatorproto.AclDeleteInviteRequest{
			SpaceId:   spaceId,
			InviteCid: inviteCid.Bytes(),
		}))
		assert.Equal(t, []string{inviteCid.String()}, fs.deleted)
	})

	t.Run("active invite refused", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		expectCanManage(fx)
		fx.inviteStore.EXPECT().Get(gomock.Any(), inviteCid.String()).Return(invitestore.Entry{
			Cid:          inviteCid.String(),
			SpaceId:      spaceId,
			InvitePubKey: activeKeyRaw,
		}, nil)
		expectReadState(fx)

		err := fx.AclDeleteInvite(ownerCtx, &coordinatorproto.AclDeleteInviteRequest{
			SpaceId:   spaceId,
			InviteCid: inviteCid.Bytes(),
		})
		require.ErrorIs(t, err, coordinatorproto.ErrInviteStillActive)
	})

	t.Run("binding of another space refused", func(t *testing.T) {
		// a cid bound to another space cannot be deleted through this one
		fx := newFixture(t)
		defer fx.finish(t)
		expectCanManage(fx)
		fx.inviteStore.EXPECT().Get(gomock.Any(), inviteCid.String()).Return(invitestore.Entry{
			Cid:          inviteCid.String(),
			SpaceId:      "other.space",
			InvitePubKey: activeKeyRaw,
		}, nil)

		err := fx.AclDeleteInvite(ownerCtx, &coordinatorproto.AclDeleteInviteRequest{
			SpaceId:   spaceId,
			InviteCid: inviteCid.Bytes(),
		})
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})

	t.Run("cannot manage accounts", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.acl.EXPECT().Permissions(gomock.Any(), gomock.Any(), spaceId).
			Return(list.AclPermissions(aclrecordproto.AclUserPermissions_Reader), nil)
		err := fx.AclDeleteInvite(ownerCtx, &coordinatorproto.AclDeleteInviteRequest{
			SpaceId:   spaceId,
			InviteCid: inviteCid.Bytes(),
		})
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})

	t.Run("no pub key", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		err := fx.AclDeleteInvite(context.Background(), &coordinatorproto.AclDeleteInviteRequest{SpaceId: spaceId})
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})

	t.Run("unbound invite deleted", func(t *testing.T) {
		// not in the binding collection: an invite from before the binding existed, so there is
		// nothing to check the file against
		fx := newFixture(t)
		defer fx.finish(t)
		expectCanManage(fx)
		fx.inviteStore.EXPECT().Get(gomock.Any(), inviteCid.String()).
			Return(invitestore.Entry{}, invitestore.ErrNotFound)
		fs := &testFileServer{blocks: map[string][]byte{inviteCid.String(): data}}
		fx.expectFilePeer(t, fs)

		require.NoError(t, fx.AclDeleteInvite(ownerCtx, &coordinatorproto.AclDeleteInviteRequest{
			SpaceId:   spaceId,
			InviteCid: inviteCid.Bytes(),
		}))
		assert.Equal(t, []string{inviteCid.String()}, fs.deleted)
	})
}

func TestInviteAlive(t *testing.T) {
	var spaceId = "space.id"

	ownerKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	aclList, err := list.NewInMemoryDerivedAcl(spaceId, ownerKeys)
	require.NoError(t, err)
	inv, err := aclList.RecordBuilder().BuildInvite()
	require.NoError(t, err)
	require.NoError(t, aclList.AddRawRecord(listtest.WrapAclRecord(inv.InviteRec)))
	state := aclList.AclState()

	t.Run("active member invite", func(t *testing.T) {
		assert.True(t, inviteAlive(state, inv.InviteKey.GetPublic()))
	})

	t.Run("guest key holding permissions", func(t *testing.T) {
		// a guest never appears as an invite record, only as an account with permissions;
		// the owner stands in for one here
		assert.True(t, inviteAlive(state, ownerKeys.SignKey.GetPublic()))
	})

	t.Run("revoked or unknown key", func(t *testing.T) {
		_, unknown, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		assert.False(t, inviteAlive(state, unknown))
	})
}
