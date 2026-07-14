package coordinator

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-sync/commonfile/fileproto"
	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotoerr"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"
	"storj.io/drpc"

	"github.com/anyproto/any-sync-coordinator/invitestore"
)

const maxInviteSize = 128 * 1024

// AclUploadInvite pushes an invite file to the filenodes and binds it to the invite it
// belongs to, so that revoking the invite is enough for the coordinator to delete the file.
// The binding carries only public material: the coordinator never learns the invite file
// key, which would otherwise let it decrypt the invite and join the space itself.
func (c *coordinator) AclUploadInvite(ctx context.Context, req *coordinatorproto.AclUploadInviteRequest) (err error) {
	if len(req.Data) > maxInviteSize {
		return fmt.Errorf("too big invite size")
	}
	inviteCid, err := cid.Cast(req.Cid)
	if err != nil {
		return err
	}
	if req.SpaceId == "" {
		// a client from before the binding: push the file as we always did. Nothing links it
		// to an invite, so it can never be collected; the counter tells us how many are left.
		c.unboundInviteUploads.Inc()
		return c.pushInviteFile(ctx, req.Cid, req.Data)
	}

	pubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return coordinatorproto.ErrForbidden
	}
	perms, err := c.acl.Permissions(ctx, pubKey, req.SpaceId)
	if err != nil {
		return err
	}
	if !perms.CanManageAccounts() {
		return coordinatorproto.ErrForbidden
	}

	// the cid has to actually address the data, otherwise the binding would name a file
	// that nobody can ever produce
	chkCid, err := inviteCid.Prefix().Sum(req.Data)
	if err != nil {
		return err
	}
	if !chkCid.Equals(inviteCid) {
		return fmt.Errorf("block data doesn't match the requested cid")
	}
	if _, err = crypto.UnmarshalEd25519PublicKeyProto(req.InvitePubKey); err != nil {
		return fmt.Errorf("invalid invite key: %w", err)
	}

	// bind before pushing, and drop the binding if the push fails: a binding whose file never
	// arrived is harmless (a later delete finds nothing on the filenode and still clears it),
	// whereas a pushed file with no binding could never be deleted through a revoke.
	err = c.inviteStore.Bind(ctx, invitestore.Entry{
		Cid:          inviteCid.String(),
		SpaceId:      req.SpaceId,
		InvitePubKey: req.InvitePubKey,
	})
	if err != nil {
		if errors.Is(err, invitestore.ErrCidBound) {
			return coordinatorproto.ErrForbidden
		}
		return err
	}
	if err = c.pushInviteFile(ctx, req.Cid, req.Data); err != nil {
		if delErr := c.inviteStore.Delete(ctx, inviteCid.String()); delErr != nil {
			log.WarnCtx(ctx, "unbind invite after failed push",
				zap.String("cid", inviteCid.String()), zap.Error(delErr))
		}
		return err
	}
	return nil
}

// deadInvites reads the acl state once and returns the bindings whose invite is no longer alive.
func (c *coordinator) deadInvites(ctx context.Context, spaceId string, entries []invitestore.Entry) (dead []invitestore.Entry, err error) {
	err = c.acl.ReadState(ctx, spaceId, func(s *list.AclState) error {
		for _, entry := range entries {
			inviteKey, keyErr := crypto.UnmarshalEd25519PublicKeyProto(entry.InvitePubKey)
			if keyErr != nil {
				// Bind validates the key, so this should be unreachable
				log.WarnCtx(ctx, "unparseable invite key in binding", zap.String("cid", entry.Cid), zap.Error(keyErr))
				continue
			}
			if !inviteAlive(s, inviteKey) {
				dead = append(dead, entry)
			}
		}
		return nil
	})
	return
}

// inviteAlive reports whether the invite key still grants access to the space.
//
// A member invite lives in the acl as an invite record; a guest key lives as an account
// holding permissions. Neither type ever appears as the other, so this union covers both
// without the client having to tell us which kind of invite it uploaded.
//
// A key that is alive by neither test is either revoked or was never recorded at all — and
// both mean the same thing for the file: nobody can use it any more.
func inviteAlive(s *list.AclState, inviteKey crypto.PubKey) bool {
	for _, invite := range s.Invites() {
		if invite.Key.Equals(inviteKey) {
			return true
		}
	}
	return !s.Permissions(inviteKey).NoPermissions()
}

func (c *coordinator) pushInviteFile(ctx context.Context, inviteCid, data []byte) (err error) {
	filePeer, err := c.pool.GetOneOf(ctx, c.nodeConf.FilePeers())
	if err != nil {
		return err
	}
	return filePeer.DoDrpc(ctx, func(conn drpc.Conn) error {
		_, err := fileproto.NewDRPCFileClient(conn).BlockPush(ctx, &fileproto.BlockPushRequest{
			Cid:  inviteCid,
			Data: data,
		})
		return err
	})
}

func (c *coordinator) deleteInviteFile(ctx context.Context, inviteCid []byte) (err error) {
	filePeer, err := c.pool.GetOneOf(ctx, c.nodeConf.FilePeers())
	if err != nil {
		return err
	}
	return deleteInviteBlock(ctx, filePeer, inviteCid)
}

func deleteInviteBlock(ctx context.Context, filePeer peer.Peer, inviteCid []byte) (err error) {
	return filePeer.DoDrpc(ctx, func(conn drpc.Conn) error {
		_, err := fileproto.NewDRPCFileClient(conn).BlockDeleteUnbound(ctx, &fileproto.BlockDeleteUnboundRequest{
			Cid: inviteCid,
		})
		if err != nil {
			if err = rpcerr.Unwrap(err); errors.Is(err, fileprotoerr.ErrCIDNotFound) {
				// already gone: the binding should still go away
				return nil
			}
			return err
		}
		return nil
	})
}
