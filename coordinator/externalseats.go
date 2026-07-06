package coordinator

import (
	"context"

	"github.com/anyproto/any-sync/commonspace/object/acl/aclrecordproto"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/util/crypto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-coordinator/spacestatus"
)

// External seats (nested spaces, docs/16 phase 5): a compartment may admit an
// identity that is NOT a member of the parent (org) space only within the
// legalOwner's externalSeatsLimit — one per-org pool of DISTINCT external
// identities across all of the org's children (grooming decision, Q9).
//
// The gate runs live off the acl states (no persistent counter to drift); the
// externalSeats collection is a best-effort registry that backs the
// ExternalCompartments discovery rpc for the externals themselves, who cannot
// read the parent's registrations.

const externalSeatsCollName = "externalSeats"

type externalSeatEntry struct {
	Id       string `bson:"_id"` // spaceId + "/" + identity
	Identity string `bson:"identity"`
	SpaceId  string `bson:"spaceId"`
	OrgOwner string `bson:"orgOwner"`
}

// admittedIdentities extracts the identities an acl record admits as members
// (direct adds, request accepts, invite joins). Content sniffing only —
// anything unparseable is handled by the full validation in acl.AddRecord.
func admittedIdentities(rec *consensusproto.RawRecord) (admitted []crypto.PubKey) {
	var aclRec consensusproto.Record
	if err := aclRec.UnmarshalVT(rec.Payload); err != nil {
		return nil
	}
	var aclData aclrecordproto.AclData
	if err := aclData.UnmarshalVT(aclRec.Data); err != nil {
		return nil
	}
	addKey := func(raw []byte) {
		if key, err := crypto.UnmarshalEd25519PublicKeyProto(raw); err == nil {
			admitted = append(admitted, key)
		}
	}
	for _, content := range aclData.GetAclContent() {
		switch {
		case content.GetAccountsAdd() != nil:
			for _, add := range content.GetAccountsAdd().GetAdditions() {
				addKey(add.Identity)
			}
		case content.GetRequestAccept() != nil:
			addKey(content.GetRequestAccept().Identity)
		case content.GetInviteJoin() != nil:
			addKey(content.GetInviteJoin().Identity)
		}
	}
	return admitted
}

// removedIdentities extracts the identities an acl record removes.
func removedIdentities(rec *consensusproto.RawRecord) (removed []crypto.PubKey) {
	var aclRec consensusproto.Record
	if err := aclRec.UnmarshalVT(rec.Payload); err != nil {
		return nil
	}
	var aclData aclrecordproto.AclData
	if err := aclData.UnmarshalVT(aclRec.Data); err != nil {
		return nil
	}
	addKey := func(raw []byte) {
		if key, err := crypto.UnmarshalEd25519PublicKeyProto(raw); err == nil {
			removed = append(removed, key)
		}
	}
	for _, content := range aclData.GetAclContent() {
		switch {
		case content.GetAccountRemove() != nil:
			for _, raw := range content.GetAccountRemove().GetIdentities() {
				addKey(raw)
			}
		case content.GetAccountRemoveNoRotate() != nil:
			for _, raw := range content.GetAccountRemoveNoRotate().GetIdentities() {
				addKey(raw)
			}
		}
	}
	return removed
}

// verifyExternalSeats gates external admissions into a child space against the
// legalOwner's per-org pool and returns the externals among the admitted set.
func (c *coordinator) verifyExternalSeats(ctx context.Context, statusEntry spacestatus.StatusEntry, admitted []crypto.PubKey) (externals []crypto.PubKey, err error) {
	// which of the admitted are NOT parent members
	err = c.acl.ReadState(ctx, statusEntry.ParentSpaceId, func(st *list.AclState) error {
		for _, identity := range admitted {
			if st.Permissions(identity).NoPermissions() {
				externals = append(externals, identity)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(externals) == 0 {
		return nil, nil
	}
	orgOwner, err := c.acl.OwnerPubKey(ctx, statusEntry.ParentSpaceId)
	if err != nil {
		return nil, err
	}
	limits, err := c.accountLimit.GetLimits(ctx, orgOwner.Account())
	if err != nil {
		return nil, err
	}
	// distinct externals across all of the org's children, live from the acls
	var (
		parentMembers = map[string]struct{}{}
		childIds      []string
	)
	err = c.acl.ReadState(ctx, statusEntry.ParentSpaceId, func(st *list.AclState) error {
		for _, acc := range st.CurrentAccounts() {
			if !acc.Permissions.NoPermissions() {
				parentMembers[acc.PubKey.Account()] = struct{}{}
			}
		}
		for _, reg := range st.ChildRegistrations() {
			if !reg.Revoked {
				childIds = append(childIds, reg.ChildSpaceId)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	seats := map[string]struct{}{}
	for _, childId := range childIds {
		err = c.acl.ReadState(ctx, childId, func(st *list.AclState) error {
			for _, acc := range st.CurrentAccounts() {
				if acc.Permissions.NoPermissions() || acc.Status != list.StatusActive {
					continue
				}
				account := acc.PubKey.Account()
				if _, isMember := parentMembers[account]; !isMember {
					seats[account] = struct{}{}
				}
			}
			return nil
		})
		if err != nil {
			// a child acl that can't be loaded must not break admissions elsewhere
			log.Debug("external-seat count: skipping child", zap.String("childId", childId), zap.Error(err))
			err = nil
		}
	}
	for _, identity := range externals {
		seats[identity.Account()] = struct{}{}
	}
	if uint32(len(seats)) > limits.ExternalSeatsLimit {
		return nil, coordinatorproto.ErrSpaceLimitReached
	}
	return externals, nil
}

// recordExternalSeats maintains the discovery registry, best-effort.
func (c *coordinator) recordExternalSeats(ctx context.Context, spaceId, orgOwner string, externals []crypto.PubKey) {
	if c.externalSeats == nil {
		return
	}
	for _, identity := range externals {
		account := identity.Account()
		_, err := c.externalSeats.UpdateOne(ctx,
			bson.M{"_id": spaceId + "/" + account},
			bson.M{"$set": externalSeatEntry{
				Id:       spaceId + "/" + account,
				Identity: account,
				SpaceId:  spaceId,
				OrgOwner: orgOwner,
			}},
			options.Update().SetUpsert(true),
		)
		if err != nil {
			log.Warn("external-seat registry upsert failed", zap.String("spaceId", spaceId), zap.Error(err))
		}
	}
}

// dropExternalSeats removes registry rows for identities removed from spaceId.
func (c *coordinator) dropExternalSeats(ctx context.Context, spaceId string, removed []crypto.PubKey) {
	if c.externalSeats == nil {
		return
	}
	for _, identity := range removed {
		if _, err := c.externalSeats.DeleteOne(ctx, bson.M{"_id": spaceId + "/" + identity.Account()}); err != nil {
			log.Debug("external-seat registry delete failed", zap.String("spaceId", spaceId), zap.Error(err))
		}
	}
}

// ExternalCompartments lists the child spaces the calling identity holds an
// external seat in. Rows whose space is gone are filtered out lazily.
func (c *coordinator) ExternalCompartments(ctx context.Context) (spaceIds []string, err error) {
	accountPubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return nil, err
	}
	if c.externalSeats == nil {
		return nil, coordinatorproto.ErrUnexpected
	}
	cur, err := c.externalSeats.Find(ctx, bson.M{"identity": accountPubKey.Account()})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var entry externalSeatEntry
		if err := cur.Decode(&entry); err != nil {
			continue
		}
		if entry.SpaceId == "" {
			continue
		}
		if st, err := c.spaceStatus.Status(ctx, entry.SpaceId); err != nil || st.Status != spacestatus.SpaceStatusCreated {
			continue
		}
		spaceIds = append(spaceIds, entry.SpaceId)
	}
	return spaceIds, nil
}
