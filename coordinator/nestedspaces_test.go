package coordinator

import (
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/aclrecordproto"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/list/listtest"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
)

func buildNestedSetup(t *testing.T) (creatorKeys *accountdata.AccountKeys, parentAcl list.AclList, childId string, childHeader []byte, childAclRootId string) {
	parentEx := list.NewAclExecutor("parent.id")
	require.NoError(t, parentEx.Execute("a.init::a"))
	parentAcl = parentEx.ActualAccounts()["a"].Acl

	creatorKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	master, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	metaKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	readKey, _ := crypto.NewRandomAES()
	out, err := spacepayloads.StoragePayloadForSpaceCreateV1(spacepayloads.SpaceCreatePayload{
		SigningKey:     creatorKeys.SignKey,
		SpaceType:      "anytype.space",
		ReplicationKey: 42,
		MasterKey:      master,
		ReadKey:        readKey,
		MetadataKey:    metaKey,
		Metadata:        []byte("md"),
		ParentSpaceId:   "parent.id",
		LegalOwner:      parentAcl.AclState().Identity(),
		ParentAclRootId: parentAcl.Id(),
	})
	require.NoError(t, err)
	return creatorKeys, parentAcl, out.SpaceHeaderWithId.Id, out.SpaceHeaderWithId.RawHeader, out.AclWithId.Id
}

func registerChild(t *testing.T, parentAcl list.AclList, childId, childAclRootId string) (parentRecId string) {
	reg, err := parentAcl.RecordBuilder().BuildChildRegister(list.ChildRegisterPayload{
		ChildSpaceId:   childId,
		ChildAclRootId: childAclRootId,
	})
	require.NoError(t, err)
	require.NoError(t, parentAcl.AddRawRecord(listtest.WrapAclRecord(reg)))
	return parentAcl.Head().Id
}

func TestCoordinator_SpaceSignNested(t *testing.T) {
	creatorKeys, parentAcl, childId, childHeader, childAclRootId := buildNestedSetup(t)
	pubKeyData, err := creatorKeys.SignKey.GetPublic().Marshall()
	require.NoError(t, err)
	signCtx := peer.CtxWithPeerId(peer.CtxWithIdentity(ctx, pubKeyData), "peer.id")
	parentOwner := parentAcl.AclState().Identity()

	expectParentReads := func(fx *fixture, entry spacestatus.StatusEntry) {
		fx.spaceStatus.EXPECT().Status(gomock.Any(), "parent.id").Return(entry, nil)
	}
	activeParent := spacestatus.StatusEntry{
		SpaceId:  "parent.id",
		Identity: parentOwner.Account(),
		Status:   spacestatus.SpaceStatusCreated,
		Type:     spacestatus.SpaceTypeRegular,
	}
	expectReadState := func(fx *fixture) {
		fx.acl.EXPECT().ReadState(gomock.Any(), "parent.id", gomock.Any()).DoAndReturn(
			func(_ interface{ Done() <-chan struct{} }, _ string, f func(s *list.AclState) error) error {
				return f(parentAcl.AclState())
			})
	}
	// the nested branch checks the child's own status first (new vs re-sign)
	expectChildNew := func(fx *fixture) {
		fx.spaceStatus.EXPECT().Status(gomock.Any(), childId).Return(spacestatus.StatusEntry{}, coordinatorproto.ErrSpaceNotExists)
	}

	t.Run("success", func(t *testing.T) {
		parentRecId := registerChild(t, parentAcl, childId, childAclRootId)
		fx := newFixture(t)
		defer fx.finish(t)
		fx.account = networkKeys(t)
		expectChildNew(fx)
		expectParentReads(fx, activeParent)
		expectReadState(fx)
		fx.accountLimit.EXPECT().GetLimits(gomock.Any(), parentOwner.Account()).Return(accountlimit.Limits{SharedSpacesLimit: 5}, nil)
		fx.spaceStatus.EXPECT().NewChildStatus(gomock.Any(), childId, gomock.Any(), "parent.id", parentOwner.Account(), uint32(5), spacestatus.SpaceTypeRegular, false).Return(nil)
		fx.coordLog.EXPECT().SpaceReceipt(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		fx.aclEventLog.EXPECT().AddLog(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		receipt, err := fx.SpaceSign(signCtx, childId, childHeader, false, parentRecId)
		require.NoError(t, err)
		require.NotNil(t, receipt)
	})

	t.Run("missing parentAclRecordId", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.account = networkKeys(t)
		expectChildNew(fx)
		_, err := fx.SpaceSign(signCtx, childId, childHeader, false, "")
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})

	t.Run("parent deleted", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.account = networkKeys(t)
		expectChildNew(fx)
		expectParentReads(fx, spacestatus.StatusEntry{
			SpaceId: "parent.id",
			Status:  spacestatus.SpaceStatusDeleted,
			Type:    spacestatus.SpaceTypeRegular,
		})
		_, err := fx.SpaceSign(signCtx, childId, childHeader, false, "some.rec")
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceIsDeleted)
	})

	t.Run("parent is itself a child", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.account = networkKeys(t)
		expectChildNew(fx)
		nested := activeParent
		nested.BilledIdentity = "some.org"
		expectParentReads(fx, nested)
		_, err := fx.SpaceSign(signCtx, childId, childHeader, false, "some.rec")
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})

	t.Run("wrong registration record id", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.account = networkKeys(t)
		expectChildNew(fx)
		expectParentReads(fx, activeParent)
		expectReadState(fx)
		_, err := fx.SpaceSign(signCtx, childId, childHeader, false, "not.the.registration")
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})

	t.Run("top-level sign with parentAclRecordId is rejected", func(t *testing.T) {
		topKeys, err := accountdata.NewRandom()
		require.NoError(t, err)
		master, _, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		metaKey, _, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		readKey, _ := crypto.NewRandomAES()
		out, err := spacepayloads.StoragePayloadForSpaceCreateV1(spacepayloads.SpaceCreatePayload{
			SigningKey:     topKeys.SignKey,
			SpaceType:      "anytype.space",
			ReplicationKey: 43,
			MasterKey:      master,
			ReadKey:        readKey,
			MetadataKey:    metaKey,
			Metadata:       []byte("md"),
		})
		require.NoError(t, err)
		topKeyData, err := topKeys.SignKey.GetPublic().Marshall()
		require.NoError(t, err)
		topCtx := peer.CtxWithPeerId(peer.CtxWithIdentity(ctx, topKeyData), "peer.id")

		fx := newFixture(t)
		defer fx.finish(t)
		fx.account = networkKeys(t)
		_, err = fx.SpaceSign(topCtx, out.SpaceHeaderWithId.Id, out.SpaceHeaderWithId.RawHeader, false, "some.rec")
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})

	t.Run("children disallowed by parent options", func(t *testing.T) {
		// separate parent with the toggle set
		parentEx := list.NewAclExecutor("parent.id")
		require.NoError(t, parentEx.Execute("a.init::a"))
		lockedParent := parentEx.ActualAccounts()["a"].Acl
		optsChange, err := lockedParent.RecordBuilder().BuildSpaceOptionsChange(&aclrecordproto.AclSpaceOptions{
			ChildrenCreationDisallowed: true,
		})
		require.NoError(t, err)
		require.NoError(t, lockedParent.AddRawRecord(listtest.WrapAclRecord(optsChange)))

		lockedCreator, err := accountdata.NewRandom()
		require.NoError(t, err)
		master, _, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		metaKey, _, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		readKey, _ := crypto.NewRandomAES()
		out, err := spacepayloads.StoragePayloadForSpaceCreateV1(spacepayloads.SpaceCreatePayload{
			SigningKey:     lockedCreator.SignKey,
			SpaceType:      "anytype.space",
			ReplicationKey: 44,
			MasterKey:      master,
			ReadKey:        readKey,
			MetadataKey:    metaKey,
			Metadata:        []byte("md"),
			ParentSpaceId:   "parent.id",
			LegalOwner:      lockedParent.AclState().Identity(),
			ParentAclRootId: lockedParent.Id(),
		})
		require.NoError(t, err)
		lockedKeyData, err := lockedCreator.SignKey.GetPublic().Marshall()
		require.NoError(t, err)
		lockedCtx := peer.CtxWithPeerId(peer.CtxWithIdentity(ctx, lockedKeyData), "peer.id")

		fx := newFixture(t)
		defer fx.finish(t)
		fx.account = networkKeys(t)
		fx.spaceStatus.EXPECT().Status(gomock.Any(), out.SpaceHeaderWithId.Id).Return(spacestatus.StatusEntry{}, coordinatorproto.ErrSpaceNotExists)
		fx.spaceStatus.EXPECT().Status(gomock.Any(), "parent.id").Return(activeParent, nil)
		fx.acl.EXPECT().ReadState(gomock.Any(), "parent.id", gomock.Any()).DoAndReturn(
			func(_ interface{ Done() <-chan struct{} }, _ string, f func(s *list.AclState) error) error {
				return f(lockedParent.AclState())
			})
		_, err = fx.SpaceSign(lockedCtx, out.SpaceHeaderWithId.Id, out.SpaceHeaderWithId.RawHeader, false, "some.rec")
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})
}

func TestCoordinator_SpaceSignNested_ResignAfterOwnershipTransfer(t *testing.T) {
	// build a parent whose ownership will move a -> b, and a child pinned to a (the genesis owner)
	parentEx := list.NewAclExecutor("parent.id")
	for _, cmd := range []string{
		"a.init::a",
		"a.invite::inv",
		"b.join::inv",
		"a.approve::b,adm",
	} {
		require.NoError(t, parentEx.Execute(cmd))
	}
	parentAcl := parentEx.ActualAccounts()["a"].Acl
	genesisOwner := parentAcl.AclState().Identity() // account a

	creatorKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	master, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	metaKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	readKey, _ := crypto.NewRandomAES()
	out, err := spacepayloads.StoragePayloadForSpaceCreateV1(spacepayloads.SpaceCreatePayload{
		SigningKey:      creatorKeys.SignKey,
		SpaceType:       "anytype.space",
		ReplicationKey:  77,
		MasterKey:       master,
		ReadKey:         readKey,
		MetadataKey:     metaKey,
		Metadata:        []byte("md"),
		ParentSpaceId:   "parent.id",
		LegalOwner:      genesisOwner, // pinned to a
		ParentAclRootId: parentAcl.Id(),
	})
	require.NoError(t, err)
	childId := out.SpaceHeaderWithId.Id

	// transfer parent ownership a -> b FIRST (via the executor, which tracks the head),
	// then register the child directly as the last acl write
	require.NoError(t, parentEx.Execute("a.ownership_change::b,adm"))
	currentOwner, err := parentAcl.AclState().OwnerPubKey()
	require.NoError(t, err)
	require.False(t, currentOwner.Equals(genesisOwner), "ownership must have moved")
	registerChild(t, parentAcl, childId, out.AclWithId.Id)

	pubKeyData, err := creatorKeys.SignKey.GetPublic().Marshall()
	require.NoError(t, err)
	signCtx := peer.CtxWithPeerId(peer.CtxWithIdentity(ctx, pubKeyData), "peer.id")

	fx := newFixture(t)
	defer fx.finish(t)
	fx.account = networkKeys(t)
	// child already exists → re-sign path (isNew = false)
	fx.spaceStatus.EXPECT().Status(gomock.Any(), childId).Return(spacestatus.StatusEntry{
		SpaceId:       childId,
		Status:        spacestatus.SpaceStatusCreated,
		Type:          spacestatus.SpaceTypeRegular,
		ParentSpaceId: "parent.id",
	}, nil)
	fx.spaceStatus.EXPECT().Status(gomock.Any(), "parent.id").Return(spacestatus.StatusEntry{
		SpaceId:  "parent.id",
		Identity: currentOwner.Account(),
		Status:   spacestatus.SpaceStatusCreated,
		Type:     spacestatus.SpaceTypeRegular,
	}, nil)
	fx.acl.EXPECT().ReadState(gomock.Any(), "parent.id", gomock.Any()).DoAndReturn(
		func(_ interface{ Done() <-chan struct{} }, _ string, f func(s *list.AclState) error) error {
			return f(parentAcl.AclState())
		})
	// billing must target the CURRENT owner, not the pinned genesis owner
	fx.accountLimit.EXPECT().GetLimits(gomock.Any(), currentOwner.Account()).Return(accountlimit.Limits{SharedSpacesLimit: 5}, nil)
	fx.spaceStatus.EXPECT().NewChildStatus(gomock.Any(), childId, gomock.Any(), "parent.id", currentOwner.Account(), uint32(5), spacestatus.SpaceTypeRegular, false).Return(nil)
	fx.coordLog.EXPECT().SpaceReceipt(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	fx.aclEventLog.EXPECT().AddLog(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// re-sign: header pins the OLD owner but the space is already Created, so isNew=false
	// skips the pinned==current check and succeeds, billing the new owner
	receipt, err := fx.SpaceSign(signCtx, childId, out.SpaceHeaderWithId.RawHeader, false, parentAcl.Head().Id)
	require.NoError(t, err)
	require.NotNil(t, receipt)
}

// networkKeys works around the fixture's registration order: the coordinator's Init reads the
// account before the test account service generated it, so SpaceSign needs the key set directly.
func networkKeys(t *testing.T) *accountdata.AccountKeys {
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)
	return keys
}

func TestCoordinator_SpaceDeleteAsLegalOwner(t *testing.T) {
	legalOwnerKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	strangerKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	legalOwnerKeyData, err := legalOwnerKeys.SignKey.GetPublic().Marshall()
	require.NoError(t, err)
	strangerKeyData, err := strangerKeys.SignKey.GetPublic().Marshall()
	require.NoError(t, err)
	legalOwnerCtx := peer.CtxWithPeerId(peer.CtxWithIdentity(ctx, legalOwnerKeyData), "peer.id")
	strangerCtx := peer.CtxWithPeerId(peer.CtxWithIdentity(ctx, strangerKeyData), "peer.id")

	childEntry := spacestatus.StatusEntry{
		SpaceId:       "child.id",
		Identity:      "creator.identity",
		Status:        spacestatus.SpaceStatusCreated,
		Type:          spacestatus.SpaceTypeRegular,
		ParentSpaceId: "parent.id",
	}

	t.Run("legal owner deletes a child it does not own", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.account = networkKeys(t)
		fx.spaceStatus.EXPECT().Status(gomock.Any(), "child.id").Return(childEntry, nil)
		fx.acl.EXPECT().OwnerPubKey(gomock.Any(), "parent.id").Return(legalOwnerKeys.SignKey.GetPublic(), nil)
		fx.nodeConf.EXPECT().Configuration().Return(nodeconf.Configuration{NetworkId: "net"})
		fx.spaceStatus.EXPECT().SpaceDelete(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ interface{ Done() <-chan struct{} }, payload spacestatus.SpaceDeletion) (int64, error) {
				require.True(t, payload.AuthorizedByLegalOwner)
				require.Equal(t, "child.id", payload.SpaceId)
				return 42, nil
			})
		toBeDeleted, err := fx.SpaceDelete(legalOwnerCtx, "child.id", 60, []byte("payload"), "payload.id")
		require.NoError(t, err)
		require.Equal(t, int64(42), toBeDeleted)
	})

	t.Run("non-owner non-legal-owner is rejected", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.account = networkKeys(t)
		fx.spaceStatus.EXPECT().Status(gomock.Any(), "child.id").Return(childEntry, nil)
		fx.acl.EXPECT().OwnerPubKey(gomock.Any(), "parent.id").Return(legalOwnerKeys.SignKey.GetPublic(), nil)
		_, err := fx.SpaceDelete(strangerCtx, "child.id", 60, []byte("payload"), "payload.id")
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})

	t.Run("top-level space keeps the owner-only path", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.account = networkKeys(t)
		topEntry := childEntry
		topEntry.ParentSpaceId = ""
		fx.spaceStatus.EXPECT().Status(gomock.Any(), "child.id").Return(topEntry, nil)
		fx.nodeConf.EXPECT().Configuration().Return(nodeconf.Configuration{NetworkId: "net"})
		fx.spaceStatus.EXPECT().SpaceDelete(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ interface{ Done() <-chan struct{} }, payload spacestatus.SpaceDeletion) (int64, error) {
				require.False(t, payload.AuthorizedByLegalOwner)
				return 7, nil
			})
		_, err := fx.SpaceDelete(strangerCtx, "child.id", 60, []byte("payload"), "payload.id")
		require.NoError(t, err)
	})
}

func keylessRemoveRecord(t *testing.T, signer *accountdata.AccountKeys, target crypto.PubKey) []byte {
	targetProto, err := target.Marshall()
	require.NoError(t, err)
	data := &aclrecordproto.AclData{AclContent: []*aclrecordproto.AclContentValue{{
		Value: &aclrecordproto.AclContentValue_AccountRemoveNoRotate{
			AccountRemoveNoRotate: &aclrecordproto.AclAccountRemoveNoRotate{Identities: [][]byte{targetProto}},
		},
	}}}
	marshalledData, err := data.MarshalVT()
	require.NoError(t, err)
	signerProto, err := signer.SignKey.GetPublic().Marshall()
	require.NoError(t, err)
	rec := &consensusproto.Record{PrevId: "prev", Identity: signerProto, Data: marshalledData}
	marshalledRec, err := rec.MarshalVT()
	require.NoError(t, err)
	sig, err := signer.SignKey.Sign(marshalledRec)
	require.NoError(t, err)
	raw, err := (&consensusproto.RawRecord{Payload: marshalledRec, Signature: sig}).MarshalVT()
	require.NoError(t, err)
	return raw
}

func TestCoordinator_AclAddRecordKeylessGate(t *testing.T) {
	legalOwnerKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	exOwnerKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	targetKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	callerKeyData, err := legalOwnerKeys.SignKey.GetPublic().Marshall()
	require.NoError(t, err)
	callCtx := peer.CtxWithPeerId(peer.CtxWithIdentity(ctx, callerKeyData), "peer.id")

	creatorKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	childEntry := spacestatus.StatusEntry{
		SpaceId:       "child.id",
		Identity:      creatorKeys.SignKey.GetPublic().Account(),
		Status:        spacestatus.SpaceStatusCreated,
		Type:          spacestatus.SpaceTypeRegular,
		IsShareable:   true,
		ParentSpaceId: "parent.id",
	}

	t.Run("current parent owner passes", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		payload := keylessRemoveRecord(t, legalOwnerKeys, targetKeys.SignKey.GetPublic())
		fx.spaceStatus.EXPECT().Status(gomock.Any(), "child.id").Return(childEntry, nil)
		fx.acl.EXPECT().OwnerPubKey(gomock.Any(), "parent.id").Return(legalOwnerKeys.SignKey.GetPublic(), nil)
		fx.acl.EXPECT().OwnerPubKey(gomock.Any(), "child.id").Return(creatorKeys.SignKey.GetPublic(), nil)
		fx.accountLimit.EXPECT().GetLimitsBySpace(gomock.Any(), "child.id").Return(accountlimit.SpaceLimits{SpaceMembersRead: 10, SpaceMembersWrite: 10}, nil)
		fx.acl.EXPECT().AddRecord(gomock.Any(), "child.id", gomock.Any(), gomock.Any()).Return(&consensusproto.RawRecordWithId{Id: "rec.id"}, nil)
		fx.aclEventLog.EXPECT().AddLog(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		_, err := fx.AclAddRecord(callCtx, "child.id", payload)
		require.NoError(t, err)
	})

	t.Run("ex-owner is rejected at submit (fail-closed window)", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		payload := keylessRemoveRecord(t, exOwnerKeys, targetKeys.SignKey.GetPublic())
		fx.spaceStatus.EXPECT().Status(gomock.Any(), "child.id").Return(childEntry, nil)
		fx.acl.EXPECT().OwnerPubKey(gomock.Any(), "parent.id").Return(legalOwnerKeys.SignKey.GetPublic(), nil)
		_, err := fx.AclAddRecord(callCtx, "child.id", payload)
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})

	t.Run("keyless record on a top-level space is rejected", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		payload := keylessRemoveRecord(t, legalOwnerKeys, targetKeys.SignKey.GetPublic())
		topEntry := childEntry
		topEntry.ParentSpaceId = ""
		fx.spaceStatus.EXPECT().Status(gomock.Any(), "child.id").Return(topEntry, nil)
		_, err := fx.AclAddRecord(callCtx, "child.id", payload)
		require.ErrorIs(t, err, coordinatorproto.ErrForbidden)
	})
}

func accountsAddRecord(t *testing.T, signer *accountdata.AccountKeys, target crypto.PubKey) []byte {
	targetProto, err := target.Marshall()
	require.NoError(t, err)
	data := &aclrecordproto.AclData{AclContent: []*aclrecordproto.AclContentValue{{
		Value: &aclrecordproto.AclContentValue_AccountsAdd{
			AccountsAdd: &aclrecordproto.AclAccountsAdd{Additions: []*aclrecordproto.AclAccountAdd{{
				Identity:    targetProto,
				Permissions: aclrecordproto.AclUserPermissions_Writer,
			}}},
		},
	}}}
	marshalledData, err := data.MarshalVT()
	require.NoError(t, err)
	signerProto, err := signer.SignKey.GetPublic().Marshall()
	require.NoError(t, err)
	rec := &consensusproto.Record{PrevId: "prev", Identity: signerProto, Data: marshalledData}
	marshalledRec, err := rec.MarshalVT()
	require.NoError(t, err)
	sig, err := signer.SignKey.Sign(marshalledRec)
	require.NoError(t, err)
	raw, err := (&consensusproto.RawRecord{Payload: marshalledRec, Signature: sig}).MarshalVT()
	require.NoError(t, err)
	return raw
}

func TestCoordinator_AclAddRecordExternalSeats(t *testing.T) {
	parentEx := list.NewAclExecutor("parent.id")
	for _, cmd := range []string{
		"a.init::a",
		"a.invite::inv",
		"b.join::inv",
		"a.approve::b,rw",
	} {
		require.NoError(t, parentEx.Execute(cmd))
	}
	var (
		parentAcl   = parentEx.ActualAccounts()["a"].Acl
		orgOwnerKey = parentEx.ActualAccounts()["a"].Keys
		orgMember   = parentEx.ActualAccounts()["b"].Keys
	)
	external, err := accountdata.NewRandom()
	require.NoError(t, err)
	creatorKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	callerKeyData, err := creatorKeys.SignKey.GetPublic().Marshall()
	require.NoError(t, err)
	callCtx := peer.CtxWithPeerId(peer.CtxWithIdentity(ctx, callerKeyData), "peer.id")

	childEntry := spacestatus.StatusEntry{
		SpaceId:       "child.id",
		Identity:      creatorKeys.SignKey.GetPublic().Account(),
		Status:        spacestatus.SpaceStatusCreated,
		Type:          spacestatus.SpaceTypeRegular,
		IsShareable:   true,
		ParentSpaceId: "parent.id",
	}
	expectReadState := func(fx *fixture) {
		fx.acl.EXPECT().ReadState(gomock.Any(), "parent.id", gomock.Any()).DoAndReturn(
			func(_ interface{ Done() <-chan struct{} }, _ string, f func(s *list.AclState) error) error {
				return f(parentAcl.AclState())
			}).AnyTimes()
	}

	t.Run("org member admission needs no seats", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		payload := accountsAddRecord(t, creatorKeys, orgMember.SignKey.GetPublic())
		fx.spaceStatus.EXPECT().Status(gomock.Any(), "child.id").Return(childEntry, nil)
		expectReadState(fx)
		fx.accountLimit.EXPECT().GetLimitsBySpace(gomock.Any(), "child.id").Return(accountlimit.SpaceLimits{SpaceMembersRead: 10, SpaceMembersWrite: 10}, nil)
		fx.acl.EXPECT().AddRecord(gomock.Any(), "child.id", gomock.Any(), gomock.Any()).Return(&consensusproto.RawRecordWithId{Id: "rec.id"}, nil)
		fx.acl.EXPECT().OwnerPubKey(gomock.Any(), "child.id").Return(creatorKeys.SignKey.GetPublic(), nil)
		fx.aclEventLog.EXPECT().AddLog(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		_, err := fx.AclAddRecord(callCtx, "child.id", payload)
		require.NoError(t, err)
	})

	t.Run("external rejected when the pool is exhausted", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		payload := accountsAddRecord(t, creatorKeys, external.SignKey.GetPublic())
		fx.spaceStatus.EXPECT().Status(gomock.Any(), "child.id").Return(childEntry, nil)
		expectReadState(fx)
		fx.acl.EXPECT().OwnerPubKey(gomock.Any(), "parent.id").Return(orgOwnerKey.SignKey.GetPublic(), nil)
		fx.accountLimit.EXPECT().GetLimits(gomock.Any(), orgOwnerKey.SignKey.GetPublic().Account()).Return(accountlimit.Limits{ExternalSeatsLimit: 0}, nil)
		_, err := fx.AclAddRecord(callCtx, "child.id", payload)
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceLimitReached)
	})

	t.Run("external admitted within the pool", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		payload := accountsAddRecord(t, creatorKeys, external.SignKey.GetPublic())
		fx.spaceStatus.EXPECT().Status(gomock.Any(), "child.id").Return(childEntry, nil)
		expectReadState(fx)
		fx.acl.EXPECT().OwnerPubKey(gomock.Any(), "parent.id").Return(orgOwnerKey.SignKey.GetPublic(), nil).AnyTimes()
		fx.accountLimit.EXPECT().GetLimits(gomock.Any(), orgOwnerKey.SignKey.GetPublic().Account()).Return(accountlimit.Limits{ExternalSeatsLimit: 3}, nil)
		fx.accountLimit.EXPECT().GetLimitsBySpace(gomock.Any(), "child.id").Return(accountlimit.SpaceLimits{SpaceMembersRead: 10, SpaceMembersWrite: 10}, nil)
		fx.acl.EXPECT().AddRecord(gomock.Any(), "child.id", gomock.Any(), gomock.Any()).Return(&consensusproto.RawRecordWithId{Id: "rec.id"}, nil)
		fx.acl.EXPECT().OwnerPubKey(gomock.Any(), "child.id").Return(creatorKeys.SignKey.GetPublic(), nil)
		fx.aclEventLog.EXPECT().AddLog(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		_, err := fx.AclAddRecord(callCtx, "child.id", payload)
		require.NoError(t, err)
	})
}
