package spacestatus

import (
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/acl/aclrecordproto"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifySpaceHeader(t *testing.T) {
	privKey, puKey, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)

	newRawHeader := func(t *testing.T, spaceHeader *spacesyncproto.SpaceHeader, badSig bool) []byte {
		spaceHeaderBytes, err := spaceHeader.MarshalVT()
		require.NoError(t, err)

		sig, err := privKey.Sign(spaceHeaderBytes)
		require.NoError(t, err)
		if badSig {
			sig = append(sig, 1)
		}
		rawHeader := &spacesyncproto.RawSpaceHeader{
			SpaceHeader: spaceHeaderBytes,
			Signature:   sig,
		}
		rawHeaderBytes, err := rawHeader.MarshalVT()
		require.NoError(t, err)
		return rawHeaderBytes
	}
	// aclRoot is the payload of an acl root that the author signs, a one-to-one root with oneToOne
	aclRoot := func(t *testing.T, author crypto.PrivKey, oneToOne *aclrecordproto.AclOneToOneInfo) []byte {
		builder := list.NewAclRecordBuilder("", crypto.NewKeyStorage(), nil, recordverifier.NewValidateFull())
		content := list.RootContent{PrivKey: author, MasterKey: author}
		if oneToOne != nil {
			root, err := builder.BuildOneToOneRoot(content, oneToOne)
			require.NoError(t, err)
			return root.Payload
		}
		root, err := builder.BuildRoot(content)
		require.NoError(t, err)
		return root.Payload
	}
	v1 := spacesyncproto.SpaceHeaderVersion_SpaceHeaderVersion1

	t.Run("invalid signature", func(t *testing.T) {
		_, _, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp: 123,
			SpaceType: "123",
		}, true))
		assert.Error(t, err)
	})
	t.Run("personal", func(t *testing.T) {
		spaceType, headerType, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp: 0,
			SpaceType: "anytype.space",
		}, false))
		require.NoError(t, err)
		assert.Equal(t, SpaceTypePersonal, spaceType)
		assert.Equal(t, "anytype.space", headerType)
	})
	t.Run("tech", func(t *testing.T) {
		spaceType, headerType, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp: 0,
			SpaceType: techSpaceType,
		}, false))
		require.NoError(t, err)
		assert.Equal(t, SpaceTypeTech, spaceType)
		assert.Equal(t, techSpaceType, headerType)
	})
	t.Run("regular", func(t *testing.T) {
		spaceType, headerType, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp: 123243,
			SpaceType: "anytype.space",
		}, false))
		require.NoError(t, err)
		assert.Equal(t, SpaceTypeRegular, spaceType)
		assert.Equal(t, "anytype.space", headerType)
	})
	t.Run("chat", func(t *testing.T) {
		spaceType, _, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp: 0,
			SpaceType: chatSpaceType,
		}, false))
		require.NoError(t, err)
		assert.Equal(t, SpaceTypeRegular, spaceType)
	})
	t.Run("any.space", func(t *testing.T) {
		spaceType, headerType, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp:        123243,
			SpaceType:        anySpaceType,
			FileprotoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
		}, false))
		require.NoError(t, err)
		assert.Equal(t, SpaceTypeRegular, spaceType)
		assert.Equal(t, anySpaceType, headerType)
	})
	t.Run("any.space derived is personal", func(t *testing.T) {
		spaceType, _, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp:        0,
			SpaceType:        anySpaceType,
			FileprotoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
		}, false))
		require.NoError(t, err)
		assert.Equal(t, SpaceTypePersonal, spaceType)
	})
	t.Run("any.techspace", func(t *testing.T) {
		spaceType, headerType, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp:        0,
			SpaceType:        anyTechSpaceType,
			FileprotoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
		}, false))
		require.NoError(t, err)
		assert.Equal(t, SpaceTypeTech, spaceType)
		assert.Equal(t, anyTechSpaceType, headerType)
	})
	t.Run("an organization and its space are regular", func(t *testing.T) {
		for _, tp := range []string{anyOrganizationType, anyOrganizationSpaceType} {
			spaceType, headerType, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
				Timestamp:        123243,
				SpaceType:        tp,
				FileprotoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
				AclPayload:       aclRoot(t, privKey, nil),
				Version:          v1,
			}, false))
			require.NoError(t, err, tp)
			assert.Equal(t, SpaceTypeRegular, spaceType, tp)
			assert.Equal(t, tp, headerType)
		}
	})
	t.Run("an organization and its space need the acl root of the signer in a v1 header", func(t *testing.T) {
		otherKey, otherPub, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		signer, err := puKey.Marshall()
		require.NoError(t, err)
		other, err := otherPub.Marshall()
		require.NoError(t, err)
		oneToOne := &aclrecordproto.AclOneToOneInfo{Owner: other, Writers: [][]byte{signer, other}}
		for name, change := range map[string]func(h *spacesyncproto.SpaceHeader){
			"no root":                                 func(h *spacesyncproto.SpaceHeader) { h.AclPayload = nil },
			"not a root":                              func(h *spacesyncproto.SpaceHeader) { h.AclPayload = []byte("root") },
			"a v0 header, which binds no root":        func(h *spacesyncproto.SpaceHeader) { h.Version = spacesyncproto.SpaceHeaderVersion_SpaceHeaderVersion0 },
			"the root of another account":             func(h *spacesyncproto.SpaceHeader) { h.AclPayload = aclRoot(t, otherKey, nil) },
			"a one-to-one root that the signer signs": func(h *spacesyncproto.SpaceHeader) { h.AclPayload = aclRoot(t, privKey, oneToOne) },
		} {
			header := &spacesyncproto.SpaceHeader{
				Timestamp:        123243,
				FileprotoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
				AclPayload:       aclRoot(t, privKey, nil),
				Version:          v1,
			}
			change(header)
			for _, tp := range []string{anyOrganizationType, anyOrganizationSpaceType} {
				header.SpaceType = tp
				_, _, err := VerifySpaceHeader(puKey, newRawHeader(t, header, false))
				assert.ErrorContains(t, err, "requires the v1 header with the acl root of its creator", "%s: %s", tp, name)
			}
			// a space of another type keeps any root
			header.SpaceType = anySpaceType
			_, _, err := VerifySpaceHeader(puKey, newRawHeader(t, header, false))
			assert.NoError(t, err, name)
		}
	})
	t.Run("an organization and its space without a timestamp rejected", func(t *testing.T) {
		for _, tp := range []string{anyOrganizationType, anyOrganizationSpaceType} {
			_, headerType, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
				SpaceType:        tp,
				FileprotoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
			}, false))
			assert.ErrorContains(t, err, "requires a timestamp", tp)
			assert.Equal(t, tp, headerType)
		}
	})
	t.Run("any.* without fileproto v2 rejected", func(t *testing.T) {
		for _, tp := range []string{anySpaceType, anyTechSpaceType, anyOrganizationType, anyOrganizationSpaceType} {
			_, _, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
				Timestamp: 123243,
				SpaceType: tp,
			}, false))
			assert.ErrorContains(t, err, "requires fileproto version", tp)
		}
	})
	t.Run("anytype types accept fileproto v0", func(t *testing.T) {
		spaceType, _, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp: 123243,
			SpaceType: regularSpaceType,
		}, false))
		require.NoError(t, err)
		assert.Equal(t, SpaceTypeRegular, spaceType)
	})
	t.Run("onetoone variants", func(t *testing.T) {
		aSk, aPub, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		_, bPub, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)

		for tp, build := range map[string]func() ([]byte, error){
			"anytype.onetoone": func() ([]byte, error) {
				p, err := spacepayloads.StoragePayloadForOneToOneSpace(aSk, bPub)
				if err != nil {
					return nil, err
				}
				return p.SpaceHeaderWithId.RawHeader, nil
			},
			"any.onetoone": func() ([]byte, error) {
				p, err := spacepayloads.StoragePayloadForOneToOneSpaceWithType(aSk, bPub, spacepayloads.SpaceTypeOneToOneAny)
				if err != nil {
					return nil, err
				}
				return p.SpaceHeaderWithId.RawHeader, nil
			},
		} {
			raw, err := build()
			require.NoError(t, err, tp)
			spaceType, headerType, err := VerifySpaceHeader(aPub, raw)
			require.NoError(t, err, tp)
			assert.Equal(t, SpaceTypeOneToOne, spaceType, tp)
			assert.Equal(t, tp, headerType)
		}
	})
	t.Run("unknown type rejected", func(t *testing.T) {
		_, _, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp: 123243,
			SpaceType: "other.space",
		}, false))
		assert.ErrorContains(t, err, "unknown space type")
	})
}

func TestHeaderParent(t *testing.T) {
	const parentId = "parent.id"
	rawHeader := func(t *testing.T, header *spacesyncproto.SpaceHeader) []byte {
		headerBytes, err := header.MarshalVT()
		require.NoError(t, err)
		raw, err := (&spacesyncproto.RawSpaceHeader{SpaceHeader: headerBytes}).MarshalVT()
		require.NoError(t, err)
		return raw
	}
	v1 := spacesyncproto.SpaceHeaderVersion_SpaceHeaderVersion1

	t.Run("a top-level space has no parent", func(t *testing.T) {
		for _, tc := range []struct {
			spaceType  SpaceType
			headerType string
		}{
			{SpaceTypeTech, anyTechSpaceType},
			{SpaceTypeRegular, anySpaceType},
			{SpaceTypeRegular, anyOrganizationType},
		} {
			for _, version := range []spacesyncproto.SpaceHeaderVersion{spacesyncproto.SpaceHeaderVersion_SpaceHeaderVersion0, v1} {
				parent, err := HeaderParent(rawHeader(t, &spacesyncproto.SpaceHeader{Version: version}), tc.spaceType, tc.headerType)
				require.NoError(t, err, tc.headerType)
				assert.Empty(t, parent, tc.headerType)
			}
		}
	})
	t.Run("a space of an organization with the v1 header names its parent", func(t *testing.T) {
		parent, err := HeaderParent(rawHeader(t, &spacesyncproto.SpaceHeader{ParentSpaceId: parentId, Version: v1}), SpaceTypeRegular, anyOrganizationSpaceType)
		require.NoError(t, err)
		assert.Equal(t, parentId, parent)
	})
	t.Run("a space of an organization that names no parent is refused", func(t *testing.T) {
		_, err := HeaderParent(rawHeader(t, &spacesyncproto.SpaceHeader{Version: v1}), SpaceTypeRegular, anyOrganizationSpaceType)
		require.ErrorContains(t, err, "requires a parent space")
		require.NotErrorIs(t, err, coordinatorproto.ErrParentSpaceInvalid)
	})
	t.Run("a header that is not v1 cannot name a parent", func(t *testing.T) {
		_, err := HeaderParent(rawHeader(t, &spacesyncproto.SpaceHeader{ParentSpaceId: parentId}), SpaceTypeRegular, anyOrganizationSpaceType)
		require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceInvalid)
	})
	t.Run("only a space of an organization can name a parent", func(t *testing.T) {
		for _, tc := range []struct {
			spaceType  SpaceType
			headerType string
		}{
			{SpaceTypeRegular, anyOrganizationType},
			{SpaceTypeRegular, anySpaceType},
			{SpaceTypePersonal, anySpaceType},
			{SpaceTypeTech, anyTechSpaceType},
			{SpaceTypeOneToOne, anyOneToOneSpaceType},
			{SpaceTypeRegular, regularSpaceType},
			{SpaceTypeRegular, chatSpaceType},
			{SpaceTypeRegular, ""},
		} {
			_, err := HeaderParent(rawHeader(t, &spacesyncproto.SpaceHeader{ParentSpaceId: parentId, Version: v1}), tc.spaceType, tc.headerType)
			require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceInvalid, "%d %q", tc.spaceType, tc.headerType)
		}
	})
}

func TestHasGuestRole(t *testing.T) {
	for _, headerType := range []string{anyOrganizationType, anyOrganizationSpaceType} {
		assert.False(t, HasGuestRole(headerType), headerType)
	}
	for _, headerType := range []string{"", regularSpaceType, chatSpaceType, oneToOneSpaceType, anySpaceType, anyOneToOneSpaceType, "other.space"} {
		assert.True(t, HasGuestRole(headerType), headerType)
	}
}

func TestCheckParent(t *testing.T) {
	parent := func(change func(e *StatusEntry)) StatusEntry {
		e := StatusEntry{SpaceId: "parent.id", Type: SpaceTypeRegular, HeaderType: anyOrganizationType, Status: SpaceStatusCreated}
		if change != nil {
			change(&e)
		}
		return e
	}
	require.NoError(t, CheckParent(parent(nil)))

	for _, status := range []int{SpaceStatusDeletionPending, SpaceStatusDeletionStarted, SpaceStatusDeleted} {
		err := CheckParent(parent(func(e *StatusEntry) { e.Status = status }))
		require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceNotAvailable, "status %d", status)
	}
	for name, change := range map[string]func(e *StatusEntry){
		"a space of an organization": func(e *StatusEntry) {
			e.ParentSpaceId, e.HeaderType = "grandparent.id", anyOrganizationSpaceType
		},
		"an organization with a parent": func(e *StatusEntry) { e.ParentSpaceId = "grandparent.id" },
		"a regular any.space":           func(e *StatusEntry) { e.HeaderType = anySpaceType },
		"a derived space":               func(e *StatusEntry) { e.Type, e.HeaderType = SpaceTypePersonal, anySpaceType },
		"a tech space":                  func(e *StatusEntry) { e.Type, e.HeaderType = SpaceTypeTech, anyTechSpaceType },
		"a 1-1 space":                   func(e *StatusEntry) { e.Type, e.HeaderType = SpaceTypeOneToOne, anyOneToOneSpaceType },
		"an anytype space":              func(e *StatusEntry) { e.HeaderType = regularSpaceType },
		"a chat space":                  func(e *StatusEntry) { e.HeaderType = chatSpaceType },
		"an untyped space":              func(e *StatusEntry) { e.HeaderType = "" },
		"an unknown type":               func(e *StatusEntry) { e.HeaderType = "other.space" },
		"a space of another class":      func(e *StatusEntry) { e.Type = SpaceTypePersonal },
		"a chat-class space":            func(e *StatusEntry) { e.Type = SpaceTypeChat },
	} {
		require.ErrorIs(t, CheckParent(parent(change)), coordinatorproto.ErrParentSpaceInvalid, name)
	}
}
