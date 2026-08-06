package spacestatus

import (
	"testing"

	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
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
	t.Run("any.* without fileproto v2 rejected", func(t *testing.T) {
		for _, tp := range []string{anySpaceType, anyTechSpaceType} {
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
	t.Run("unknown type rejected", func(t *testing.T) {
		_, _, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp: 123243,
			SpaceType: "other.space",
		}, false))
		assert.ErrorContains(t, err, "unknown space type")
	})
}
