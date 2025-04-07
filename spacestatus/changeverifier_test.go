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
		_, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp: 123,
			SpaceType: "123",
		}, true))
		assert.Error(t, err)
	})
	t.Run("personal", func(t *testing.T) {
		spaceType, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp: 0,
			SpaceType: "anytype.space",
		}, false))
		require.NoError(t, err)
		assert.Equal(t, SpaceTypePersonal, spaceType)
	})
	t.Run("tech", func(t *testing.T) {
		spaceType, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp: 0,
			SpaceType: techSpaceType,
		}, false))
		require.NoError(t, err)
		assert.Equal(t, SpaceTypeTech, spaceType)
	})
	t.Run("regular", func(t *testing.T) {
		spaceType, err := VerifySpaceHeader(puKey, newRawHeader(t, &spacesyncproto.SpaceHeader{
			Timestamp: 123243,
			SpaceType: "anytype.space",
		}, false))
		require.NoError(t, err)
		assert.Equal(t, SpaceTypeRegular, spaceType)
	})

}
