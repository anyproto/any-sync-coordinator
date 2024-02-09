package filelimit

import (
	"context"
	"math"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
)

const CName = "coordinator.filelimit"

func New() FileLimit {
	return &fileLimit{}
}

type FileLimit interface {
	Get(ctx context.Context, identity []byte, spaceId string) (limit uint64, storageKey string, err error)

	app.Component
}

type fileLimit struct {
	conf        Config
	db          *fileLimitDb
	spaceStatus spacestatus.SpaceStatus
}

func (f *fileLimit) Init(a *app.App) (err error) {
	f.db = newDb(a.MustComponent(db.CName).(db.Database))
	f.spaceStatus = a.MustComponent(spacestatus.CName).(spacestatus.SpaceStatus)
	f.conf = a.MustComponent("config").(configGetter).GetFileLimit()
	return
}

func (f *fileLimit) Name() (name string) {
	return CName
}

func (f *fileLimit) Get(ctx context.Context, identity []byte, spaceId string) (limit uint64, storageKey string, err error) {
	pk, err := crypto.UnmarshalEd25519PublicKeyProto(identity)
	if err != nil {
		return
	}

	if spaceId != "" {
		statusEntry, err := f.spaceStatus.Status(ctx, spaceId)
		if err != nil {
			return 0, "", err
		}
		if statusEntry.Status != spacestatus.SpaceStatusCreated {
			return 0, "", coordinatorproto.ErrSpaceIsDeleted
		}
	}

	// by default, we use identity as storageKey, later will be additional logic to use the spaceId as the key
	storageKey = pk.Account()

	if f.conf.LimitDefault == 0 {
		// if not defined - unlimited
		return math.MaxUint64, storageKey, nil
	}

	limit, err = f.db.Get(ctx, storageKey)
	if err != nil {
		if err == ErrNotFound {
			limit = f.conf.LimitDefault
			err = nil
		} else {
			return
		}
	}
	return
}
