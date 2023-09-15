package filelimit

import (
	"context"
	"github.com/anyproto/any-sync-coordinator/cafeapi"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
	"math"
	"time"
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
	cafeApi     cafeapi.CafeApi
	spaceStatus spacestatus.SpaceStatus
}

func (f *fileLimit) Init(a *app.App) (err error) {
	f.db = newDb(a.MustComponent(db.CName).(db.Database))
	f.cafeApi = a.MustComponent(cafeapi.CName).(cafeapi.CafeApi)
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
	statusEntry, err := f.spaceStatus.Status(ctx, spaceId, pk)
	if err != nil {
		return
	}
	if statusEntry.Status != spacestatus.SpaceStatusCreated {
		return 0, "", coordinatorproto.ErrSpaceIsDeleted
	}

	if f.conf.LimitDefault == 0 {
		// if not defined - unlimited
		return math.MaxUint64, statusEntry.Identity, nil
	}

	// by default, we use identity as storageKey, later will be additional logic to use the spaceId as the key
	storageKey = statusEntry.Identity

	var shouldCheckCafe bool
	limit, err = f.db.Get(ctx, spaceId)
	if err != nil {
		if err == ErrNotFound {
			limit = f.conf.LimitDefault
			shouldCheckCafe = true
			err = nil
		} else {
			return
		}
	}

	if shouldCheckCafe {
		cafeCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		userType, cafeErr := f.cafeApi.CheckCafeUserStatus(cafeCtx, statusEntry.OldIdentity)
		if cafeErr == nil {
			switch userType {
			case cafeapi.UserTypeOld:
				limit = f.conf.LimitAlphaUsers
			case cafeapi.UserTypeNightly:
				limit = f.conf.LimitNightlyUsers
			}
		}
		if userType != cafeapi.UserTypeNew {
			_ = f.db.Set(ctx, statusEntry.SpaceId, limit)
		}
	}
	return
}
