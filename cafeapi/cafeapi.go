//go:generate mockgen -destination mock_cafeapi/mock_cafeapi.go github.com/anyproto/any-sync-coordinator/cafeapi CafeApi
package cafeapi

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/anyproto/any-sync/app"
	"net/http"
)

const CName = "cafeapi"

func New() CafeApi {
	return new(cafeApi)
}

type UserType uint

const (
	UserTypeNew UserType = iota
	UserTypeOld
	UserTypeNightly
)

type CafeApi interface {
	CheckCafeUserStatus(ctx context.Context, oldIdentity string) (userType UserType, err error)
	app.Component
}

type cafeApi struct {
	conf Config
}

func (c *cafeApi) Init(a *app.App) (err error) {
	c.conf = a.MustComponent("config").(configGetter).GetCafeApi()
	return
}

func (c *cafeApi) Name() (name string) {
	return CName
}

type UserInfo struct {
	ID                string `json:"id"`
	CreatedAt         int64  `json:"created_at"`
	PrereleaseEnabled bool   `json:"prerelease_enabled"`
}

func (c *cafeApi) CheckCafeUserStatus(ctx context.Context, oldIdentity string) (userType UserType, err error) {
	if c.conf.Url == "" {
		// do not check if config is empty
		return UserTypeNew, nil
	}
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/admin/user?id=%s", c.conf.Url, oldIdentity), nil)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return
	}

	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound:
		return UserTypeNew, nil
	case http.StatusOK:
		var info UserInfo
		if err = json.NewDecoder(resp.Body).Decode(&info); err != nil {
			return
		}
		if info.PrereleaseEnabled {
			return UserTypeNightly, nil
		} else {
			return UserTypeOld, nil
		}
	default:
		return 0, fmt.Errorf("unexpected http status: %s", resp.Status)
	}
}
