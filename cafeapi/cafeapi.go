package cafeapi

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/anyproto/any-sync/app"
	"net/http"
)

const CName = "cafeapi"

type UserType uint

const (
	UserTypeNew UserType = iota
	UserTypeOld
	UserTypeNightly
)

type CafeApi struct {
	conf Config
}

func (c *CafeApi) Init(a *app.App) (err error) {
	c.conf = a.MustComponent("config").(configGetter).GetCafeApi()
	return
}

func (c *CafeApi) Name() (name string) {
	return CName
}

type UserInfo struct {
	ID                string `json:"id"`
	CreatedAt         int64  `json:"created_at"`
	PrereleaseEnabled bool   `json:"prerelease_enabled"`
}

func (c *CafeApi) CheckCafeUserStatus(ctx context.Context, oldIdentity string) (userType UserType, err error) {
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
