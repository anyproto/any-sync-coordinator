package identityrepo

import (
	"time"
)

type Entry struct {
	Id string `bson:"_id"`
	// kind -> data
	Data    map[string]Data `bson:"data"`
	Updated time.Time       `bson:"updated"`
}

func (e Entry) Kinds(kinds []string) Entry {
	res := Entry{
		Id:      e.Id,
		Data:    map[string]Data{},
		Updated: e.Updated,
	}
	for _, kind := range kinds {
		if data, ok := e.Data[kind]; ok {
			res.Data[kind] = data
		}
	}
	return res
}

type Data struct {
	Data      []byte `bson:"data"`
	Signature []byte `bson:"signature"`
}
