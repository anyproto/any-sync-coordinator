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

type Data struct {
	Data      []byte `bson:"data"`
	Signature []byte `bson:"signature"`
}
