package db

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

// Lease is a coarse cross-instance mutex backed by a mongo collection: one
// document per key, held for a TTL and taken over once expired. It serializes
// short critical sections across coordinator replicas — an in-process mutex
// only covers one instance.
//
// It is NOT a fencing lock: a holder stalled past the TTL loses exclusivity
// silently while its critical section may still be running. Size the TTL well
// above the worst-case critical section and treat the expiry as a bounded
// residual race, not an impossibility.
type Lease struct {
	coll *mongo.Collection
	ttl  time.Duration
	poll time.Duration
}

// NewLease wraps coll as a lease registry. ttl bounds how long a crashed
// holder blocks the key; poll is the base wait between takeover attempts
// (jittered up to 2x).
func NewLease(coll *mongo.Collection, ttl, poll time.Duration) *Lease {
	return &Lease{coll: coll, ttl: ttl, poll: poll}
}

type leaseDoc struct {
	Id        string    `bson:"_id"`
	Holder    string    `bson:"holder"`
	ExpiresAt time.Time `bson:"expiresAt"`
}

// Acquire blocks until it holds key's lease or ctx is done. The returned
// release is safe to call exactly once and works even when ctx is already
// canceled (it uses its own short deadline), so `defer release()` after a
// failed request still frees the key for other instances.
func (l *Lease) Acquire(ctx context.Context, key string) (release func(), err error) {
	tokenBytes := make([]byte, 16)
	if _, err = rand.Read(tokenBytes); err != nil {
		return nil, err
	}
	token := hex.EncodeToString(tokenBytes)
	release = func() {
		relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, derr := l.coll.DeleteOne(relCtx, bson.M{"_id": key, "holder": token}); derr != nil {
			// the TTL reclaims the key eventually; log so a systematic release
			// failure (mongo outage) is visible rather than silent serialization
			log.Warn("lease release failed, ttl will reclaim", zap.String("key", key), zap.Error(derr))
		}
	}
	for {
		now := time.Now()
		// take over a free (expired) lease
		res, uerr := l.coll.UpdateOne(ctx,
			bson.M{"_id": key, "expiresAt": bson.M{"$lt": now}},
			bson.M{"$set": bson.M{"holder": token, "expiresAt": now.Add(l.ttl)}},
		)
		if uerr != nil {
			return nil, uerr
		}
		if res.MatchedCount == 1 {
			return release, nil
		}
		// no expired doc: the key is either absent (claim it) or actively held (wait)
		_, ierr := l.coll.InsertOne(ctx, leaseDoc{Id: key, Holder: token, ExpiresAt: now.Add(l.ttl)})
		if ierr == nil {
			return release, nil
		}
		if !mongo.IsDuplicateKeyError(ierr) {
			return nil, ierr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(l.poll + jitter(l.poll)):
		}
	}
}

// jitter returns a uniform random duration in [0, max) to de-synchronize
// competing pollers.
func jitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64())
}
