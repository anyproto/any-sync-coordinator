package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func newTestLeaseColl(t *testing.T) *mongo.Collection {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	coll := client.Database("coordinator_unittest_lease").Collection("leases")
	require.NoError(t, coll.Drop(ctx))
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = coll.Drop(cctx)
		_ = client.Disconnect(cctx)
	})
	return coll
}

func TestLease_MutualExclusion(t *testing.T) {
	lease := NewLease(newTestLeaseColl(t), 30*time.Second, 10*time.Millisecond)
	ctx := context.Background()

	release1, err := lease.Acquire(ctx, "org.1")
	require.NoError(t, err)

	// an unrelated key is not blocked
	releaseOther, err := lease.Acquire(ctx, "org.2")
	require.NoError(t, err)
	releaseOther()

	acquired := make(chan struct{})
	go func() {
		release2, err := lease.Acquire(ctx, "org.1")
		assert.NoError(t, err)
		close(acquired)
		release2()
	}()

	select {
	case <-acquired:
		t.Fatal("second acquire must block while the lease is held")
	case <-time.After(150 * time.Millisecond):
	}

	release1()
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("second acquire must proceed after release")
	}
}

func TestLease_ExpiredLeaseIsTakenOver(t *testing.T) {
	lease := NewLease(newTestLeaseColl(t), 100*time.Millisecond, 10*time.Millisecond)
	ctx := context.Background()

	// acquired and never released — a crashed holder
	_, err := lease.Acquire(ctx, "org.1")
	require.NoError(t, err)

	start := time.Now()
	release2, err := lease.Acquire(ctx, "org.1")
	require.NoError(t, err)
	release2()
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond, "takeover must wait for expiry")
}

func TestLease_ReleaseSurvivesCanceledContext(t *testing.T) {
	lease := NewLease(newTestLeaseColl(t), 30*time.Second, 10*time.Millisecond)

	reqCtx, cancel := context.WithCancel(context.Background())
	release, err := lease.Acquire(reqCtx, "org.1")
	require.NoError(t, err)
	cancel() // request failed/canceled after the lease was taken
	release()

	// the key is free immediately, not after the 30s ttl
	ctx, tcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer tcancel()
	release2, err := lease.Acquire(ctx, "org.1")
	require.NoError(t, err)
	release2()
}

func TestLease_AcquireHonorsContext(t *testing.T) {
	lease := NewLease(newTestLeaseColl(t), 30*time.Second, 10*time.Millisecond)

	release, err := lease.Acquire(context.Background(), "org.1")
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = lease.Acquire(ctx, "org.1")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
