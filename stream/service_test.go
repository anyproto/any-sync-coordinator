package stream

import (
	"context"
	consensus "github.com/anyproto/any-sync-consensusnode"
	"github.com/anyproto/any-sync-consensusnode/db"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var ctx = context.Background()

func TestService_NewStream(t *testing.T) {
	t.Run("watch/unwatch", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.Finish(t)

		var expLogId = "logId"
		var preloadLogId = "preloadId"

		fx.mockDB.fetchLog = func(ctx context.Context, logId string) (log consensus.Log, err error) {
			require.Equal(t, expLogId, logId)
			return consensus.Log{
				Id: logId,
				Records: []consensus.Record{
					{
						Id: "1",
					},
				},
			}, nil
		}

		fx.mockDB.receiver(preloadLogId, []consensus.Record{
			{
				Id:     "2",
				PrevId: "1",
			},
			{
				Id: "1",
			},
		})

		st1 := fx.NewStream()
		sr1 := readStream(st1)
		assert.Equal(t, uint64(1), sr1.id)
		st1.WatchIds(ctx, []string{expLogId, preloadLogId})
		st1.UnwatchIds(ctx, []string{preloadLogId})
		assert.Equal(t, []string{expLogId}, st1.LogIds())

		st2 := fx.NewStream()
		sr2 := readStream(st2)
		assert.Equal(t, uint64(2), sr2.id)
		st2.WatchIds(ctx, []string{expLogId, preloadLogId})

		fx.mockDB.receiver(expLogId, []consensus.Record{
			{
				Id: "1",
			},
		})
		fx.mockDB.receiver(expLogId, []consensus.Record{
			{
				Id:     "2",
				PrevId: "1",
			},
			{
				Id: "1",
			},
		})
		fx.mockDB.receiver(preloadLogId, []consensus.Record{
			{
				Id:     "3",
				PrevId: "4",
			},
			{
				Id:     "2",
				PrevId: "1",
			},
			{
				Id: "1",
			},
		})
		st1.Close()
		st2.Close()

		for _, sr := range []*streamReader{sr1, sr2} {
			select {
			case <-time.After(time.Second / 3):
				require.False(t, true, "timeout")
			case <-sr.finished:
			}
		}

		require.Len(t, sr1.logs, 2)
		assert.Len(t, sr1.logs[string(expLogId)].Records, 2)
		assert.Equal(t, "2", sr1.logs[string(expLogId)].Records[0].Id)
		assert.Equal(t, "2", sr1.logs[string(preloadLogId)].Records[0].Id)

		require.Len(t, sr2.logs, 2)
		assert.Len(t, sr2.logs[string(expLogId)].Records, 2)
		assert.Equal(t, "2", sr2.logs[string(expLogId)].Records[0].Id)
		assert.Equal(t, "3", sr2.logs[string(preloadLogId)].Records[0].Id)
	})
	t.Run("error", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.Finish(t)
		fx.mockDB.fetchLog = func(ctx context.Context, logId string) (log consensus.Log, err error) {
			return log, consensuserr.ErrLogNotFound
		}
		st1 := fx.NewStream()
		sr1 := readStream(st1)
		id := "nonExists"
		assert.Equal(t, uint64(1), sr1.id)
		st1.WatchIds(ctx, []string{id})
		st1.Close()
		<-sr1.finished
		require.Len(t, sr1.logs, 1)
		assert.Equal(t, consensuserr.ErrLogNotFound, sr1.logs[string(id)].Err)
	})
}

func newFixture(t *testing.T) *fixture {
	fx := &fixture{
		Service: New(),
		mockDB:  &mockDB{},
		a:       new(app.App),
	}

	fx.a.Register(fx.Service).Register(fx.mockDB)
	require.NoError(t, fx.a.Start(ctx))
	return fx
}

type fixture struct {
	Service
	mockDB *mockDB
	a      *app.App
}

func (fx *fixture) Finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
}

func readStream(st *Stream) *streamReader {
	sr := &streamReader{
		id:       st.id,
		stream:   st,
		logs:     map[string]consensus.Log{},
		finished: make(chan struct{}),
	}
	go sr.read()
	return sr
}

type streamReader struct {
	id     uint64
	stream *Stream

	logs     map[string]consensus.Log
	finished chan struct{}
}

func (sr *streamReader) read() {
	defer close(sr.finished)
	for {
		logs := sr.stream.WaitLogs()
		if len(logs) == 0 {
			return
		}
		for _, l := range logs {
			if el, ok := sr.logs[l.Id]; !ok {
				sr.logs[l.Id] = l
			} else {
				rec := make([]consensus.Record, len(l.Records))
				copy(rec, l.Records)
				el.Records = append(rec, el.Records...)
				sr.logs[l.Id] = el
			}
		}
	}
}

var _ db.Service = &mockDB{}

type mockDB struct {
	receiver db.ChangeReceiver
	fetchLog func(ctx context.Context, logId string) (log consensus.Log, err error)
}

func (m *mockDB) AddLog(ctx context.Context, log consensus.Log) (err error) { return nil }
func (m *mockDB) DeleteLog(ctx context.Context, logId string) error         { return nil }
func (m *mockDB) AddRecord(ctx context.Context, logId string, record consensus.Record) (err error) {
	return nil
}

func (m *mockDB) FetchLog(ctx context.Context, logId string) (log consensus.Log, err error) {
	return m.fetchLog(ctx, logId)
}

func (m *mockDB) SetChangeReceiver(receiver db.ChangeReceiver) (err error) {
	m.receiver = receiver
	return nil
}

func (m *mockDB) Init(a *app.App) (err error) {
	return nil
}

func (m *mockDB) Name() (name string) {
	return db.CName
}

func (m *mockDB) Run(ctx context.Context) (err error) {
	return
}

func (m *mockDB) Close(ctx context.Context) (err error) {
	return
}
