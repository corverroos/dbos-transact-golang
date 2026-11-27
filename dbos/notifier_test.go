package dbos

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

// memNotifier is an in-process Notifier for tests: a broadcast hub shared by
// several DBOS instances, standing in for a Redis channel or a Postgres
// LISTEN/NOTIFY channel between processes.
type memNotifier struct {
	mu      sync.Mutex
	subs    map[chan string]struct{}
	drop    atomic.Bool  // when set, Notify accepts topics and delivers none
	dropped atomic.Int64 // topics dropped while drop is set
}

var _ Notifier = (*memNotifier)(nil)

func newMemNotifier() *memNotifier {
	return &memNotifier{subs: make(map[chan string]struct{})}
}

func (n *memNotifier) Notify(_ context.Context, topics []string) error {
	if n.drop.Load() {
		n.dropped.Add(int64(len(topics)))
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for ch := range n.subs {
		for _, t := range topics {
			select {
			case ch <- t:
			default: // a slow subscriber loses hints, never blocks the notifier
			}
		}
	}
	return nil
}

func (n *memNotifier) Subscribe(ctx context.Context) (<-chan string, error) {
	ch := make(chan string, 1024)
	n.mu.Lock()
	n.subs[ch] = struct{}{}
	n.mu.Unlock()
	go func() {
		<-ctx.Done()
		n.unsubscribe(ch)
	}()
	return ch, nil
}

func (n *memNotifier) unsubscribe(ch chan string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.subs[ch]; ok {
		delete(n.subs, ch)
		close(ch)
	}
}

// closeAll ends every subscription, as a dropped Redis or LISTEN connection would.
func (n *memNotifier) closeAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for ch := range n.subs {
		delete(n.subs, ch)
		close(ch)
	}
}

func (n *memNotifier) subscriberCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.subs)
}

const (
	// slowPoll is far longer than any wake should take, so a wait that
	// completes within fastWake was woken by the notifier, not by a poll.
	slowPoll = 10 * time.Second
	fastWake = 2 * time.Second
	// quickPoll lets the lost-signal tests finish on polling alone.
	quickPoll = 100 * time.Millisecond
)

// notifierStarted holds, per workflow ID, a channel the started workflow closes.
var notifierStarted sync.Map

func startedChan(workflowID string) chan struct{} {
	ch, _ := notifierStarted.LoadOrStore(workflowID, make(chan struct{}))
	return ch.(chan struct{})
}

// notifierStartWorkflow reports that it started, then optionally waits for a
// message on topic "release" so it keeps its queue's concurrency slot.
func notifierStartWorkflow(ctx Context, hold bool) (string, error) {
	id, err := GetWorkflowID(ctx)
	if err != nil {
		return "", err
	}
	close(startedChan(id))
	if hold {
		return Recv[string](ctx, "release", time.Minute)
	}
	return id, nil
}

func notifierRecvWorkflow(ctx Context, topic string) (string, error) {
	return Recv[string](ctx, topic, time.Minute)
}

// newNotifierTestClient builds a Client like NewClient does (no LISTEN
// connection), with the poller cadence set by the test.
func newNotifierTestClient(t *testing.T, n Notifier, pollInterval time.Duration) Client {
	t.Helper()
	ctx, err := NewContext(context.Background(), Config{
		DatabaseURL:              backendDatabaseURL(t),
		AppName:                  "dbos-client",
		Notifier:                 n,
		SkipMigrations:           true,
		namelessOwner:            true,
		isClient:                 true,
		notificationPollInterval: pollInterval,
	})
	require.NoError(t, err)
	dc := ctx.(*dbosContext)
	dc.systemDB.Launch(dc)
	t.Cleanup(func() { _ = Shutdown(ctx, 30*time.Second) })
	return ctx
}

func sysDBOf(c Client) *sysdb.SysDB {
	return c.(*dbosContext).systemDB.(*sysdb.SysDB)
}

type notifierFixture struct {
	server Context
	client Client
	queue  Queue // worker concurrency 1
	hub    *memNotifier
}

// setupNotifierFixture launches a server and a client sharing hub. The server
// holds no LISTEN connection, so only the notifier or polling (at
// pollInterval) can wake its queue workers and Recv waiters.
func setupNotifierFixture(t *testing.T, hub *memNotifier, pollInterval time.Duration) notifierFixture {
	t.Helper()
	skipIfCockroach(t, "covered on Postgres and SQLite")
	server := setupDBOS(t, setupDBOSOptions{
		dropDB:                   true,
		notifier:                 hub,
		disableListener:          true,
		notificationPollInterval: pollInterval,
	})
	queue, err := RegisterQueue(server, "notifier-queue",
		WithQueueBasePollingInterval(pollInterval), WithWorkerConcurrency(1))
	require.NoError(t, err)
	RegisterWorkflow(server, notifierStartWorkflow, WithWorkflowName("NotifierStartWorkflow"))
	RegisterWorkflow(server, notifierRecvWorkflow, WithWorkflowName("NotifierRecvWorkflow"))

	require.NoError(t, Launch(server))
	client := newNotifierTestClient(t, hub, pollInterval)
	require.Eventually(t, func() bool { return hub.subscriberCount() == 2 }, fastWake, 5*time.Millisecond)
	return notifierFixture{server: server, client: client, queue: queue, hub: hub}
}

func (f notifierFixture) enqueue(t *testing.T, workflowID string, hold bool) {
	t.Helper()
	_, err := Enqueue[string, bool](f.client, f.queue.GetName(), "NotifierStartWorkflow", hold,
		WithEnqueueWorkflowID(workflowID),
		WithEnqueueApplicationVersion(f.server.GetApplicationVersion()))
	require.NoError(t, err)
}

func awaitStart(t *testing.T, workflowID string, timeout time.Duration) {
	t.Helper()
	select {
	case <-startedChan(workflowID):
	case <-time.After(timeout):
		t.Fatalf("workflow %s did not start within %s", workflowID, timeout)
	}
}

// enqueueAndAwaitStart enqueues from the client and returns the time until
// the server started the workflow.
func (f notifierFixture) enqueueAndAwaitStart(t *testing.T, workflowID string, timeout time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	f.enqueue(t, workflowID, false)
	awaitStart(t, workflowID, timeout)
	return time.Since(start)
}

// freeSlotAndAwaitStart fills the queue's one slot with a holding workflow,
// enqueues a second behind it, releases the first and returns the time from
// the release until the second started.
func (f notifierFixture) freeSlotAndAwaitStart(t *testing.T, prefix string, timeout time.Duration) time.Duration {
	t.Helper()
	holder, waiter := prefix+"-holder", prefix+"-waiter"
	f.enqueue(t, holder, true)
	awaitStart(t, holder, timeout)
	f.enqueue(t, waiter, false)
	select {
	case <-startedChan(waiter):
		t.Fatal("second workflow started while the slot was held")
	case <-time.After(50 * time.Millisecond):
	}

	start := time.Now()
	require.NoError(t, Send(f.client, holder, "go", "release"))
	awaitStart(t, waiter, timeout)
	return time.Since(start)
}

// recvAfterSend runs a receiving workflow on the server, sends to it from the
// client once it is waiting, and returns the time from Send to its result.
func (f notifierFixture) recvAfterSend(t *testing.T, workflowID string, timeout time.Duration) time.Duration {
	t.Helper()
	handle, err := RunWorkflow(f.server, notifierRecvWorkflow, "go", WithWorkflowID(workflowID))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return sysDBOf(f.server).RecvNotifier.Has(workflowID + "::go")
	}, fastWake, time.Millisecond, "server Recv never started waiting")

	start := time.Now()
	require.NoError(t, Send(f.client, workflowID, "hello", "go"))
	out, err := handle.GetResult(WithHandleTimeout(timeout))
	require.NoError(t, err)
	require.Equal(t, "hello", out)
	return time.Since(start)
}

// TestNotifierWakesFasterThanPolling proves each wait is woken by the
// notifier: every poll here is 10s, and each must finish in under 2s.
func TestNotifierWakesFasterThanPolling(t *testing.T) {
	f := setupNotifierFixture(t, newMemNotifier(), slowPoll)

	t.Run("EnqueueStartsImmediately", func(t *testing.T) {
		elapsed := f.enqueueAndAwaitStart(t, "notifier-fast-enqueue", time.Minute)
		require.Less(t, elapsed, fastWake)
	})

	t.Run("FreedSlotStartsNextImmediately", func(t *testing.T) {
		// Send wakes the holder's Recv, its completion wakes the queue.
		elapsed := f.freeSlotAndAwaitStart(t, "notifier-fast-slot", time.Minute)
		require.Less(t, elapsed, fastWake)
	})

	t.Run("RecvFromClientSend", func(t *testing.T) {
		elapsed := f.recvAfterSend(t, "notifier-fast-recv", time.Minute)
		require.Less(t, elapsed, fastWake)
	})
}

// TestNotifierLostSignalsFallBackToPolling drops every topic: each wait still
// completes, on its polling interval.
func TestNotifierLostSignalsFallBackToPolling(t *testing.T) {
	hub := newMemNotifier()
	hub.drop.Store(true)
	f := setupNotifierFixture(t, hub, quickPoll)

	f.enqueueAndAwaitStart(t, "notifier-lossy-enqueue", 10*time.Second)
	f.recvAfterSend(t, "notifier-lossy-recv", 10*time.Second)
	require.Positive(t, hub.dropped.Load(), "expected topics to have been sent and dropped")
}

// TestNotifierSubscriptionLossWakesWaiters checks that a closed subscription
// makes waiters re-check: a message is sent while topics are dropped, then the
// subscription closes, and the waiting Recv finds the message without waiting
// out its 10s poll.
func TestNotifierSubscriptionLossWakesWaiters(t *testing.T) {
	hub := newMemNotifier()
	f := setupNotifierFixture(t, hub, slowPoll)
	const workflowID = "notifier-resubscribe"

	handle, err := RunWorkflow(f.server, notifierRecvWorkflow, "go", WithWorkflowID(workflowID))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return sysDBOf(f.server).RecvNotifier.Has(workflowID + "::go")
	}, fastWake, time.Millisecond)

	hub.drop.Store(true)
	require.NoError(t, Send(f.client, workflowID, "hello", "go"))
	require.Eventually(t, func() bool { return hub.dropped.Load() > 0 }, fastWake, time.Millisecond)
	hub.drop.Store(false)

	start := time.Now()
	hub.closeAll()
	out, err := handle.GetResult(WithHandleTimeout(time.Minute))
	require.NoError(t, err)
	require.Equal(t, "hello", out)
	require.Less(t, time.Since(start), fastWake)

	// Both instances subscribe again.
	require.Eventually(t, func() bool { return hub.subscriberCount() == 2 }, 5*time.Second, 5*time.Millisecond)
}
