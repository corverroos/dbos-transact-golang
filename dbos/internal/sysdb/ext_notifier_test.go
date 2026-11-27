package sysdb

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"
)

// recordingNotifier records Notify calls.
type recordingNotifier struct {
	mu       sync.Mutex
	notified []string
}

func (n *recordingNotifier) Notify(_ context.Context, topics []string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.notified = append(n.notified, topics...)
	return nil
}

func (n *recordingNotifier) Subscribe(context.Context) (<-chan string, error) {
	return make(chan string), nil
}

func (n *recordingNotifier) topics() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := slices.Clone(n.notified)
	slices.Sort(out)
	return out
}

func notifierSysDB(n Notifier) *SysDB {
	s := &SysDB{
		dialect:        SqliteDialect{},
		logger:         slog.New(slog.DiscardHandler),
		RecvNotifier:   newNotifyRegistry(_DBOS_NOTIFICATIONS_CHANNEL, false),
		EventNotifier:  newNotifyRegistry(_DBOS_WORKFLOW_EVENTS_CHANNEL, false),
		streamNotifier: newNotifyRegistry(_DBOS_STREAMS_CHANNEL, false),
	}
	s.attachNotifier(n)
	return s
}

func fired(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func waitFired(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter not woken")
	}
}

// Without a Notifier every signal is a no-op and the queue wake channel is
// nil, so selecting on it never fires: behaviour is exactly as before.
func TestNotifierUnsetIsNoop(t *testing.T) {
	s := notifierSysDB(nil)
	ctx := context.Background()
	recv := s.RecvNotifier.subscribe("dest::t")
	s.SignalQueue("q")
	s.SignalMessagesSent(ctx, []WorkflowSendRow{{DestinationID: "dest", Topic: "t"}})
	s.flushNotifier(ctx)

	q, release := s.QueueWakeChannel("q")
	defer release()
	if q != nil {
		t.Fatal("expected a nil queue wake channel without a Notifier")
	}
	if fired(recv) {
		t.Fatal("expected no Recv wake without a Notifier")
	}
}

// A signal wakes this process's waiters at once and is broadcast on the next flush.
func TestNotifierSignalWakesLocallyAndBroadcasts(t *testing.T) {
	n := &recordingNotifier{}
	s := notifierSysDB(n)
	ctx := context.Background()

	q, releaseQ := s.QueueWakeChannel("q")
	defer releaseQ()
	other, releaseOther := s.QueueWakeChannel("other")
	defer releaseOther()
	recv := s.RecvNotifier.subscribe("dest::t")
	nullTopic := s.RecvNotifier.subscribe("dest::" + NullTopic)

	s.SignalQueue("q")
	s.SignalMessagesSent(ctx, []WorkflowSendRow{{DestinationID: "dest", Topic: "t"}, {DestinationID: "dest"}})

	if !fired(q) || fired(other) || !fired(recv) || !fired(nullTopic) {
		t.Fatal("expected exactly the signaled waiters to wake")
	}
	if got := n.topics(); len(got) != 0 {
		t.Fatalf("expected no broadcast before the flush, got %v", got)
	}

	s.flushNotifier(ctx)
	want := []string{"dbos:queue:q", "dbos:recv:dest::" + NullTopic, "dbos:recv:dest::t"}
	if got := n.topics(); !slices.Equal(got, want) {
		t.Fatalf("broadcast topics = %v, want %v", got, want)
	}
	s.flushNotifier(ctx) // nothing new queued
	if got := n.topics(); len(got) != len(want) {
		t.Fatalf("expected no second broadcast, got %v", got)
	}
}

// Topics from other processes wake the waiters they name; closing the
// subscription wakes every waiter to re-check.
func TestNotifierSubscribeDispatchesAndWakesAllOnClose(t *testing.T) {
	s := notifierSysDB(&recordingNotifier{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q, releaseQ := s.QueueWakeChannel("q")
	defer releaseQ()
	other, releaseOther := s.QueueWakeChannel("other")
	defer releaseOther()
	recv := s.RecvNotifier.subscribe("dest::t")

	sub := make(chan string, 8)
	done := make(chan struct{})
	go func() {
		s.consumeTopics(ctx, sub)
		close(done)
	}()

	sub <- "dbos:recv:dest::t"
	sub <- "dbos:queue:q"
	sub <- "unrelated"
	waitFired(t, recv)
	waitFired(t, q)
	close(sub)
	<-done
	if fired(other) {
		t.Fatal("queue waiter woke without its topic")
	}

	s.wakeAllNotifierWaiters() // what the subscribe loop does after a close
	if !fired(other) {
		t.Fatal("expected every waiter to wake after the subscription closed")
	}
}
