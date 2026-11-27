package sysdb

import (
	"context"
	"strings"
)

// ext_notifier.go: the optional, user-supplied Notifier.
//
// The system database is the source of truth; a Notifier topic only tells a
// waiter "re-check now". Every wait keeps its polling interval as the
// fallback, so a lost, late or duplicated topic costs latency, never
// correctness. With no Notifier configured none of this runs.
//
// Two waits are covered:
//   - queue workers, woken when a workflow is enqueued on their queue or a
//     queued workflow finishes and frees concurrency;
//   - Recv, woken when a message is sent to it.

// Notifier is an optional wake-up channel next to the system database, for
// waking waiters in other processes sooner than their polling intervals.
//
// Topics are hints only. DBOS never relies on delivery: a topic that is
// dropped, delayed or duplicated leaves every wait to complete on its polling
// interval, as it would without a Notifier.
//
// Implementations broadcast: every topic passed to Notify by any process
// sharing the system database should reach every Subscribe stream, including
// the notifying process's own. Topics are opaque strings.
type Notifier interface {
	// Notify broadcasts that the given topics changed. DBOS calls it from a
	// background loop after the writes have committed, with topics coalesced
	// over the notification coalesce interval. An error is logged and the
	// topics are dropped.
	Notify(ctx context.Context, topics []string) error

	// Subscribe returns a stream of every topic notified by any process until
	// ctx is done. The returned channel closing means topics may have been
	// lost: DBOS then wakes every waiter to re-check and subscribes again. An
	// error is retried with backoff.
	Subscribe(ctx context.Context) (<-chan string, error)
}

// Topic layout: "dbos:<kind>:<id>". A recv id is the Recv waiter payload, so a
// received topic maps straight onto the registry.
const (
	topicRecv  = "dbos:recv:"
	topicQueue = "dbos:queue:"
)

// attachNotifier enables the Notifier paths. Call before Launch.
func (s *SysDB) attachNotifier(n Notifier) {
	if n == nil {
		return
	}
	s.notifier = n
	s.notifierPending = make(map[string]struct{})
	s.queueWake = newNotifyRegistry("", false)
}

// publish wakes local waiters for topics and queues the topics for the
// notifier loop to broadcast. Call only after the writes have committed.
func (s *SysDB) publish(topics ...string) {
	if s.notifier == nil {
		return
	}
	for _, t := range topics {
		s.dispatchTopic(t)
	}
	s.notifierMu.Lock()
	for _, t := range topics {
		s.notifierPending[t] = struct{}{}
	}
	s.notifierMu.Unlock()
}

func (s *SysDB) drainNotifierPending() []string {
	s.notifierMu.Lock()
	defer s.notifierMu.Unlock()
	if len(s.notifierPending) == 0 {
		return nil
	}
	topics := make([]string, 0, len(s.notifierPending))
	for t := range s.notifierPending {
		topics = append(topics, t)
	}
	clear(s.notifierPending)
	return topics
}

// flushNotifier broadcasts the topics queued since the last flush.
func (s *SysDB) flushNotifier(ctx context.Context) {
	if s.notifier == nil {
		return
	}
	topics := s.drainNotifierPending()
	if len(topics) == 0 {
		return
	}
	if err := s.notifier.Notify(ctx, topics); err != nil && ctx.Err() == nil {
		// Waiters make progress on their polling interval.
		s.logger.Warn("Notifier failed to notify", "count", len(topics), "error", err)
	}
}

// dispatchTopic wakes the local waiters a topic names. Unknown topics are ignored.
func (s *SysDB) dispatchTopic(topic string) {
	switch {
	case strings.HasPrefix(topic, topicRecv):
		s.RecvNotifier.notify(strings.TrimPrefix(topic, topicRecv))
	case strings.HasPrefix(topic, topicQueue):
		s.queueWake.notify(strings.TrimPrefix(topic, topicQueue))
	}
}

// wakeAllNotifierWaiters wakes every waiter to re-check, after topics may have been lost.
func (s *SysDB) wakeAllNotifierWaiters() {
	s.RecvNotifier.notifyAll()
	s.queueWake.notifyAll()
}

// notifierSubscribeLoop feeds received topics to local waiters until ctx is done.
func (s *SysDB) notifierSubscribeLoop(ctx context.Context) {
	defer s.logger.Debug("Notifier subscribe loop exiting")
	attempt := 0
	subscribed := false
	for ctx.Err() == nil {
		topics, err := s.notifier.Subscribe(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("Notifier subscribe failed", "error", err)
			if !WaitForRetry(ctx, ConnectionRetryBackoff.DelayFor(attempt+1)) {
				return
			}
			attempt++
			continue
		}
		attempt = 0
		if subscribed {
			// Topics sent while resubscribing were missed.
			s.wakeAllNotifierWaiters()
		}
		subscribed = true
		s.consumeTopics(ctx, topics)
		if ctx.Err() == nil {
			s.logger.Debug("Notifier subscription closed, resubscribing")
			s.wakeAllNotifierWaiters()
		}
	}
}

func (s *SysDB) consumeTopics(ctx context.Context, topics <-chan string) {
	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-topics:
			if !ok {
				return
			}
			s.dispatchTopic(t)
		}
	}
}

// QueueWakeChannel returns a channel signaled when work may be ready on the
// named queue, and a func to drop the registration. Without a Notifier the
// channel is nil, so selecting on it never fires.
func (s *SysDB) QueueWakeChannel(queueName string) (<-chan struct{}, func()) {
	if s.notifier == nil {
		return nil, func() {}
	}
	ch := s.queueWake.subscribe(queueName)
	return ch, func() { s.queueWake.unsubscribe(queueName, ch) }
}

// SignalQueue reports that work may be ready on the named queue: a workflow
// was enqueued, or a queued one finished and freed concurrency. Call it after
// the write commits.
func (s *SysDB) SignalQueue(queueName string) {
	if s.notifier == nil || queueName == "" {
		return
	}
	s.publish(topicQueue + queueName)
}

// SignalMessagesSent wakes the receivers of messages, here and elsewhere. Call
// it after the write commits.
func (s *SysDB) SignalMessagesSent(ctx context.Context, messages []WorkflowSendRow) {
	if s.notifier == nil || len(messages) == 0 {
		return
	}
	topics := make([]string, 0, len(messages))
	for _, m := range messages {
		key, err := s.keyOf(ctx, m.DestinationID, m.ShardKey)
		if err != nil {
			continue // the receiver's poll still finds it
		}
		topics = append(topics, topicRecv+waiterPayload(key, m.DestinationID, notificationTopic(m)))
	}
	s.publish(topics...)
}
