package batcher

import (
	"github.com/dkrieger/redistream"
	"github.com/go-redis/redis"
)

func (b *Batcher) ReapSome(entries []redistream.Entry, streamName string) ([]redistream.Entry, error) {
	out := []redistream.Entry{}
	for _, e := range entries {
		val, err := b.redisClient.XPendingExt(&redis.XPendingExtArgs{
			Stream:   streamName,
			Group:    b.Consumer().Group,
			Start:    e.ID,
			End:      e.ID,
			Count:    1,
			Consumer: b.Consumer().Name,
		}).Result()
		if err != nil {
			return entries, err
		}
		if len(val) != 1 {
			out = append(out, e)
			continue
		}
		if b.shouldReap(val[0]) {
			err = b.redisClient.XClaim(&redis.XClaimArgs{
				Stream:   streamName,
				Group:    b.Consumer().Group,
				Consumer: b.reaper,
				Messages: []string{val[0].Id},
			}).Err()
			if err != nil {
				return entries, err
			}
		} else {
			out = append(out, e)
		}
	}
	return out, nil
}

// ReapBatch aggregates and stores entries that failed to be batched. the resulting
// stream functions as a log of failed entries. Entries get reaped based on
// being idle for too long, or being retried too many times. In normal
// operation, entries shouldn't be reaped very often if at all.
//
// NOTE: this name is somewhat ambiguous, as it might be interpreted as reaping
// failed batches. However, processing the batches should be left entirely to
// the client, or handled by a companion redis stream worker -- maybe even a
// differently configured batcher instance. In short, processing the
// destination batch is outside the scope of Batcher.
// e.g. maybe the client should check how many times it's tried the batch (or
// how long it sat idle) and, using MULTI EXEC, XADD to a failed batch log
// stream and XDEL the batch
func (b *Batcher) ReapBatch(name string) error {
	batches := b.getBatches()
	batch := batches[name]
	// make sure this is the only goroutine consuming this stream
	batch.consumerMutex.Lock()
	defer batch.consumerMutex.Unlock()
	conf := redistream.Config{
		MaxLenApprox: 1000,
	}
	streamClient := redistream.WrapClient(b.redisClient, conf)
	streamRaw, err := streamClient.Consume(redistream.ConsumeArgs{
		Consumer: b.ReaperConsumer(),
		Streams:  []string{name, "0"},
	})
	if err != nil {
		return err
	}
	stream, err := streamRaw.Merge(nil)
	aggregated, err := b.AggregateBatch(stream)
	if err != nil {
		return err
	}
	dest := []redistream.Entry{{
		Meta: &redistream.EntryMeta{Stream: b.MetaPrefix() + "reaped"},
		Hash: map[string]interface{}{"batch": aggregated, "from": name},
	}}
	_, _, err = streamClient.Process(redistream.ProcessArgs{
		From: stream, To: dest})
	return err
}
