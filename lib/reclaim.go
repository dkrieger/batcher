package batcher

import (
	"github.com/go-redis/redis"
	"time"
)

// Reclaim `XCLAIM`s entries from old consumers on behalf of the current consumer
// Don't delete the consumer, or we'll get repeats.
func (b *Batcher) Reclaim(name string) error {
	err := b.renewLock()
	if err != nil {
		return err
	}
	cli := b.redisClient
	// get list of consumers
	val, err := cli.XPending(b.StreamPrefix()+name, b.Consumer().Group).Result()
	if err != nil {
		return err
	}
	oldConsumers := map[string]int64{}
	for k, v := range val.Consumers {
		if k != b.Consumer().Name {
			oldConsumers[k] = v
		}
	}
	vals := []redis.XPendingExt{}
	for c, count := range oldConsumers {
		val, err := cli.XPendingExt(&redis.XPendingExtArgs{
			Stream:   b.StreamPrefix() + name,
			Group:    b.Consumer().Group,
			Start:    "-",
			End:      "+",
			Count:    count,
			Consumer: c,
		}).Result()
		if err != nil {
			return err
		}
		vals = append(vals, val...)
	}
	claim := []string{}
	for _, v := range vals {
		claim = append(claim, v.Id)
	}
	if len(claim) == 0 {
		return nil
	}
	err = cli.XClaim(&redis.XClaimArgs{
		Stream:   b.StreamPrefix() + name,
		Group:    b.Consumer().Group,
		Consumer: b.Consumer().Name,
		MinIdle:  time.Second,
		Messages: claim,
	}).Err()
	if err != nil {
		return err
	}
	return nil
}
