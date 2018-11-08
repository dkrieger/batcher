/*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 2 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program; if not, see <http://www.gnu.org/licenses/>.
*
* Copyright (C) Doug Krieger <doug@debutdigital.com>, 2018
 */

package batcher

import (
	"github.com/dkrieger/redistream"
	"github.com/go-redis/redis"
)

func (b *Batcher) ReapSome(entries []redistream.Entry, name string) ([]redistream.Entry, error) {
	out := []redistream.Entry{}
	for _, e := range entries {
		val, err := b.redisClient.XPendingExt(&redis.XPendingExtArgs{
			Stream:   name,
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
				Stream:   name,
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
func (b *Batcher) ReapBatch(name string) error {
	batchTmp, _ := b.batches.Load(name)
	batch := batchTmp.(*Batch)
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

// Reclaim `XCLAIM`s entries from old consumers on behalf of the current consumer
// Don't delete the consumer, or we'll get repeats.
func (b *Batcher) Reclaim(name string) error {
	err := b.renewLock()
	if err != nil {
		return err
	}
	cli := b.redisClient
	// get list of consumers
	val, err := cli.XPending(name, b.Consumer().Group).Result()
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
			Stream:   name,
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
	reap := []string{}
	for _, v := range vals {
		if b.shouldReap(v) {
			reap = append(reap, v.Id)
		} else {
			claim = append(claim, v.Id)
		}
	}
	err = cli.XClaim(&redis.XClaimArgs{
		Stream:   name,
		Group:    b.Consumer().Group,
		Consumer: b.Consumer().Name,
		Messages: claim,
	}).Err()
	err = cli.XClaim(&redis.XClaimArgs{
		Stream:   name,
		Group:    b.Consumer().Group,
		Consumer: b.reaper,
		Messages: claim,
	}).Err()
	if err != nil {
		return err
	}
	return nil
}
