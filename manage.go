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
	"github.com/go-redis/redis"
)

func (b *Batcher) RefreshManifest() {
	// clear out our manifest and refresh it from Redis
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
	return nil
}
