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
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
)

// ScheduleBatch
func (b *Batcher) ScheduleBatch(name string, signals <-chan batchSignal) {
	stderr := log.New(os.Stderr, "", 0)
	err := b.Reclaim(name)
	if err != nil {
		stderr.Printf("Reclaimer error:\n%#v\n", err)
	}
	paused := false
	lastSend := time.Time{}
Outer:
	for {
		select {
		case s, ok := <-signals:
			if !ok {
				break Outer
			}
			switch s {
			case quit:
				break Outer
			case pause:
				paused = true
			case resume:
				paused = false
			}
		default:
			// throttle loop
			time.Sleep(time.Second)
			if paused {
				continue
			}
			batches := b.getBatches()
			batch := batches[name]
			conf := batch.config
			metrics := batch.metrics
			if metrics.LastSend.After(lastSend) {
				lastSend = metrics.LastSend
			}
			// if !conf.Active {
			// 	break Outer
			// }
			// sleep until next time batch should be sent
			interval := conf.TargetInterval
			now := time.Now()
			time.Sleep(lastSend.Add(interval).Sub(now))
			err := b.SendBatch(name)
			if err == nil {
				lastSend = time.Now()
				lastSendStr := strconv.Itoa(int(lastSend.Unix()))
				_, err := b.redisClient.HSet(b.MetricsPrefix()+"lastSend", name, lastSendStr).Result()
				if err != nil {
					stderr.Printf("ScheduleBatch() error:\ndetails: HSET lastSend\n%#v\n", err)
				}
			} else {
				stderr.Printf("SendBatch() error: \n%#v\ntime: %#v\n", err, time.Now())
			}
		}
		runtime.Gosched()
	}
}
