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
	"runtime"
	"time"
)

// ScheduleBatch
func (b *Batcher) ScheduleBatch(name string, signals <-chan batchSignal) {
	paused := false
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
			// get batch config and state
			// MOCK
			state := BatchMetrics{
				LastSend: time.Now(),
			}
			// . . .
			if !conf.Active {
				break Outer
			}
			// sleep until next time batch should be sent
			last := state.LastSend
			interval := conf.TargetInterval
			now := time.Now()
			time.Sleep(last.Add(interval).Sub(now))
			b.SendBatch(name)
		}
		runtime.Gosched()
	}
}

// UnscheduleBatch
func (b *Batcher) UnscheduleBatch(name string) {
	scheduled := b.getScheduled()
	keyExists := func(key string, batches map[string](chan batchSignal)) bool {
		for k, _ := range batches {
			if k == key {
				return true
			}
		}
		return false
	}
	if keyExists(name, scheduled) {
		scheduled[name] <- quit
	}
}

// UnscheduleAllBatches
func (b *Batcher) UnscheduleAllBatches() {
	scheduled := b.getScheduled()
	for k, _ := range scheduled {
		b.UnscheduleBatch(k)
	}
}
