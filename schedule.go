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
func (b *Batcher) ScheduleBatch(name string) {
	signals := make(chan batchSignal)
	b.scheduledBatches.Store(name, signals)
	batch, _ := b.batches.Load(name)
	conf := batch.(Batch).config
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
			case changeConfig:
				batch, _ = b.batches.Load(name)
				conf = batch.(Batch).config
			}
		default:
			// throttle loop
			time.Sleep(time.Minute)
			// get batch config and state
			// MOCK
			state := BatchState{
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
	b.scheduledBatches.Delete(name)
	close(signals)
}

// UnscheduleBatch
func (b *Batcher) UnscheduleBatch(name string) {
	signals, ok := b.scheduledBatches.Load(name)
	if ok {
		signals.(chan batchSignal) <- quit
		close(signals.(chan batchSignal))
	}
}

// UnscheduleAllBatches
func (b *Batcher) UnscheduleAllBatches() {
	b.scheduledBatches.Range(func(key, value interface{}) bool {
		b.UnscheduleBatch(key.(string))
		return true
	})
}
