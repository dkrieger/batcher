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
			// limit concurrency
			b.lockPool.Lock()
			defer func() {
				time.Sleep(b.BatcherDelay())
				b.lockPool.Unlock()
			}()

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
