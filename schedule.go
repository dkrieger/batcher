package batcher

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
	}
}

// UnscheduleAllBatches
func (b *Batcher) UnscheduleAllBatches() {
	b.scheduledBatches.Range(func(key, value interface{}) bool {
		b.UnscheduleBatch(key.(string))
		return true
	})
}
