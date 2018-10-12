package batcher

import (
	"github.com/go-redis/redis"
	"runtime"
	"sync"
	"time"
)

type BatchConfig struct {
	TargetInterval time.Duration
	MaxSize        int
	MinSize        int
	Active         bool
	NoWait         bool
}

type BatchMetrics struct {
	EntriesPerHour float64
}

type BatchState struct {
	LastSend time.Time
}

type Batch struct {
	config BatchConfig
}

type Batcher struct {
	config           *BatcherConfig
	manifest         *sync.Map
	scheduledBatches *sync.Map
	batches          *sync.Map
	redisClient      *redis.Client
}

type BatcherConfig struct {
	RedisOpts              *redis.Options
	BatcherRedisPrefix     string
	BatcherMetaRedisPrefix string
	DefaultBatchConfig     *BatchConfig
}

func NewBatcher(config *BatcherConfig) *Batcher {
	if config.BatcherRedisPrefix == "" {
		config.BatcherRedisPrefix = "{batcher}."
	}
	if config.BatcherMetaRedisPrefix == "" {
		config.BatcherMetaRedisPrefix = "meta."
	}
	if config.DefaultBatchConfig == nil {
		config.DefaultBatchConfig = &BatchConfig{
			TargetInterval: 10 * time.Minute,
			MaxSize:        5,
			MinSize:        1,
			Active:         true,
			NoWait:         false,
		}
	}
	client := redis.NewClient(config.RedisOpts)
	return &Batcher{
		scheduledBatches: &sync.Map{},
		manifest:         &sync.Map{},
		redisClient:      client,
	}
}

type batchSignal uint16

const (
	quit batchSignal = iota
	changeConfig
)

func (b *Batcher) RefreshManifest() {
	// clear out our manifest and refresh it from Redis
}

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
			SendBatch(name)
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

func SendBatch(string) {
	// read MaxSize items from batch stream, move to ready stream (which an
	// external consumer watches)
}
