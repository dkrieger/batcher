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
}

type BatchMetrics struct {
	EntriesPerHour float64
}

type BatchState struct {
	LastSend time.Time
	Active   bool
	NoWait   bool
}

type Batcher struct {
	config           *BatcherConfig
	manifest         *sync.Map
	scheduledBatches *sync.Map
	redisClient      *redis.Client
}

type BatcherConfig struct {
	RedisOpts              *redis.Options
	BatcherRedisPrefix     string
	BatcherMetaRedisPrefix string
}

func NewBatcher(config *BatcherConfig) *Batcher {
	if config.BatcherRedisPrefix == "" {
		config.BatcherRedisPrefix = "{batcher}."
	}
	if config.BatcherMetaRedisPrefix == "" {
		config.BatcherMetaRedisPrefix = "meta."
	}
	client := redis.NewClient(config.RedisOpts)
	return &Batcher{
		scheduledBatches: &sync.Map{},
		manifest:         &sync.Map{},
		redisClient:      client,
	}
}

func (b *Batcher) RefreshManifest() {
	// clear out our manifest and refresh it from Redis
}

func (b *Batcher) ScheduleBatch(name string) {
	quit := make(chan bool, 1)
	b.scheduledBatches.Store(name, quit)
Outer:
	for {
		select {
		case <-quit:
			break Outer
		default:
			// throttle loop
			time.Sleep(time.Minute)
			// get batch config and state
			// MOCK
			config := BatchConfig{
				TargetInterval: 10 * time.Minute,
				MaxSize:        5,
				MinSize:        1,
			}
			// MOCK
			state := BatchState{
				LastSend: time.Now(),
				Active:   true,
				NoWait:   false,
			}
			// . . .
			if !state.Active {
				break Outer
			}
			// sleep until next time batch should be sent
			last := state.LastSend
			interval := config.TargetInterval
			now := time.Now()
			time.Sleep(last.Add(interval).Sub(now))
			SendBatch(name)
		}
		runtime.Gosched()
	}
	b.scheduledBatches.Delete(name)
	close(quit)
}

// UnscheduleBatch
func (b *Batcher) UnscheduleBatch(name string) {
	quit, ok := b.scheduledBatches.Load(name)
	if ok {
		quit.(chan bool) <- true
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
