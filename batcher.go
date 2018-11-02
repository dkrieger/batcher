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
	scheduledBatches *sync.Map
	batches          *sync.Map
	redisClient      *redis.Client
	batchDest        string
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
