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
	"errors"
	"github.com/go-redis/redis"
	"hash/crc32"
	"strconv"
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
	name          string
	consumerMutex sync.Mutex
	config        BatchConfig
}

type Batcher struct {
	uuid             uint32
	config           *BatcherConfig
	scheduledBatches *sync.Map
	batches          *sync.Map
	redisClient      *redis.Client
	batchDest        string
}

func (b *Batcher) Prefix() string {
	return b.config.BatcherRedisPrefix
}

func (b *Batcher) MetaPrefix() string {
	return b.Prefix() + b.config.BatcherMetaRedisPrefix
}

type BatcherConfig struct {
	RedisOpts              *redis.Options
	BatcherRedisPrefix     string
	BatcherMetaRedisPrefix string
	DefaultBatchConfig     *BatchConfig
}

func (b *Batcher) renewLock() error {
	lock := b.MetaPrefix() + "lock"
	_, err := b.redisClient.SetNX(lock, b.uuid, 10*time.Second).Result()
	if err == nil {
		// lock set
		return nil
	} else if err != redis.Nil {
		// redis error
		return err
	}
	// check who holds the lock
	val, err := b.redisClient.Get(lock).Result()
	if err != nil {
		// redis error
		return err
	}
	if val != strconv.Itoa(int(b.uuid)) {
		// someone else has the lock
		return errors.New("lock held by another batcher instance")
	}
	// we got the lock!
	return nil

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
	uuid := crc32.ChecksumIEEE([]byte(strconv.Itoa(int(time.Now().UnixNano()))))
	b := &Batcher{
		scheduledBatches: &sync.Map{},
		redisClient:      client,
		uuid:             uuid,
	}

	// ensure no other batcher instance running
	err := b.renewLock()
	if err != nil {
		panic(err)
	}

	return b
}

type batchSignal uint16

const (
	quit batchSignal = iota
	changeConfig
)
