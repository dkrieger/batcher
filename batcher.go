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
	"github.com/dkrieger/redistream"
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
	LastSend       time.Time
}

type Batch struct {
	name          string
	consumerMutex *sync.Mutex
	signals       chan batchSignal
	config        BatchConfig
	metrics       BatchMetrics
}

type Batcher struct {
	uuid        string
	config      *BatcherConfig
	batches     map[string]*Batch
	redisClient *redis.Client
	batchDest   string
	reaper      string
	shouldReap  func(redis.XPendingExt) bool
}

func (b *Batcher) getBatches() map[string]*Batch {
	ret := map[string]*Batch{}
	for k, v := range b.batches {
		ret[k] = v
	}
	return ret
}

// Prefix is used for "public" parts of the Batcher redis keyspace
func (b *Batcher) Prefix() string {
	return b.config.BatcherRedisPrefix
}

// MetaPrefix is used for "private" parts of the Batcher redis keyspace
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
	if val != b.uuid {
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
	uuid := strconv.Itoa(int(crc32.ChecksumIEEE([]byte(strconv.Itoa(int(time.Now().UnixNano()))))))
	shouldReap := func(val redis.XPendingExt) bool {
		return val.Idle > time.Hour*48 || val.RetryCount > 20
	}
	b := &Batcher{
		redisClient: client,
		uuid:        uuid,
		reaper:      "reaper",
		shouldReap:  shouldReap,
	}

	// ensure no other batcher instance running
	err := b.renewLock()
	if err != nil {
		panic(err)
	}

	return b
}

func (b *Batcher) Consumer() redistream.Consumer {
	return redistream.Consumer{
		Group: "batcher",
		Name:  b.uuid,
	}
}

func (b *Batcher) ReaperConsumer() redistream.Consumer {
	return redistream.Consumer{
		Group: "batcher",
		Name:  b.reaper,
	}
}

type batchSignal uint16

const (
	quit batchSignal = iota
	pause
	resume
)
