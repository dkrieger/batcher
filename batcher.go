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
	"github.com/go-redis/redis"
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
