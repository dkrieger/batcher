package batcher

import (
	"encoding/json"
	"github.com/go-redis/redis"
	"strconv"
	"sync"
	"time"
)

// getBatches gets batch configs/metrics from redis.
func (b *Batcher) checkBatches() (*map[string]*Batch, error) {
	cli := b.redisClient

	// Batch (streams)
	cursor := -1
	batches := map[string]*Batch{}
	for cursor != 0 {
		if cursor == -1 {
			cursor = 0
		}
		page, curs, err := cli.Scan(uint64(cursor), b.StreamPrefix()+"*", 0).Result()
		if err != nil {
			return nil, err
		}
		for _, name := range page {
			batch := new(Batch)
			batch.config = *b.config.DefaultBatchConfig
			offset := len(b.StreamPrefix())
			batch.name = name[offset:]
			batches[batch.name] = batch
		}
		cursor = int(curs)
	}

	// BatchConfig
	batchConfigs := map[string]BatchConfig{}
	val, err := cli.HGetAll(b.ConfigsKey()).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	for name, config := range val {
		var cfg BatchConfig
		err := json.Unmarshal([]byte(config), cfg)
		if err != nil {
			return nil, err
		}
		batchConfigs[name] = cfg
	}

	// BatchMetrics
	batchMetrics := map[string]BatchMetrics{}
	val, err = cli.HGetAll(b.MetricsPrefix() + "lastSend").Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	for name, lastSend := range val {
		i, err := strconv.ParseInt(lastSend, 10, 64)
		if err != nil {
			return nil, err
		}
		batchMetrics[name] = BatchMetrics{
			LastSend: time.Unix(i, 0),
		}
	}
	val, err = cli.HGetAll(b.MetricsPrefix() + "entriesPerHour").Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	for name, eph := range val {
		bmn := batchMetrics[name]
		f, err := strconv.ParseFloat(eph, 64)
		if err != nil {
			return nil, err
		}
		bmn.EntriesPerHour = f
		batchMetrics[name] = bmn
	}

	// Fill in Configs and Metrics
	confExists := func(key string, configs map[string]BatchConfig) bool {
		for k, _ := range configs {
			if k == key {
				return true
			}
		}
		return false
	}
	metricsExists := func(key string, metricsMap map[string]BatchMetrics) bool {
		for k, _ := range metricsMap {
			if k == key {
				return true
			}
		}
		return false
	}
	for name, batch := range batches {
		if confExists(name, batchConfigs) {
			batch.config = batchConfigs[name]
		}
		if metricsExists(name, batchMetrics) {
			batch.metrics = batchMetrics[name]
		}
		batches[name] = batch
	}
	return &batches, nil
}

// syncBatches makes local batch config/metrics match redis batch
// config/metrics.
// TODO: This can't reasonably be run frequently enough for
// BatchMetrics.LastSend to be always accurate, so it makes sense to track it
// locally, and when syncBatches runs, preserve the local value if it is
// greater than the redis value.  in this way, we can remain (pseudo) lockless
// while having up-to-date LastSend info available in the respective
// ScheduleBatch goroutines
// TODO: run this on startup, listen on pubsub channel for configuration change
// (or just poll)
func (b *Batcher) syncBatches() error {
	tmp, err := b.checkBatches()
	if err != nil {
		return err
	}
	batches := *tmp
	localBatches := b.getBatches()
	keyExists := func(key string, batches map[string]*Batch) bool {
		for k, _ := range batches {
			if k == key {
				return true
			}
		}
		return false
	}
	sliceHas := func(value string, slice []string) bool {
		for _, v := range slice {
			if v == value {
				return true
			}
		}
		return false
	}

	// identify removed batches
	extra := []string{}
	for _, batch := range localBatches {
		if !keyExists(batch.name, batches) {
			extra = append(extra, batch.name)
		}
	}

	// identify new batches
	missing := []string{}
	for name, _ := range batches {
		if !keyExists(name, localBatches) {
			missing = append(missing, name)
		}
	}

	// transfer mutexes and signal chans
	for k, v := range localBatches {
		// transfer/init mutexes
		if sliceHas(k, extra) {
			go func() { v.signals <- quit; close(v.signals) }()
			continue
		}
		lmut := v.consumerMutex
		lsig := v.signals
		r := batches[k]
		r.consumerMutex = lmut
		r.signals = lsig
		batches[k] = r
	}
	for _, k := range missing {
		r := batches[k]
		mut := new(sync.Mutex)
		r.consumerMutex = mut
		r.signals = make(chan batchSignal)
	}

	// overwrite Batcher.batches map and schedule batches
	b.batches = batches
	for _, k := range missing {
		go b.ScheduleBatch(k, b.batches[k].signals)
	}

	return nil
}
