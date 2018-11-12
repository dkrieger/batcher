package batcher

import (
	"encoding/json"
	"github.com/dkrieger/redistream"
	"github.com/go-redis/redis"
	"log"
	"os"
	"time"
)

// SendBatch aggregates up to BatchConfig.MaxSize entries into one entry,
// adding to Batcher.batchDest() stream
func (b *Batcher) SendBatch(name string) error {
	batches := b.getBatches()
	batch := batches[name]
	// make sure this is the only goroutine consuming this stream
	batch.consumerMutex.Lock()
	defer batch.consumerMutex.Unlock()
	conf := redistream.Config{
		MaxLenApprox: 1000,
		Block:        time.Second,
		Count:        int64(batch.config.MaxSize),
	}
	streamClient := redistream.WrapClient(b.redisClient, conf)
	fullName := b.StreamPrefix() + name
	old, err := streamClient.Consume(redistream.ConsumeArgs{
		Consumer: b.Consumer(),
		Streams:  []string{fullName, "0"},
	})
	if err != nil && err != redis.Nil {
		return err
	}
	oldStream, err := old.Merge(nil)
	if err != nil {
		return err
	}
	stderr := log.New(os.Stderr, "", 0)
	oldStream, err = b.ReapSome(oldStream, fullName)
	if err != nil {
		stderr.Printf("ReapSome() error: \nentries: %#v\nname: %s\n%s\n",
			oldStream, fullName, err)
	}
	override := conf
	override.Count = int64(batch.config.MaxSize - len(oldStream))
	new, err := streamClient.Consume(redistream.ConsumeArgs{
		Consumer: b.Consumer(),
		Streams:  []string{fullName, ">"},
		Override: &override,
	})
	if err != nil && err != redis.Nil {
		return err
	}
	newStream, err := new.Merge(nil)
	if err != nil {
		return err
	}
	newStream, err = b.ReapSome(newStream, fullName)
	if err != nil {
		stderr.Printf("ReapSome() error: \nentries: %#v\nname: %s\n%s\n",
			newStream, fullName, err)
	}
	stream := append(oldStream, newStream...)
	if len(stream) == 0 {
		return nil
	}
	aggregated, err := b.AggregateBatch(stream)
	if err != nil {
		return err
	}
	dest := []redistream.Entry{{
		Meta: &redistream.EntryMeta{Stream: b.BatchDest()},
		Hash: map[string]interface{}{
			"batch":  aggregated,
			"outlet": name,
		},
	}}
	_, _, err = streamClient.Process(redistream.ProcessArgs{
		From: stream, To: dest})
	return err
}

func (b *Batcher) AggregateBatch(entries []redistream.Entry) (string, error) {
	agg := []map[string]interface{}{}
	for _, e := range entries {
		agg = append(agg, e.Hash)
	}
	json, err := json.Marshal(agg)
	if err != nil {
		return "", err
	}
	return string(json), nil
}
