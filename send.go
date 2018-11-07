package batcher

import (
	"encoding/json"
	"github.com/dkrieger/redistream"
	"time"
)

func (b *Batcher) SendBatch(name string) {
	// read MaxSize items from batch stream, move to ready stream (which an
	// external consumer watches)
	batchTmp, _ := b.batches.Load(name)
	batch := batchTmp.(Batch)
	conf := redistream.Config{
		MaxLenApprox: 1000,
		Block:        5 * time.Second,
		Count:        int64(batch.config.MaxSize),
	}
	streamClient := redistream.WrapClient(b.redisClient, conf)
	old, err := streamClient.Consume(redistream.ConsumeArgs{
		Consumer: redistream.DefaultConsumer(),
		Streams:  []string{name, "0"},
	})
	if err != nil {
		panic(err)
	}
	oldStream, err := old.Merge(nil)
	if err != nil {
		panic(err)
	}
	override := conf
	override.Count = int64(batch.config.MaxSize - len(oldStream))
	new, err := streamClient.Consume(redistream.ConsumeArgs{
		Consumer: redistream.DefaultConsumer(),
		Streams:  []string{name, ">"},
		Override: &override,
	})
	if err != nil {
		panic(err)
	}
	newStream, err := new.Merge(nil)
	if err != nil {
		panic(err)
	}
	stream := append(oldStream, newStream...)
	_, _, err = b.AggregateBatch(stream, streamClient)
}
func (b *Batcher) AggregateBatch(entries []redistream.Entry, streamClient *redistream.Client) ([]redistream.XAckResult, []string, error) {
	agg := []map[string]interface{}{}
	for _, e := range entries {
		agg = append(agg, e.Hash)
	}
	json, err := json.Marshal(agg)
	if err != nil {
		panic(err)
	}
	out := []redistream.Entry{{
		Meta: &redistream.EntryMeta{Stream: b.batchDest},
		Hash: map[string]interface{}{"batch": string(json)}}}
	xackResults, ids, err := streamClient.Process(redistream.ProcessArgs{
		From: entries, To: out})
	return xackResults, ids, err
}
