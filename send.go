package batcher

func (b *Batcher) SendBatch(name string) {
	// read MaxSize items from batch stream, move to ready stream (which an
	// external consumer watches)
	batch, _ := b.batches.Load(name).(Batch)
	streamClient := redistreams.NewWrapper(b.redisClient)
	streams, err := streamClient.Consume("batcher", "batcher", batch.config.MaxSize, []string{name, ">"})
	if err != nil {
		panic(err)
	}
	stream := streams[0]
	aggregated := AggregateBatch(stream)
	streamClient.Produce(aggregated, b.batchDest)
}
func AggregateBatch(entries []*redistreams.Entry) *redistreams.Entry {
}
