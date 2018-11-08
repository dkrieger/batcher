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
	"encoding/json"
	"github.com/dkrieger/redistream"
	"time"
)

// SendBatch aggregates up to BatchConfig.MaxSize entries into one entry,
// adding to Batcher.batchDest stream
func (b *Batcher) SendBatch(name string) {
	batchTmp, _ := b.batches.Load(name)
	batch := batchTmp.(Batch)
	// make sure this is the only goroutine consuming this stream
	batch.consumerMutex.Lock()
	defer batch.consumerMutex.Unlock()
	conf := redistream.Config{
		MaxLenApprox: 1000,
		Block:        5 * time.Second,
		Count:        int64(batch.config.MaxSize),
	}
	streamClient := redistream.WrapClient(b.redisClient, conf)
	old, err := streamClient.Consume(redistream.ConsumeArgs{
		Consumer: b.Consumer(),
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
		Consumer: b.Consumer(),
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
