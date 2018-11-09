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
	old, err := streamClient.Consume(redistream.ConsumeArgs{
		Consumer: b.Consumer(),
		Streams:  []string{name, "0"},
	})
	if err != nil {
		return err
	}
	oldStream, err := old.Merge(nil)
	if err != nil {
		return err
	}
	stderr := log.New(os.Stderr, "", 0)
	oldStream, err = b.ReapSome(oldStream, name)
	if err != nil {
		stderr.Printf("ReapSome() error: \nentries: %#v\nname: %s\n%s\n",
			oldStream, name, err)
	}
	override := conf
	override.Count = int64(batch.config.MaxSize - len(oldStream))
	new, err := streamClient.Consume(redistream.ConsumeArgs{
		Consumer: b.Consumer(),
		Streams:  []string{name, ">"},
		Override: &override,
	})
	if err != nil {
		return err
	}
	newStream, err := new.Merge(nil)
	if err != nil {
		return err
	}
	newStream, err = b.ReapSome(newStream, name)
	if err != nil {
		stderr.Printf("ReapSome() error: \nentries: %#v\nname: %s\n%s\n",
			newStream, name, err)
	}
	stream := append(oldStream, newStream...)
	aggregated, err := b.AggregateBatch(stream)
	if err != nil {
		return err
	}
	dest := []redistream.Entry{{
		Meta: &redistream.EntryMeta{Stream: b.batchDest()},
		Hash: map[string]interface{}{"batch": aggregated},
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
