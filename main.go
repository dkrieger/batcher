package main

import (
	"fmt"
	. "github.com/dkrieger/batcher/lib"
	"github.com/go-redis/redis"
	"gopkg.in/urfave/cli.v1"
	"gopkg.in/urfave/cli.v1/altsrc"
	"os"
)

func main() {
	app := cli.NewApp()
	app.Name = "batcher"
	app.Usage = "Batch contents of various redis streams into one output" +
		" stream. Different source streams can have different batch schedules"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: "batcher.yaml",
			Usage: "Load configuration from YAML `FILE` (recommended over using CLI flags)",
		},
		altsrc.NewBoolFlag(cli.BoolFlag{
			Name:  "redis.unix, u",
			Usage: "use unix socket instead of tcp for redis instance backing Batcher",
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:  "redis.address, a",
			Value: "127.0.0.1:6379",
			Usage: "set address of redis instance backing Batcher",
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:  "batcher.shardKey, k",
			Value: "batcher",
			Usage: "set the redis shard key",
		}),
	}

	app.Before = altsrc.InitInputSourceWithContext(app.Flags, altsrc.NewYamlSourceFromFlagFunc("config"))
	app.Action = initBatcher
	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}

func initBatcher(c *cli.Context) error {
	network := "tcp"
	if c.Bool("redis.unix") {
		network = "unix"
	}
	_, err := NewBatcher(&BatcherConfig{
		RedisOpts: &redis.Options{
			Network: network,
			Addr:    c.String("redis.address"),
		},
		BatcherShardKey: c.String("batcher.shardKey"),
	})
	if err != nil {
		return err
	}
	return nil
}
