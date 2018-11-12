package main

import (
	. "github.com/dkrieger/batcher/lib"
	"github.com/go-redis/redis"
	"gopkg.in/urfave/cli.v1"
	"gopkg.in/urfave/cli.v1/altsrc"
	"os"
	"runtime"
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
		altsrc.NewUintFlag(cli.UintFlag{
			Name:  "batcher.concurrency",
			Value: 1,
			Usage: "define how many batching goroutines can run" +
				" at once. this sets the size of the lock" +
				" pool.",
		}),
		altsrc.NewUintFlag(cli.UintFlag{
			Name:  "batcher.minDelaySeconds",
			Value: 0,
			Usage: "min time a batching goroutine should retain" +
				" its lock before releasing back to the lock" +
				" pool.",
		}),
		altsrc.NewUintFlag(cli.UintFlag{
			Name:  "batcher.maxDelaySeconds",
			Value: 0,
			Usage: "max time a batching goroutine should retain" +
				" its lock before releasing back to the lock" +
				" pool.",
		}),
	}

	app.Before = func(c *cli.Context) error {
		if _, err := os.Stat(c.String("config")); os.IsNotExist(err) {
			return nil
		}
		_, err := altsrc.NewYamlSourceFromFlagFunc("config")(c)
		return err
	}
	// app.Before = altsrc.InitInputSourceWithContext(app.Flags, altsrc.NewYamlSourceFromFlagFunc("config"))
	app.Action = initBatcher
	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
	// infinite loop
	for {
		runtime.Gosched()
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
		Concurrency:     c.Uint("batcher.concurrency"),
		MinDelaySeconds: c.Uint("batcher.minDelaySeconds"),
		MaxDelaySeconds: c.Uint("batcher.maxDelaySeconds"),
	})
	if err != nil {
		return err
	}
	return nil
}
