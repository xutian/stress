package main

import (
	"bytes"
	"flag"
	"fmt"
	"mpp-stress/utils"
	"os"
	"runtime/pprof"
	"sync"

	"github.com/panjf2000/ants"
	log "github.com/sirupsen/logrus"
)

func Init() {
	log.SetLevel(log.DebugLevel)
}

func main() {
	var (
		wgIn  sync.WaitGroup
		wgOut sync.WaitGroup
	)
	var cfgPath string

	fd, err  := os.Create("cpu.prof")

	if err != nil {
		log.Fatalf("Create cpu.prof failed, %v", err)
	}
	defer fd.Close()

	if err := pprof.StartCPUProfile(fd); err != nil {
		log.Fatalln("Could not start cpu profile")
	}
	defer pprof.StopCPUProfile()

	flag.StringVar(&cfgPath,
		"conf",
		"./stress.toml",
		"conf file path, default is './stress.toml'")
	flag.Parse()

	conf := utils.NewConfByFile(cfgPath)
	conf.Validate()
	statis := utils.NewStatistician(uint64(conf.TotalRecords))

	poolSize := conf.Threads
	log.Println(fmt.Sprintf("PoolSize: %v", poolSize))

	// Make multi-channel for select and write data
	var pipes [3]*chan *bytes.Buffer
	for i := 0; i < len(pipes); i++ {
		ch := make(chan *bytes.Buffer, 64)
		pipes[i] = &ch
	}

	wgIn.Add(int(statis.MaxRecords))
	for i := 0; i < int(statis.MaxRecords); i++ {
		go utils.PushMessage(&pipes, statis, conf.Records)
		wgIn.Done()
	}

	var handler interface{}
	topic := conf.Topics[0]
	if conf.MethodId == 1 {
		handler = utils.NewHttpHandler(conf.Eip, topic, statis)
	} else {
		handler = utils.NewKafkaHandler(conf.Brokers, topic, statis)
		defer handler.(*utils.KafkaHandler).Writer.Close()
	}

	// New pool for send data
	poolOut, o_err := ants.NewPoolWithFunc(
		int(poolSize), func(i interface{}) {
			utils.SendMessage(&pipes, handler)
			wgOut.Done()
		})
	if o_err != nil {
		panic(o_err)
	}
	defer poolOut.Release()

	wgOut.Add(int(statis.MaxRecords))
	for i := 0; i < int(statis.MaxRecords); i++ {
		poolOut.Invoke(1)
	}
	wgOut.Wait()
	statis.PrintReport()
}
