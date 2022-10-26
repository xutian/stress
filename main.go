package main

import (
	"bytes"
	"flag"
	"fmt"
	"mpp-stress/utils"

	//"os"
	//"runtime/pprof"
	"sync"

	"github.com/panjf2000/ants"
	log "github.com/sirupsen/logrus"
)

func Init() {
	log.SetLevel(log.DebugLevel)
}

func loadStress(conf *utils.Config, statis *utils.Statistician, topic string) {
	var (
		wgIn  sync.WaitGroup
		wgOut sync.WaitGroup
	)
	// Make multi-channel for select and write data
	var pipes [3]*chan *bytes.Buffer
	for i := 0; i < len(pipes); i++ {
		ch := make(chan *bytes.Buffer, 64)
		pipes[i] = &ch
	}

	wgIn.Add(int(conf.MessageNum))
	for i := 0; i < conf.MessageNum; i++ {
		go utils.PushMessage(&pipes, statis, conf.MessageSize)
		wgIn.Done()
	}

	var handler interface{}
	if conf.MethodId == 1 {
		handler = utils.NewHttpHandler(conf.Eip, topic, statis)
	} else {
		handler = utils.NewKafkaHandler(conf.Brokers, topic, statis)
		defer handler.(*utils.KafkaHandler).Writer.Close()
	}

	// New pool for send data
	poolOut, o_err := ants.NewPoolWithFunc(
		int(conf.Threads), func(i interface{}) {
			utils.SendMessage(&pipes, handler)
			wgOut.Done()
		})
	if o_err != nil {
		panic(o_err)
	}
	defer poolOut.Release()

	wgOut.Add(conf.MessageNum)
	for i := 0; i < conf.MessageNum; i++ {
		poolOut.Invoke(1)
	}
	wgIn.Wait()
	wgOut.Wait()
}

func main() {

	var cfgPath string
	var wg sync.WaitGroup

	//// Code for anysiszied CPU usage
	// fd, err := os.Create("cpu.prof")

	// if err != nil {
	// 	log.Fatalf("Create cpu.prof failed, %v", err)
	// }
	// defer fd.Close()

	// if err := pprof.StartCPUProfile(fd); err != nil {
	// 	log.Fatalln("Could not start cpu profile")
	// }
	// defer pprof.StopCPUProfile()

	flag.StringVar(&cfgPath,
		"cfg",
		"./stress.toml",
		"conf file path, default is './stress.toml'")
	flag.Parse()

	conf := utils.NewConfByFile(cfgPath)
	conf.Validate()
	log.Println(fmt.Sprintf("PoolSize: %v", conf.Threads))

	statisMap := make(map[string]*utils.Statistician, len(conf.Topics))
	for i := 0; i < len(conf.Topics); i++ {
		wg.Add(1)
		topic := conf.Topics[i]
		statis := utils.NewStatistician(uint64(conf.TotalMessageSize), uint64(conf.MessageSize))
		statisMap[topic] = statis
		go func(i int) {
			loadStress(conf, statis, topic)
			wg.Done()
		}(i)
	}
	wg.Wait()
	for k, v := range statisMap {
		fmt.Println("==================================")
		fmt.Printf("Statis report for topic %s \n", k)
		v.PrintReport()
	}
}
