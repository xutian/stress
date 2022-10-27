package main

import (
	"bytes"
	"flag"
	"fmt"
	"mpp-stress/utils"
	"time"

	//"os"
	//"runtime/pprof"
	"sync"

	"github.com/panjf2000/ants"
	log "github.com/sirupsen/logrus"
)

func Init() {
	log.SetLevel(log.DebugLevel)
}

func loadStress(conf *utils.Config, topic string, out *chan *utils.Statistician) {
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
		go utils.PushMessage(&pipes, conf.MessageSize)
		wgIn.Done()
	}

	var handler interface{}
	if conf.MethodId == 1 {
		handler = utils.NewHttpHandler(topic, conf)
	} else {
		handler = utils.NewKafkaHandler(topic, conf)
		defer handler.(*utils.KafkaHandler).Writer.Close()
	}

	// New pool for send data
	poolOut, o_err := ants.NewPoolWithFunc(
		int(conf.Threads), func(i interface{}) {
			utils.SendMessage(&pipes, handler, out)
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

	chanStatis := make(chan *utils.Statistician, len(conf.Topics))
	reports := make(map[string]*utils.Report)

	// collect statis records, calculate and print summary

	for _, topic := range conf.Topics {
		wg.Add(1)
		reports[topic] = utils.NewReport(topic, conf)
		go func(topic string) {
			loadStress(conf, topic, &chanStatis)
			wg.Done()
			reports[topic].EndTime = time.Now()
		}(topic)
	}
	wg.Wait()

	for data := range chanStatis {
		topic := data.Topic
		report := reports[topic]
		if data.State {
			report.SuccessfulRows += int64(report.MessageSize)
			report.TotalSentBytes += data.SentBytes
			report.TotalSentTime += data.SentTime
		} else {
			report.FailedRows += int64(report.MessageSize)
		}
	}
	for _, v := range reports {
		v.Print()
	}

}
