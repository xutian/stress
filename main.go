package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"mpp-stress/utils"
	"os"
	"sync"
	"time"

	"github.com/panjf2000/ants"
	log "github.com/sirupsen/logrus"
)

var conf *utils.Config
var logLevel int

func init() {
	parserOpts()
	dateStr := time.Now().Format("2006-01-02-15-04-05")
	logfile := fmt.Sprintf("stress-%s", dateStr)
	log.SetFormatter(&log.TextFormatter{})
	file, err := os.OpenFile(logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("Create log file %s with error %v", logfile, err)
	}
	writers := []io.Writer{file, os.Stdout}
	fileAndStdoutWriter := io.MultiWriter(writers...)
	log.SetOutput(fileAndStdoutWriter)
	switch logLevel {
	case 7:
		log.SetLevel(log.TraceLevel)
	case 6:
		log.SetLevel(log.DebugLevel)
	case 5:
		log.SetLevel(log.InfoLevel)
	case 4:
		log.SetLevel(log.WarnLevel)
	case 2:
		log.SetLevel(log.ErrorLevel)
	case 1:
		log.SetLevel(log.FatalLevel)
	case 0:
		log.SetLevel(log.PanicLevel)
	}

}

func parserOpts() {
	var cfgPath string
	flag.StringVar(&cfgPath,
		"cfg",
		"./stress.toml",
		"conf file path, default is './stress.toml'")
	flag.IntVar(&logLevel, "loglevel", 5, "Set mini log level to print")
	flag.Parse()

	conf = utils.NewConfByFile(cfgPath)
	conf.Validate()
}

func dataProducer(conf *utils.Config, mp *map[string]*chan *bytes.Buffer, poolSize int) {
	//// create task pool to generate data and sent data to channel
	var wg sync.WaitGroup
	pool, err := ants.NewPoolWithFunc(
		poolSize,
		func(i interface{}) {
			for _, topic := range conf.Topics {
				pipe := (*mp)[topic]
				utils.PushMessage(pipe, conf.MessageSize)
			}
			wg.Done()
		})

	if err != nil {
		log.Fatalf("Create task pool for make data failed with error %v", err)
	}
	defer pool.Release()
	wg.Add(conf.MessageNum)
	log.Debugln("Begin to product data...")
	for i := 0; i < conf.MessageNum; i++ {
		pool.Invoke(i)
	}
	wg.Wait()
	for _, topic := range conf.Topics {
		ch := (*mp)[topic]
		close(*ch)
	}
	log.Debugln("Put data to channel done!")
}

func dataConsumer(conf *utils.Config, topic string, out *chan *utils.Statistician, pipe *chan *bytes.Buffer, poolSize int) {

	var wgOut sync.WaitGroup
	var handler interface{}

	if conf.MethodId == 1 {
		handler = utils.NewHttpHandler(topic, conf)
	} else {
		handler = utils.NewKafkaHandler(topic, conf)
		defer handler.(*utils.KafkaHandler).Writer.Close()
	}

	// New pool for send data
	poolOut, o_err := ants.NewPoolWithFunc(
		poolSize,
		func(i interface{}) {
			utils.SendMessage(pipe, handler, out)
			wgOut.Done()
		})
	if o_err != nil {
		panic(o_err)
	}
	defer poolOut.Release()

	wgOut.Add(conf.MessageNum)
	log.Debugln("Begin to consum data...")
	for i := 0; i < conf.MessageNum; i++ {
		poolOut.Invoke(i)
	}
	wgOut.Wait()
	log.Debugln("Sent data Done!")
}

func main() {

	var wg sync.WaitGroup
	var wgReport sync.WaitGroup

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

	log.Println(fmt.Sprintf("PoolSize: %v", conf.Threads))
	topicNum := len(conf.Topics)
	consumerPoolSize := conf.Threads
	producerPoolSize := consumerPoolSize * 2
	//capStatisChan := conf.MessageNum
	capStatisChan := conf.Threads

	// make chan to recive statis records
	chanStatis := make(chan *utils.Statistician, capStatisChan)

	// make to save report for each topic
	reports := make(map[string]*utils.Report)

	//map to keep channel ptr for each topic
	chanPipes := make(map[string]*chan *bytes.Buffer)

	wgReport.Add(topicNum)
	for _, topic := range conf.Topics {
		pipe := make(chan *bytes.Buffer, producerPoolSize+1)
		chanPipes[topic] = &pipe
		report := utils.NewReport(topic, conf, &chanStatis)
		reports[topic] = report
		go report.Calc(&wgReport)
	}

	go dataProducer(conf, &chanPipes, producerPoolSize)
	// collect statis records, calculate and print summary
	wg.Add(topicNum)
	for _, topic := range conf.Topics {
		go func(topic string) {
			var pipe = chanPipes[topic]
			dataConsumer(conf, topic, &chanStatis, pipe, consumerPoolSize)
			reports[topic].EndTime = time.Now()
			wg.Done()
			log.Debugf("Test done for topic %s!", topic)
		}(topic)
	}
	//wait every topic done
	wg.Wait()
	log.Debugln("Test done for all topics")
	//DO NOT REMOVE THIS LINE
	close(chanStatis)
	wgReport.Wait()
	for _, v := range reports {
		v.Print()
	}

}
