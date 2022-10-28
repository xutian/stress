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
		log.Fatalf("Create log file %s with error %v", logfile, err)
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
		//Start go routine to consume elements in channel chanStatis
		//to avoid process blocked after chanStatis is full
		go report.Calc(&wgReport)
	}

	go utils.DataProducer(conf, &chanPipes, producerPoolSize)
	// collect statis records, calculate and print summary
	wg.Add(topicNum)
	for _, topic := range conf.Topics {
		go func(topic string) {
			var pipe = chanPipes[topic]
			utils.DataConsumer(conf, topic, &chanStatis, pipe, consumerPoolSize)
			reports[topic].EndTime = time.Now()
			wg.Done()
			log.Debugf("Test done for topic %s!", topic)
		}(topic)
	}
	//Wait Consumer goroutine for all topic done
	wg.Wait()
	log.Debugln("Test done for all topics")
	//Close statis channel to finish Calc goroutine,
	//If not close the goroutine main goroutine blocked
	close(chanStatis)
	wgReport.Wait()
	for _, v := range reports {
		v.Print()
	}

}
