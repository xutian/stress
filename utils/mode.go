package utils

import (
	"bytes"
	//"context"
	"sync"
	"time"

	"github.com/panjf2000/ants"
	log "github.com/sirupsen/logrus"
	//"golang.org/x/time/rate"
)

func Consumer4Topics(conf *Config, ptrChanStatis *chan *Statistician, ptrMapChanPipes *map[string]*chan *bytes.Buffer, ptrMapReports *map[string]*Report, poolSize int) {
	var wg sync.WaitGroup

	topicNum := len(conf.Topics)
	mapChanPipes := *ptrMapChanPipes
	mapReports := *ptrMapReports
	chanStatis := *ptrChanStatis

	wg.Add(topicNum)
	for _, topic := range conf.Topics {
		go func(topic string) {
			var pipe = mapChanPipes[topic]
			DataConsumer(conf, topic, ptrChanStatis, pipe, poolSize)
			mapReports[topic].EndTime = time.Now()
			wg.Done()
			log.Debugf("Test done for topic %s!", topic)
		}(topic)
	}
	wg.Wait()
	//Wait Consumer goroutine for all topic done
	log.Debugln("Test done for all topics")
	//Close statis channel to finish Calc goroutine,
	//If not close the goroutine main goroutine blocked
	close(chanStatis)

}

func DataProducer(conf *Config, mp *map[string]*chan *bytes.Buffer, poolSize int) {
	//// create task pool to generate data and sent data to channel
	var wg sync.WaitGroup
	pool, err := ants.NewPoolWithFunc(
		poolSize,
		func(i interface{}) {
			for _, topic := range conf.Topics {
				pipe := (*mp)[topic]
				PushMessage(pipe, conf.MessageSize)
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

func DataConsumer(conf *Config, topic string, out *chan *Statistician, pipe *chan *bytes.Buffer, poolSize int) {

	var wgOut sync.WaitGroup

	//limiter := rate.NewLimiter(rate.Limit(conf.Interval), 1)
	//ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	//defer cancel()

	// New pool for send data
	poolOut, o_err := ants.NewPoolWithFunc(
		poolSize,
		func(i interface{}) {
			if conf.MethodId == 1 {
				handler := NewHttpHandler(topic, conf)
				SendMessage(pipe, handler, out)
			} else {
				handler := NewKafkaHandler(topic, conf)
				SendMessage(pipe, handler, out)
				handler.Close()
			}
			wgOut.Done()
		})
	if o_err != nil {
		panic(o_err)
	}
	defer poolOut.Release()

	wgOut.Add(conf.MessageNum)
	log.Debugln("Begin to consum data...")
	for i := 0; i < conf.MessageNum; i++ {
		//limiter.Wait(ctx)
		poolOut.Invoke(i)
	}
	wgOut.Wait()
	log.Debugln("Sent data Done!")
}
