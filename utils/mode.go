package utils

import (
	"bytes"
	//"context"
	"sync"

	"github.com/panjf2000/ants"
	log "github.com/sirupsen/logrus"
	//"golang.org/x/time/rate"
)

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
