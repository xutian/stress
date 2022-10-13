package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"main/schema"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"

	//	"reflect"

	"time"

	kafka "github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	viper "github.com/spf13/viper"
	"gopkg.in/avro.v0"
)

func send(usemethod int, w *kafka.Writer, buf []byte, ops *uint64, errops *uint64, client *http.Client) {
	if usemethod == 1 {
		url := "http://" + eip + "/dataload?topic=" + topic
		req, err := http.NewRequest("POST", url, bytes.NewReader(buf))
		if err != nil {
			panic(err)
		}
		req.Header.Add("Context-Type", "avro")
		req.Header.Add("Connection", "keep-alive")
		req.Header.Add("User", "a")
		req.Header.Add("Password", "b")
		req.Header.Add("Content-Type", "application/avro")
		req.Header.Add("Transfer-Encoding", "chunked")
		resp, err := client.Do(req)
		if err != nil {
			log.Errorf("send err:%v", err)
			return
		}
		defer req.Body.Close()
		resbody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("read resp err:%v", err)
			return
		}
		if resp.StatusCode != 200 {
			atomic.AddUint64(errops, 1)
			log.Info(string(resbody))
		} else {
			atomic.AddUint64(ops, 1)
			log.Info(string(resbody))
		}
	} else {
		msg := kafka.Message{
			Key:   []byte("1"),
			Value: buf,
		}
		err := w.WriteMessages(context.Background(), msg)
		log.Infof("send kafka: message size:%dB\n", len(buf))
		if err != nil {
			atomic.AddUint64(errops, 1)
			log.Error(err)
		} else {
			atomic.AddUint64(ops, 1)
		}
	}

}

func writeToBuffer(record *avro.GenericRecord, avrowriter *avro.GenericDatumWriter, thread int, threadChans []chan []byte) {
	buffer := new(bytes.Buffer)
	encoder := avro.NewBinaryEncoder(buffer)
	for count := 0; count < recordnum; count++ {

		switch schemaname {
		case 1:
			record.Set("c_netnum", int32(count))
			record.Set("c_time", time.Now().Unix())
		case 2:
			record.Set("c_log_time", time.Now().UnixMilli())
			record.Set("c_src_ipv4", int64(ipdata))
			record.Set("c_dest_ipv4", int64(ipdata))
			record.Set("c_ip", int64(ipdata))
			record.Set("c_s_tunnel_ip", int64(ipdata))
			record.Set("c_d_tunnel_ip", int64(ipdata))
			record.Set("c_src_ipv6", ipv6)
			record.Set("c_dest_ipv6", ipv6)
			record.Set("c_netnum", int32(count))
			record.Set("c_s_tunnel_port", int32(8080))
		}

		err := avrowriter.Write(record, encoder)
		if err != nil {
			panic(err)
		}
		if count == 0 {
			prebuffer = len(buffer.Bytes())
		}
	}
	//fmt.Printf("thread-%d : pre buffer size:%dB\n", thread, prebuffer)
	threadChans[thread] <- buffer.Bytes()
}

var (
	sndnum       int
	threadsnum   int
	recordnum    int
	topic        string
	runtostop    float64
	flow         bool
	flowinterval int
	ipdata       = binary.BigEndian.Uint32(net.ParseIP("98.138.253.109")[12:16])
	ipv6         = []byte("1111111111111111")
	prebuffer    int
	schemaname   int
	brokerips    []string
	usemethod    int
	eip          string
)

func initConf() {
	//调试用
	// work, _ := os.Getwd()
	// viper.SetConfigName("stress")
	// viper.SetConfigType("toml")
	// viper.AddConfigPath(work)

	if len(os.Args) >= 3 {
		if os.Args[1] == "-c" {
			cfgFile := os.Args[2]
			viper.SetConfigFile(cfgFile)
		}
	} else {
		log.Infof("请选择配置文件：-c xxxx.toml")
		os.Exit(0)
	}

	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println(err)
		panic("err")
	}

	topic = viper.GetString("required.topic")
	recordnum = viper.GetInt("required.recordnum")
	threadsnum = viper.GetInt("required.threadsnum")
	sndnum = viper.GetInt("required.sndnum")
	runtostop = viper.GetFloat64("required.runtostop")
	flow = viper.GetBool("optional.flow")
	flowinterval = viper.GetInt("optional.flowinterval")
	schemaname = viper.GetInt("required.schemaname")
	brokerips = viper.GetStringSlice("required.brokerips")
	usemethod = viper.GetInt("test.usemethod")
	eip = viper.GetString("required.eip")
	// brokerips := make([]string, len(viper.GetStringSlice("required.brokerips")))
	// for index, v := range viper.GetStringSlice("required.brokerips") {
	// 	brokerips[index] = v + ":9094"
	// }
	if topic == "" || schemaname <= 0 {
		log.Info("缺少必填项：topic or schema,请修改config")
		os.Exit(0)
	}
	if runtostop > 0 && sndnum > 0 {
		log.Info("总时长和总发送数量sndnum不能同时大于0,请修改config")
		os.Exit(0)
	}
	if flow && flowinterval <= 0 {
		log.Info("流量控制时长flowinterval必须大于0,请修改config")
		os.Exit(0)
	}
	if sndnum > 0 {
		if threadsnum > sndnum {
			threadsnum = sndnum
		}
		if sndnum%threadsnum != 0 {
			log.Info("本版本仅支持sndnum是threadsnum的倍数,请修改config")
			os.Exit(0)
		}
	}
}

func main() {
	initConf()

	waitSignal := sync.WaitGroup{}
	waitSignal.Add(threadsnum)
	//线程对应channel数
	threadChans := make([]chan []byte, threadsnum)
	for n := 0; n < threadsnum; n++ {
		threadChans[n] = make(chan []byte, 8)
	}

	var parseschema string
	switch schemaname {
	case 1:
		parseschema = schema.SchemarRaw
	case 2:
		parseschema = schema.SchemaPro
	}
	schema := avro.MustParseSchema(parseschema)
	avrowriter := avro.NewGenericDatumWriter()
	avrowriter.SetSchema(schema)
	w := kafka.NewWriter(kafka.WriterConfig{

		Brokers: brokerips,

		Topic: topic,

		Balancer: &kafka.RoundRobin{},

		BatchBytes: 2 * 1024 * 1024,

		Async: true,
	})

	// client := &http.Client{}
	client := &http.Client{
		//Timeout: 10 * time.Second,
	}
	//单个线程发送数量
	var unitsnd int
	if sndnum > 0 {
		unitsnd = sndnum / threadsnum
	}

	for t := 0; t < threadsnum; t++ {
		record := avro.NewGenericRecord(schema)
		if runtostop > 0 && sndnum == 0 {
			go func(thread int) {
				for {
					writeToBuffer(record, avrowriter, thread, threadChans)
				}
			}(t)
		} else if runtostop == 0 && sndnum > 0 {

			go func(num int, thread int) {
				for n := 0; n < num; n++ {
					writeToBuffer(record, avrowriter, thread, threadChans)
				}
				close(threadChans[thread])
			}(unitsnd, t)
		}
	}

	tokenChan := make(chan int, 1)
	go func(flow bool) {
		if flow {
			for {
				tokenChan <- 1
				log.Info("+++++ token +++++\n")
				time.Sleep(time.Duration(flowinterval) * time.Millisecond)
			}
		} else {
			tokenChan <- 0
		}
	}(flow)

	var ops uint64 = 0
	var errops uint64 = 0
	timeStart := time.Now().UnixMilli()
	runtimeStop := timeStart + int64(runtostop*60*1000)
	for t := 0; t < threadsnum; t++ {
		go func(thread int) {
			if runtostop > 0 && sndnum == 0 {
				for time.Now().UnixMilli() <= runtimeStop {
					buf := <-threadChans[thread]
					//发送的流量控制
					if flow {
						sendtoken := <-tokenChan
						if sendtoken == 1 {
							log.Infof("---\tthread-%d : read msg from buf %dB\t", thread, len(buf))
							send(usemethod, w, buf, &ops, &errops, client)
						}
					} else {
						log.Infof("---\tthread-%d : read msg from buf %dB\t", thread, len(buf))
						send(usemethod, w, buf, &ops, &errops, client)
					}
				}
				waitSignal.Done()
			} else if runtostop == 0 && sndnum > 0 {
				for {
					buf, ok := <-threadChans[thread]
					if !ok {
						waitSignal.Done()
						break
					}
					//发送的流量控制
					if flow {
						sendtoken := <-tokenChan
						if sendtoken == 1 {
							log.Infof("---\tthread-%d : read msg from buf %dB\t", thread, len(buf))
							send(usemethod, w, buf, &ops, &errops, client)
						}
					} else {
						log.Infof("---\tthread-%d : read msg from buf %dB\t", thread, len(buf))
						send(usemethod, w, buf, &ops, &errops, client)
					}
				}
			}
		}(t)
	}
	waitSignal.Wait()
	w.Close()

	//结果数据打印
	timeEnd := time.Now().UnixMilli()
	original := uint64(sndnum * recordnum)
	totalSnd := atomic.LoadUint64(&ops) * uint64(recordnum)
	errSnd := atomic.LoadUint64(&errops) * uint64(recordnum)
	totalByte := totalSnd * uint64(prebuffer) / 1024 / 1024
	ioRate := float64(totalByte) / (float64(timeEnd-timeStart) / 1000)
	log.Infof("\n======end======\nOriginal Send:%d\nActual Send:%d\nError Send:%d\nVolume:%dMB\nTime:%dms\nI/O:%.2fM/s", original, totalSnd, errSnd, totalByte, (timeEnd - timeStart), ioRate)
}
