package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"main/schema"
	"net"
	"os"
	"sync"
	"sync/atomic"

	//	"reflect"

	"time"

	kafka "github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"gopkg.in/avro.v0"
)

func sendToKafka(w *kafka.Writer, buf []byte, ops *uint64, errops *uint64) {
	msg := kafka.Message{
		Key:   []byte("1"),
		Value: buf,
	}
	err := w.WriteMessages(context.Background(), msg)
	log.Infof("send kafka: message size:%dB\n", len(buf))
	if err != nil {
		atomic.AddUint64(errops, 1)
		fmt.Println(err)
	} else {
		atomic.AddUint64(ops, 1)
	}
}

func writeToBuffer(record *avro.GenericRecord, avrowriter *avro.GenericDatumWriter, thread int, threadChans []chan []byte) {
	buffer := new(bytes.Buffer)
	encoder := avro.NewBinaryEncoder(buffer)
	//累计1M发送到channel
	//for len(buffer.Bytes()) <= 1*1000*1000 {
	for count := 0; count < recordnum; count++ {
		record.Set("c_netnum", int32(count))
		switch schemaname {
		case 1:
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
	//切换schema，用于300B、500B等多schema的切换
	schemaname int
)

func init() {
	flag.StringVar(&topic, "topic", "", "topic")
	flag.IntVar(&recordnum, "r", 1, "每条消息行数")
	flag.IntVar(&threadsnum, "t", 1, "线程数")
	flag.IntVar(&sndnum, "n", 1, "总消息数")
	flag.Float64Var(&runtostop, "T", 0, "按时长跑:单位min")
	flag.BoolVar(&flow, "flow", false, "是否流量控制,开启后,同时设置令牌间隔时间")
	flag.IntVar(&flowinterval, "ft", 0, "令牌间隔时间:单位ms")
	flag.IntVar(&schemaname, "s", 2, "选择schema:1--对应0927的schema;2--对应pro的schema")
}

func main() {
	flag.Parse()
	waitSignal := sync.WaitGroup{}
	waitSignal.Add(threadsnum)
	//线程对应channel数
	threadChans := make([]chan []byte, threadsnum)
	for n := 0; n < threadsnum; n++ {
		threadChans[n] = make(chan []byte, 512)
	}
	//单个线程发送数量
	var unitsnd int = sndnum / threadsnum

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

		Brokers: []string{"172.16.0.70:9094"},

		Topic: topic,

		Balancer: &kafka.RoundRobin{},

		BatchBytes: 2 * 1024 * 1024,

		Async: true,
	})

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
		} else {
			log.Infof("总时长和总发送数量不能同时开启")
			os.Exit(0)
		}

	}

	tokenChan := make(chan int, 1)
	go func(flow bool) {
		if flow {
			if flowinterval > 0 {
				for {
					tokenChan <- 1
					log.Infof("\n+++++ token +++++\n")
					time.Sleep(time.Duration(flowinterval) * time.Millisecond)
				}

			} else {
				tokenChan <- 0
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
							sendToKafka(w, buf, &ops, &errops)
						} else {
							log.Info("流量控制时长不能为0")
							os.Exit(0)
						}
					} else {
						log.Infof("---\tthread-%d : read msg from buf %dB\t", thread, len(buf))
						sendToKafka(w, buf, &ops, &errops)
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
							sendToKafka(w, buf, &ops, &errops)
						} else {
							log.Info("流量控制时长不能为0")
							os.Exit(0)
						}
					} else {
						log.Infof("---\tthread-%d : read msg from buf %dB\t", thread, len(buf))
						sendToKafka(w, buf, &ops, &errops)
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
