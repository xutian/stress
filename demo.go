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
	"strconv"
	"sync"
	"sync/atomic"

	//	"reflect"

	"time"

	kafka "github.com/segmentio/kafka-go"
	"gopkg.in/avro.v0"
)

func sendToKafka(w *kafka.Writer, buf []byte, ops *uint64) {
	msg := kafka.Message{
		Key:   []byte("1"),
		Value: buf,
	}
	err := w.WriteMessages(context.Background(), msg)
	fmt.Printf("send kafka: message size:%dB\n", len(buf))
	if err != nil {
		fmt.Println(err)
	} else {
		atomic.AddUint64(ops, 1)
	}
}

// func getIpv6Date() []byte {
// 	str := "1111111111111111"
// 	ipv6 := []byte(str)
// 	// fmt.Printf("%v", ipv6)
// 	return ipv6
// }

func writeToBuffer(record *avro.GenericRecord, avrowriter *avro.GenericDatumWriter, thread int, threadChans []chan []byte) {
	buffer := new(bytes.Buffer)
	encoder := avro.NewBinaryEncoder(buffer)
	//累计1M发送到channel
	//for len(buffer.Bytes()) <= 1*1000*1000 {
	for count := 0; count < recordnum; count++ {
		record.Set("c_netnum", int32(count))
		record.Set("c_log_time", time.Now().UnixNano()/1e6)
		record.Set("c_src_ipv4", int64(ipdata))
		record.Set("c_dest_ipv4", int64(ipdata))
		record.Set("c_ip", int64(ipdata))
		record.Set("c_s_tunnel_ip", int64(ipdata))
		record.Set("c_d_tunnel_ip", int64(ipdata))
		record.Set("c_src_ipv6", ipv6)
		record.Set("c_dest_ipv6", ipv6)
		err := avrowriter.Write(record, encoder)
		if err != nil {
			panic(err)
		}
		if count == 0 {
			prebuffer = len(buffer.Bytes())
		}
	}
	//fmt.Printf("thread-%d : buffer size:%dB\n", thread, prebuffer)
	threadChans[thread] <- buffer.Bytes()
}

var (
	sndnum       int
	threadsnum   int
	recordnum    int
	topic        string
	runtostop    int
	flow         bool
	flowinterval int
	ipdata       = binary.BigEndian.Uint32(net.ParseIP("98.138.253.109")[12:16])
	ipv6         = []byte("1111111111111111")
	prebuffer    int
)

func init() {
	flag.StringVar(&topic, "topic", "mpp_bus_pro", "topic")
	flag.IntVar(&recordnum, "r", 10, "每条消息行数")
	flag.IntVar(&threadsnum, "t", 1, "线程数")
	flag.IntVar(&sndnum, "n", 20, "总消息数")
	flag.IntVar(&runtostop, "T", 0, "按时长跑:单位min")
	flag.BoolVar(&flow, "flow", false, "是否流量控制,开启后,同时设置令牌间隔时间")
	flag.IntVar(&flowinterval, "ft", 0, "令牌间隔时间:单位ms")
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

	schema := avro.MustParseSchema(schema.SchemaPro)

	avrowriter := avro.NewGenericDatumWriter()
	avrowriter.SetSchema(schema)
	w := kafka.NewWriter(kafka.WriterConfig{

		Brokers: []string{"localhost:9094"},

		Topic: topic,

		Balancer: &kafka.RoundRobin{},

		BatchBytes: 2 * 1024 * 1024,

		Async: true,
	})

	var ops uint64 = 0

	runtimeStart := time.Now().UnixMilli()
	runtimeStop := runtimeStart + int64(runtostop*3600*1000)
	for t := 0; t < threadsnum; t++ {
		record := avro.NewGenericRecord(schema)
		if runtostop > 0 {
			if sndnum > 0 {
				fmt.Println("总时长和总发送数量不能同时开启")
				os.Exit(0)
			}
			go func(timeStop int64, thread int) {
				for time.Now().UnixMilli() <= timeStop {
					writeToBuffer(record, avrowriter, thread, threadChans)
				}
				close(threadChans[thread])
			}(runtimeStop, t)

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
			if flowinterval > 0 {
				for {
					tokenChan <- 1
					fmt.Printf("\n+++++ token +++++\n")
					time.Sleep(time.Duration(flowinterval) * time.Millisecond)
				}

			} else {
				tokenChan <- 0
			}
		} else {
			tokenChan <- 0
		}
	}(flow)

	timeStart := time.Now().UnixMilli()
	for t := 0; t < threadsnum; t++ {
		go func(thread int) {
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
						fmt.Printf("---\tthread-%d : read msg from buf %dB\t", thread, len(buf))
						sendToKafka(w, buf, &ops)
					} else {
						fmt.Println("流量控制时长不能为0")
						os.Exit(0)
					}
				} else {
					fmt.Printf("---\tthread-%d : read msg from buf %dB\t", thread, len(buf))
					sendToKafka(w, buf, &ops)
				}
			}
		}(t)

	}
	waitSignal.Wait()
	w.Close()

	//结果数据打印
	original := uint64(sndnum * recordnum)
	timeEnd := time.Now().UnixMilli()
	totalSnd := atomic.LoadUint64(&ops) * uint64(recordnum)
	var errorsnd uint64
	if sndnum == 0 {
		errorsnd = 0
	} else {
		errorsnd = original - totalSnd
	}
	totalByte := totalSnd * uint64(prebuffer) / 1024 / 1024
	ioRate, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(totalByte)/float64((timeEnd-timeStart)/1000)), 64)
	fmt.Printf("\n======end======\nOriginal Send:%d\nActual Send:%d\nError Send:%d\nVolume:%dMB\nTime:%dms\nI/O:%.2fM/s", original, totalSnd, errorsnd, totalByte, (timeEnd - timeStart), ioRate)
}
