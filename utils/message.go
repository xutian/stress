package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"log"
	"math/rand"
	"time"

	//"math/rand"
	"sync"

	"gopkg.in/avro.v0"
)

const SizePerRecord = 300

var DataSchema = `{
	"type":"record",
    "name":"mpp_bus_pro",
	"fields":[
		{
			"name":"c_netnum",
			"type":["null","int"]
		},
		{
			"name":"c_ip",
			"type":"long"
		},
		{
			"name":"c_flowid",
			"type":"string"
		},
		{
			"name":"c_src_ipv4",
			"type":"long"
		},
		{
			"name":"c_src_ipv6",
			"type":"bytes"
		},
		{
			"name":"c_src_port",
			"type":"int"
		},
		{
			"name":"c_s_tunnel_ip",
			"type":"long"
		},
		{
			"name":"c_s_tunnel_port",
			"type":["null","int"]
		},
		{
			"name":"c_dest_ipv4",
			"type":"long"
		},
		{
			"name":"c_dest_ipv6",
			"type":"bytes"
		},
		{
			"name":"c_dest_port",
			"type":"int"
		},
		{
			"name":"c_d_tunnel_ip",
			"type":"long"
		},
		{
			"name":"c_d_tunnel_port",
			"type":"int"
		},
		{
			"name":"c_packet_group",
			"type":"int"
		},
		{
			"name":"c_proto_type",
			"type":"int"
		},
		{
			"name":"c_connect_status",
			"type":"int"
		},
		{
			"name":"c_direct",
			"type":"int"
		},
		{
			"name":"c_server_dir",
			"type":"int"
		},
		{
			"name":"c_up_packets",
			"type":"long"
		},
		{
			"name":"c_up_bytes",
			"type":"long"
		},
		{
			"name":"c_down_packets",
			"type":"long"
		},
		{
			"name":"c_down_bytes",
			"type":"long"
		},
		{
			"name":"c_c2s_packet_jitter",
			"type":"int"
		},
		{
			"name":"c_s2c_packet_jitter",
			"type":"int"
		},
		{
			"name":"c_log_time",
			"type":"long"
		},
		{
			"name":"c_app_type",
			"type":"string"
		},
		{
			"name":"c_stream_time",
			"type":"long"
		},
		{
			"name":"c_hostr",
			"type":"string"
		},
		{
			"name":"c_s_boundary",
			"type":"long"
		},
		{
			"name":"c_s_region",
			"type":"long"
		},
		{
			"name":"c_s_city",
			"type":"long"
		},
		{
			"name":"c_s_district",
			"type":"long"
		},
		{
			"name":"c_s_operators",
			"type":"long"
		},
		{
			"name":"c_s_owner",
			"type":"string"
		},
		{
			"name":"c_d_boundary",
			"type":"long"
		},
		{
			"name":"c_d_region",
			"type":"long"
		},
		{
			"name":"c_d_city",
			"type":"long"
		},
		{
			"name":"c_d_district",
			"type":"long"
		},
		{
			"name":"c_d_operators",
			"type":"long"
		},
		{
			"name":"c_d_owner",
			"type":"string"
		},
		{
			"name":"c_s_mark1",
			"type":"long"
		},
		{
			"name":"c_s_mark2",
			"type":"long"
		},
		{
			"name":"c_s_mark3",
			"type":"long"
		},
		{
			"name":"c_s_mark4",
			"type":"long"
		},
		{
			"name":"c_s_mark5",
			"type":"long"
		},
		{
			"name":"c_d_mark1",
			"type":"long"
		},
		{
			"name":"c_d_mark2",
			"type":"long"
		},
		{
			"name":"c_d_mark3",
			"type":"long"
		},
		{
			"name":"c_d_mark4",
			"type":"long"
		},
		{
			"name":"c_d_mark5",
			"type":"long"
		}
	]
}`

type DataRow struct {
	C_netnum            int32  `avro:"c_netnum"`
	C_ip                int64  `avro:"c_ip"`
	C_flowid            string `avro:"c_flowid"`
	C_src_ipv4          int64  `avro:"c_src_ipv4"`
	C_src_ipv6          []byte `avro:"c_src_ipv6"`
	C_src_port          int32  `avro:"c_src_port"`
	C_s_tunnel_ip       int64  `avro:"c_s_tunnel_ip"`
	C_s_tunnel_port     int32  `avro:"c_s_tunnel_port"`
	C_dest_ipv4         int64  `avro:"c_dest_ipv4"`
	C_dest_ipv6         []byte `avro:"c_dest_ipv6"`
	C_dest_port         int32  `avro:"c_dest_port"`
	C_d_tunnel_ip       int64  `avro:"c_d_tunnel_ip"`
	C_d_tunnel_port     int32  `avro:"c_d_tunnel_port"`
	C_packet_group      int32  `avro:"c_packet_group"`
	C_proto_type        int32  `avro:"c_proto_type"`
	C_connect_status    int32  `avro:"c_connect_status"`
	C_direct            int32  `avro:"c_direct"`
	C_server_dir        int32  `avro:"c_server_dir"`
	C_up_packets        int64  `avro:"c_up_packets"`
	C_up_bytes          int64  `avro:"c_up_bytes"`
	C_down_packets      int64  `avro:"c_down_packets"`
	C_down_bytes        int64  `avro:"c_down_bytes"`
	C_c2s_packet_jitter int32  `avro:"c_c2s_packet_jitter"`
	C_s2c_packet_jitter int32  `avro:"c_s2c_packet_jitter"`
	C_log_time          int64  `avro:"c_log_time"`
	C_app_type          string `avro:"c_app_type"`
	C_stream_time       int64  `avro:"c_stream_time"`
	C_hostr             string `avro:"c_hostr"`
	C_s_boundary        int64  `avro:"c_s_boundary"`
	C_s_region          int64  `avro:"c_s_region"`
	C_s_city            int64  `avro:"c_s_city"`
	C_s_district        int64  `avro:"c_s_district"`
	C_s_operators       int64  `avro:"c_s_operators"`
	C_s_owner           string `avro:"c_s_owner"`
	C_d_boundary        int64  `avro:"c_d_boundary"`
	C_d_region          int64  `avro:"c_d_region"`
	C_d_city            int64  `avro:"c_d_city"`
	C_d_district        int64  `avro:"c_d_district"`
	C_d_operators       int64  `avro:"c_d_operators"`
	C_d_owner           string `avro:"c_d_owner"`
	C_s_mark1           int64  `avro:"c_s_mark1"`
	C_s_mark2           int64  `avro:"c_s_mark2"`
	C_s_mark3           int64  `avro:"c_s_mark3"`
	C_s_mark4           int64  `avro:"c_s_mark4"`
	C_s_mark5           int64  `avro:"c_s_mark5"`
	C_d_mark1           int64  `avro:"c_d_mark1"`
	C_d_mark2           int64  `avro:"c_d_mark2"`
	C_d_mark3           int64  `avro:"c_d_mark3"`
	C_d_mark4           int64  `avro:"c_d_mark4"`
	C_d_mark5           int64  `avro:"c_d_mark5"`
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func GetSchemaWriter(writer *avro.SpecificDatumWriter) {
	schema, err := avro.ParseSchema(DataSchema)
	if err != nil {
		log.Fatal(err)
	}
	writer.SetSchema(schema)
}

func RandStr(n int) string {
	//rand.Seed(time.Now().UnixNano())
	buf := make([]byte, n)
	rand.Read(buf)
	out := hex.EncodeToString(buf)
	return out
}

func RandIPv4() int64 {
	out := rand.New(rand.NewSource(time.Now().UnixNano())).Uint32()
	return int64(out)
}

func RandIPv6() []byte {
	ip := make([]byte, 8)
	out := rand.New(rand.NewSource(time.Now().UnixNano())).Uint64()
	binary.BigEndian.PutUint64(ip, out)
	return ip
}

func NewDataRow() *DataRow {
	buf := &DataRow{
		C_netnum:            int32(rand.Intn(256)),
		C_ip:                RandIPv4(),
		C_flowid:            RandStr(6),
		C_src_ipv4:          RandIPv4(),
		C_src_ipv6:          RandIPv6(),
		C_src_port:          rand.Int31n(65535),
		C_s_tunnel_ip:       RandIPv4(),
		C_s_tunnel_port:     rand.Int31n(65535),
		C_dest_ipv4:         RandIPv4(),
		C_dest_ipv6:         RandIPv6(),
		C_dest_port:         rand.Int31n(65535),
		C_d_tunnel_ip:       RandIPv4(),
		C_d_tunnel_port:     rand.Int31n(65535),
		C_packet_group:      rand.Int31n(512),
		C_proto_type:        rand.Int31n(256),
		C_connect_status:    rand.Int31n(128),
		C_direct:            rand.Int31n(512),
		C_server_dir:        rand.Int31n(512),
		C_up_packets:        rand.Int63n(9223372036854775807),
		C_up_bytes:          rand.Int63n(9223372036854775807),
		C_down_packets:      rand.Int63n(9223372036854775807),
		C_down_bytes:        rand.Int63n(9223372036854775807),
		C_c2s_packet_jitter: rand.Int31n(65535),
		C_s2c_packet_jitter: rand.Int31n(65535),
		C_log_time:          time.Now().UnixMilli(),
		C_app_type:          RandStr(6),
		C_stream_time:       time.Now().UnixMilli(),
		C_hostr:             RandStr(16),
		C_s_boundary:        rand.Int63n(128),
		C_s_region:          rand.Int63n(128),
		C_s_city:            rand.Int63n(128),
		C_s_district:        rand.Int63n(128),
		C_s_operators:       rand.Int63n(128),
		C_s_owner:           RandStr(4),
		C_d_boundary:        rand.Int63n(256),
		C_d_region:          rand.Int63n(128),
		C_d_city:            rand.Int63n(128),
		C_d_district:        rand.Int63(),
		C_d_operators:       rand.Int63n(128),
		C_d_owner:           RandStr(6),
		C_s_mark1:           rand.Int63n(64),
		C_s_mark2:           rand.Int63n(64),
		C_s_mark3:           rand.Int63n(64),
		C_s_mark4:           rand.Int63n(64),
		C_s_mark5:           rand.Int63n(64),
		C_d_mark1:           rand.Int63n(64),
		C_d_mark2:           rand.Int63n(64),
		C_d_mark3:           rand.Int63n(64),
		C_d_mark4:           rand.Int63n(64),
		C_d_mark5:           rand.Int63n(64),
	}
	//faker.FakeData(&buf)
	return buf
}


func PackMessage(bucketSize int) * bytes.Buffer {
	var msg  bytes.Buffer
	for i :=0; i < bucketSize; i++ {
		row := NewDataRow()
		buf := row.Dump2Avro()
		msg.Write(buf.Bytes())
	}
	return  &msg
}

func (m *DataRow) Dump2Avro() bytes.Buffer {
	var buf bytes.Buffer
	var once sync.Once
	writer := avro.NewSpecificDatumWriter()

	// Only create one writer
	once.Do(
		func() {
			GetSchemaWriter(writer)
		})
	encoder := avro.NewBinaryEncoder(&buf)
	if err := writer.Write(m, encoder); err != nil {
		log.Fatal(err)
	}
	return buf
}

func PushMessage(ptrPipe *[3]*chan *bytes.Buffer, statis *Statistician, bufSize int) {
	pipList := *ptrPipe
	msg := PackMessage(bufSize)
	select {
	case *pipList[0] <- msg:
		statis.IncreaseCurrentRecords()
	case *pipList[1] <- msg:
		statis.IncreaseCurrentRecords()
	case *pipList[2] <- msg:
		statis.IncreaseCurrentRecords()
	}
	
}

func sentByCli(b *bytes.Buffer, h interface{}) {
	switch t := h.(type) {
	case *HttpHandler:
		h.(*HttpHandler).Do(b)
	case *KafkaHandler:
		h.(*KafkaHandler).Do(b)
	default:
		log.Fatalf("Unknow handler type %T", t)
	}
	b.Reset()
}

func SendMessage(ptrPipe *[3]*chan *bytes.Buffer, h interface{}) {
	pipList := *ptrPipe
	
	select {

		case buf, ok := <- *pipList[0]:
			if ok {
				sentByCli(buf, h)
			}
		case buf, ok := <- *pipList[1]:
			if ok {
				sentByCli(buf, h)
			}
		case buf, ok := <- *pipList[2]:
			if ok {
				sentByCli(buf, h)
			}
		}
}
