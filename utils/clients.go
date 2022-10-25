package utils

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	kafka "github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type Handler interface {
	Do(data *bytes.Buffer)
}

type HttpHandler struct {
	Cli    *http.Client
	Url    string
	Statis *Statistician
}

func Init() {
	log.SetLevel(log.DebugLevel)
}

func NewHttpHandler(eip string, topic string, statis *Statistician) *HttpHandler {
	return &HttpHandler{
		Cli:    &http.Client{},
		Url:    fmt.Sprintf("http://%s/dataload?topic=%s", eip, topic),
		Statis: statis,
	}
}

func (h *HttpHandler) Do(data *bytes.Buffer) error {

	reader := bytes.NewReader(data.Bytes())
	request, p_err := http.NewRequest("POST", h.Url, reader)
	if p_err != nil {
		log.Errorf("Packet http request with error, %v", p_err)
		return p_err
	}
	defer request.Body.Close()
	request.Header.Add("Connection", "keep-alive")
	request.Header.Add("User", "a")
	request.Header.Add("Password", "b")
	request.Header.Add("Content-Type", "application/avro")
	request.Header.Add("Transfer-Encoding", "chunked")
	response, s_err := h.Cli.Do(request)
	if s_err != nil {
		log.Errorf("Sent http request with error, %v", s_err)
		return s_err
	}
	h.Cli.CloseIdleConnections()
	defer response.Body.Close()
	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Debugln(string(content))
	}
	if response.StatusCode != http.StatusOK {
		h.Statis.IncreaseFailedNum()
		log.Errorf("Response code: %v", response.StatusCode)
		// print response body
	} else {
		lenData := len(data.Bytes())
		log.Infof("Sent data length: %v Bytes", lenData)
		h.Statis.IncreaseSuccessfulNum()
		h.Statis.IncreaseTotalBytes2Sent(uint64(lenData))
	}
	return nil
}

type KafkaHandler struct {
	Brokers []string
	Topic   string
	IsAsync bool
	Writer  *kafka.Writer
	Statis  *Statistician
}

func NewKafkaHandler(brokers []string, topic string, statis *Statistician) *KafkaHandler {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:    brokers,
		Topic:      topic,
		Balancer:   &kafka.RoundRobin{},
		BatchBytes: 30 * 1024 * 1024,
		Async:      true,
	})
	handler := KafkaHandler{
		Brokers: brokers,
		Topic:   topic,
		IsAsync: true,
		Writer:  writer,
		Statis:  statis,
	}
	return &handler
}

func (k *KafkaHandler) Do(data *bytes.Buffer) {
	dataBytes := data.Bytes()
	msg := kafka.Message{
		Key:   []byte("1"),
		Value: dataBytes,
	}
	if err := k.Writer.WriteMessages(context.Background(), msg); err != nil {
		log.Errorf("Sent messgae to kafka with errr, %v", err)
		k.Statis.IncreaseFailedNum()
	} else {
		k.Statis.IncreaseSuccessfulNum()
		lenData := len(dataBytes)
		log.Infof("Send kafka message size:%dB", lenData)
		k.Statis.IncreaseTotalBytes2Sent(uint64(lenData))
	}
}

func (k *KafkaHandler) Close() {
	k.Writer.Close()
}
