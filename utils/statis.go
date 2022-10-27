package utils

import (
	"fmt"
	"time"
)

type Statistician struct {
	Topic     string
	SentTime  int64
	SentBytes int64
	State     bool // is Reqeust response Ok
}

func NewStatistician(topic string) *Statistician {

	return &Statistician{
		Topic:     topic,
		SentTime:  0,
		SentBytes: 0,
		State:     false,
	}
}

type Report struct {
	Name           string
	StartTime      time.Time
	EndTime        time.Time
	TotalSentBytes int64
	TotalSentTime  int64
	TotalSentRows  int64
	SuccessfulRows int64
	FailedRows     int64
	MessageSize    int
	ThreadsNum     int
}

func NewReport(name string, conf *Config) *Report {
	return &Report{
		Name:           name,
		StartTime:      time.Now(),
		EndTime:        time.Now(),
		TotalSentBytes: 0,
		TotalSentTime:  0,
		TotalSentRows:  0,
		SuccessfulRows: 0,
		FailedRows:     0,
		MessageSize:    conf.MessageSize,
		ThreadsNum:     conf.Threads,
	}
}

func (r *Report) Print() {
	fmt.Printf("==============Summary for Topic %s======================\n", r.Name)
	fmt.Printf("Start At: %v \n", r.StartTime)
	fmt.Printf("Threads: %d \n", r.ThreadsNum)
	fmt.Printf("SpentTime: %.3f s\n", float64(r.TotalSentTime/1000))

	fmt.Printf("TotalRows: %d \n", r.TotalSentRows)
	fmt.Printf("FailedRows: %d \n", r.FailedRows)
	fmt.Printf("SuccessfulRows: %d \n", r.SuccessfulRows)
	fmt.Printf("RowsPerSeconds: %.3f \n", float64(r.TotalSentRows/r.TotalSentTime))
	fmt.Printf("RowsPerReqeust: %d \n", r.MessageSize)

	fmt.Printf("SentSize: %.3f M \n", float64(r.TotalSentBytes/(2<<19)))
	fmt.Printf("MbytesPerSeonds: %.3f \n", float64(r.TotalSentBytes/r.TotalSentTime)/(2<<19))
	fmt.Printf("ElapsedTime: %.3f s \n", r.EndTime.Sub(r.StartTime).Seconds())
	fmt.Printf("StopTime: %v \n", r.EndTime)
}
