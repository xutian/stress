package utils

import (
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
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
	Name              string
	StartTime         time.Time
	EndTime           time.Time
	TotalSentBytes    int64
	TotalSentTime     int64
	TotalSentRows     int64
	SuccessfulRows    int64
	FailedRows        int64
	MessageSize       int
	ThreadsNum        int
	SizePerSecond     float64
	RowPerSecond      float64
	SussfulRequests   int64
	FailedRequests    int64
	TotalRequestsSent int64
	ChanStatis        *chan *Statistician
}

func NewReport(name string, conf *Config, chanStatis *chan *Statistician) *Report {
	return &Report{
		Name:              name,
		StartTime:         time.Now(),
		EndTime:           time.Now(),
		TotalSentBytes:    0,
		TotalSentTime:     0,
		TotalSentRows:     0,
		SuccessfulRows:    0,
		FailedRows:        0,
		MessageSize:       conf.MessageSize,
		ThreadsNum:        conf.Threads,
		ChanStatis:        chanStatis,
		SizePerSecond:     0,
		RowPerSecond:      0,
		SussfulRequests:   0,
		FailedRequests:    0,
		TotalRequestsSent: 0,
	}
}

func Calc(ptrMapReport *map[string]*Report, ptrChanStatis *chan *Statistician) {
	chanStatis := *ptrChanStatis
	mapReports := *ptrMapReport
	for data := range chanStatis {
		topic := data.Topic
		report := mapReports[topic]
		if data.State {
			report.SuccessfulRows += int64(report.MessageSize)
			report.TotalSentBytes += data.SentBytes
			report.TotalSentTime += data.SentTime
			report.SussfulRequests += 1
		} else {
			report.FailedRows += int64(report.MessageSize)
			report.FailedRequests += 1
		}
		report.TotalRequestsSent = report.FailedRequests + report.SussfulRequests
		report.TotalSentRows = report.FailedRows + report.SuccessfulRows
		sentMib := float64(report.TotalSentBytes / (2 << 19))
		spentSeconds := float64(report.TotalSentTime/1000) + 0.001
		report.SizePerSecond = sentMib / spentSeconds
		report.RowPerSecond = float64(report.TotalSentRows) / spentSeconds
	}
}

func (r *Report) Print() {
	var tableContent [16]string
	tableContent[0] = fmt.Sprintf("==============Summary for Topic %s======================", r.Name)
	tableContent[1] = fmt.Sprintf("Start At: %v", r.StartTime)
	tableContent[2] = fmt.Sprintf("Threads: %d", r.ThreadsNum)
	tableContent[3] = fmt.Sprintf("SpentTime: %.3fs", float64(r.TotalSentTime/1000)+0.001)
	tableContent[4] = fmt.Sprintf("Transmit Rows: %d (Total transmit rows)", r.TotalSentRows)
	tableContent[5] = fmt.Sprintf("Transmit MiB: %.3f M", float64(r.TotalSentBytes/(2<<19)))
	tableContent[6] = fmt.Sprintf("Transmit Failed Rows: %d", r.FailedRows)
	tableContent[7] = fmt.Sprintf("Transmit Successful Rows: %d", r.SuccessfulRows)
	tableContent[8] = fmt.Sprintf("Transmission Rate1: %.3f M/s (MiB per seconds)", r.SizePerSecond)
	tableContent[9] = fmt.Sprintf("Transmission Rate2: %.3f R/s (Rows per seconds)", r.RowPerSecond)
	tableContent[10] = fmt.Sprintf("Total Requests Sent: %d", r.TotalRequestsSent)
	tableContent[11] = fmt.Sprintf("Failed Requests: %d", r.FailedRequests)
	tableContent[12] = fmt.Sprintf("Successful Requests: %d", r.SussfulRequests)
	tableContent[13] = fmt.Sprintf("Packet Message Size: %d R/P (Rows per packet message)", r.MessageSize)
	tableContent[14] = fmt.Sprintf("ElapsedTime: %.3f s", r.EndTime.Sub(r.StartTime).Seconds())
	tableContent[15] = fmt.Sprintf("StopTime: %v", r.EndTime)
	for i := 0; i < len(tableContent); i++ {
		log.Infoln(tableContent[i])
	}
	tableStr := strings.Join(tableContent[:], "\n")
	fmt.Println(tableStr)

}

func PrintSummary4Topics(ptrReports *map[string]*Report) {
	reports := *ptrReports
	for _, report := range reports {
		report.Print()
	}
}
