package utils

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

type Statistician struct {
	StartTime      time.Time
	EndTime        time.Time
	MaxBytes       uint64
	FailNum        uint64
	SuccessfulNum  uint64
	SentRequest    uint64
	MaxRecords     uint64
	CurrentRecords uint64
	//TotalTime       time.Duration
	TotalBytes2Sent uint64
}

func NewStatistician(totalSize uint64) *Statistician {
	recordNum := totalSize / SizePerRecord
	timeNow := time.Now()
	return &Statistician{
		StartTime:      timeNow,
		EndTime:        timeNow,
		MaxBytes:       totalSize,
		FailNum:        0,
		SuccessfulNum:  0,
		SentRequest:    0,
		CurrentRecords: 0,
		MaxRecords:     uint64(math.Ceil(float64(recordNum))),
		//TotalTime: timeNow.Sub(timeNow),
		TotalBytes2Sent: 0,
	}
}

func (s *Statistician) IncreaseFailedNum() {
	atomic.AddUint64(&(s.FailNum), 1)
}

func (s *Statistician) IncreaseSuccessfulNum() {
	atomic.AddUint64(&(s.SuccessfulNum), 1)
}

func (s *Statistician) IncreaseSentRequest() {
	atomic.AddUint64(&(s.SentRequest), 1)
}

func (s *Statistician) IncreaseCurrentRecords() {
	atomic.AddUint64(&(s.CurrentRecords), 1)
}

func (s *Statistician) IncreaseTotalBytes2Sent(size uint64) {
	atomic.AddUint64(&(s.TotalBytes2Sent), size)
}

func (s *Statistician) PrintReport() {
	s.EndTime = time.Now()
	fmt.Printf("StartTime: %v \n", s.StartTime)
	fmt.Printf("EndTime: %v \n", s.EndTime)
	fmt.Printf("SizePerRecord: %d \n", SizePerRecord)
	fmt.Printf("Total Sent Bytes: %d \n", s.TotalBytes2Sent)
	fmt.Printf("Total Records: %d \n", s.SentRequest)
	fmt.Printf("Successful Records: %d \n", s.SuccessfulNum)
	fmt.Printf("Failed Records: %d \n", s.FailNum)
	fmt.Printf("Spent Time: %v\n", s.EndTime.Sub(s.StartTime))
}
