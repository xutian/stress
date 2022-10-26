package utils

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Statistician struct {
	StartTime      time.Time
	EndTime        time.Time
	FailedNum      uint64
	SuccessfulNum  uint64
	TotalBytes2Sent uint64
	ActualTimeSpent  uint64
	OriginalSent uint64
}

func NewStatistician(totalMsgSize uint64) *Statistician {
	//recordNum := totalSize / SizePerRecord
	timeNow := time.Now()
	return &Statistician{
		StartTime:      timeNow,
		EndTime:        timeNow,
		FailedNum:      0,
		OriginalSent:   totalMsgSize,
		SuccessfulNum:  0,
		TotalBytes2Sent: 0,
		ActualTimeSpent:  0,
	}
}

func (s *Statistician) IncreaseFailedNum() {
	atomic.AddUint64(&(s.FailedNum), 1)
}

func (s *Statistician) IncreaseSuccessfulNum() {
	atomic.AddUint64(&(s.SuccessfulNum), 1)
}

func (s *Statistician) IncreaseTotalBytes2Sent(size uint64) {
	atomic.AddUint64(&(s.TotalBytes2Sent), size)
}

func (s *Statistician) IncreaseSpentTime(d uint64) {
	atomic.AddUint64(&(s.ActualTimeSpent), d)
}

func (s *Statistician) PrintReport() {
	s.EndTime = time.Now()
	fmt.Printf("StartTime: %v \n", s.StartTime)
	fmt.Printf("EndTime: %v \n", s.EndTime)
	fmt.Printf("Original Sent: %d \n", s.OriginalSent)
	fmt.Printf("Volume: %d \n", s.TotalBytes2Sent)
	fmt.Printf("Actual Sent: %d \n", s.SuccessfulNum)
	fmt.Printf("Failed Sent: %d \n", s.FailedNum)
	fmt.Printf("Spent Time: %v\n", s.EndTime.Sub(s.StartTime))
}
