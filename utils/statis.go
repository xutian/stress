package utils

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Statistician struct {
	StartTime       time.Time
	EndTime         time.Time
	FailedNum       uint64
	SuccessfulNum   uint64
	TotalBytes2Sent uint64
	ActualTimeSpent uint64
	OriginalSent    uint64
	MsgSize         uint64
}

func NewStatistician(totalMsgSize uint64, msgSize uint64) *Statistician {
	//recordNum := totalSize / SizePerRecord
	timeNow := time.Now()
	return &Statistician{
		StartTime:       timeNow,
		EndTime:         timeNow,
		FailedNum:       0,
		OriginalSent:    totalMsgSize,
		SuccessfulNum:   0,
		TotalBytes2Sent: 0,
		ActualTimeSpent: 0,
		MsgSize:         msgSize,
	}
}

func (s *Statistician) IncreaseFailedNum() {
	atomic.AddUint64(&(s.FailedNum), s.MsgSize)
}

func (s *Statistician) IncreaseSuccessfulNum() {
	atomic.AddUint64(&(s.SuccessfulNum), s.MsgSize)
}

func (s *Statistician) IncreaseTotalBytes2Sent(size uint64) {
	atomic.AddUint64(&(s.TotalBytes2Sent), size)
}

func (s *Statistician) IncreaseSpentTime(d uint64) {
	atomic.AddUint64(&(s.ActualTimeSpent), d)
}

func (s *Statistician) PrintReport() {
	s.EndTime = time.Now()
	spentSeconds := float64(s.ActualTimeSpent) / 1000
	mByteSent := float64(s.TotalBytes2Sent) / (1024 * 1024)
	fmt.Printf("StartTime: %v \n", s.StartTime)
	fmt.Printf("EndTime: %v \n", s.EndTime)
	fmt.Printf("Original Sent: %d \n", s.OriginalSent)
	fmt.Printf("Volume: %.3f Mb\n", mByteSent)
	fmt.Printf("Actual Sent: %d \n", s.SuccessfulNum)
	fmt.Printf("Failed Sent: %d \n", s.FailedNum)
	fmt.Printf("IOPS: %.3f MB/s\n", mByteSent/spentSeconds)
	fmt.Printf("Runing Time: %.2f Seconds\n", s.EndTime.Sub(s.StartTime).Seconds())
	fmt.Println("")
}
