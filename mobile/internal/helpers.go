package internal

import (
	"context"
	"time"
)

func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// SleepCh 定时通道（未使用，保留）
func SleepCh(sec int) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		time.Sleep(time.Duration(sec) * time.Second)
	}()
	return ch
}
