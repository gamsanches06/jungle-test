package app

import "time"

func waitRetry() <-chan time.Time { return time.After(time.Second) }
