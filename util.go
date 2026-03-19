package main

import (
	"math/rand"
	"time"
)

func sleep(minMs, maxMs int) {
	d := time.Duration(minMs+rand.Intn(maxMs-minMs)) * time.Millisecond
	time.Sleep(d)
}
