// Copyright 2024 Alola Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// MIT License

// Copyright (c) 2018 Andy Pan

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package gopool

import (
	"context"
	"errors"
	"log"
	"math"
	"os"
	"runtime"
	"time"
)

const (
	// DefaultGoPoolSize is the default capacity for a default goroutine pool.
	DefaultGoPoolSize = math.MaxInt32

	// DefaultCleanIntervalTime is the interval time to clean up goroutines.
	DefaultCleanIntervalTime = time.Second
)

const (
	// OPENED represents that the pool is opened.
	OPENED = iota

	// CLOSED represents that the pool is closed.
	CLOSED
)

var (
	// ErrLackPoolFunc will be returned when invokers don't provide function for pool.
	ErrLackPoolFunc = errors.New("[gopool]: must provide function for pool")

	// ErrInvalidPoolExpiry will be returned when setting a negative number as the periodic duration to purge goroutines.
	ErrInvalidPoolExpiry = errors.New("invalid expiry for pool")

	// ErrPoolClosed will be returned when submitting task to a closed pool.
	ErrPoolClosed = errors.New("this pool has been closed")

	// ErrPoolOverload will be returned when the pool is full and no workers available.
	ErrPoolOverload = errors.New("too many goroutines blocked on submit or Nonblocking is set")

	// ErrInvalidPreAllocSize will be returned when trying to set up a negative capacity under PreAlloc mode.
	ErrInvalidPreAllocSize = errors.New("can not set up a negative capacity under PreAlloc mode")

	// ErrTimeout will be returned after the operations timed out.
	ErrTimeout = errors.New("operation timed out")

	// ErrInvalidPoolIndex will be returned when trying to retrieve a pool with an invalid index.
	ErrInvalidPoolIndex = errors.New("invalid pool index")

	// ErrInvalidLoadBalancingStrategy will be returned when trying to create a MultiPool with an invalid load-balancing strategy.
	ErrInvalidLoadBalancingStrategy = errors.New("invalid load-balancing strategy")

	// workerChanCap determines whether the channel of a worker should be a buffered channel
	// to get the best performance. Inspired by fasthttp at
	// https://github.com/valyala/fasthttp/blob/master/workerpool.go#L139
	workerChanCap = func() int {
		// Use blocking channel if GOMAXPROCS=1.
		// This switches context from sender to receiver immediately,
		// which results in higher performance (under go1.5 at least).
		if runtime.GOMAXPROCS(0) == 1 {
			return 0
		}

		// Use non-blocking workerChan if GOMAXPROCS>1,
		// since otherwise the sender might be dragged down if the receiver is CPU-bound.
		return 1
	}()

	// log.Lmsgprefix is not available in go1.13, just make an identical value for it.
	logLmsgprefix = 64
	defaultLogger = Logger(log.New(os.Stderr, "[gopool]: ", log.LstdFlags|logLmsgprefix|log.Lmicroseconds))

	// Init an instance pool when importing gopool.
	defaultGoPool, _ = NewPool(context.Background(), WithSize(DefaultGoPoolSize))
)

const nowTimeUpdateInterval = 500 * time.Millisecond

// Logger is used for logging formatted messages.
type Logger interface {
	// Printf must have the same semantics as log.Printf.
	Printf(format string, args ...interface{})
}

type (
	TaskFunc    func()
	TaskStream  chan TaskFunc
	InputParam  interface{}
	PoolFunc    func(InputParam)
	InputStream chan InputParam
	Nothing     struct{}
)

// const (
// 	releaseTimeoutInterval = 10
// )

// Submit submits a task to pool.
func Submit(ctx context.Context, task TaskFunc) error {
	return defaultGoPool.Submit(ctx, task)
}

// Running returns the number of the currently running goroutines.
func Running() int {
	return defaultGoPool.Running()
}

// Cap returns the capacity of this default pool.
func Cap() int {
	return defaultGoPool.Cap()
}

// Free returns the available goroutines to work.
func Free() int {
	return defaultGoPool.Free()
}

// Release Closes the default pool.
func Release(ctx context.Context) {
	defaultGoPool.Release(ctx)
}

// ReleaseTimeout is like Release but with a timeout, it waits all workers to exit before timing out.
func ReleaseTimeout(ctx context.Context, timeout time.Duration) error {
	return defaultGoPool.ReleaseTimeout(ctx, timeout)
}

// Reboot reboots the default pool.
func Reboot(ctx context.Context) {
	defaultGoPool.Reboot(ctx)
}
