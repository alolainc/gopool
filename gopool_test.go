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
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	_   = 1 << (10 * iota)
	KiB // 1024
	MiB // 1048576
)

const (
	Param    = 100
	GoPoolSize = 1000
	TestSize = 10000
	n        = 100000
)

var curMem uint64

// TestGoPoolWaitToGetWorker is used to test waiting to get worker.
func TestGoPoolWaitToGetWorker(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPool(ctx, WithSize(GoPoolSize))
	defer p.Release(ctx)

	for i := 0; i < n; i++ {
		wg.Add(1)
		_ = p.Submit(ctx, func() {
			demoPoolFunc(Param)
			wg.Done()
		})
	}
	wg.Wait()
	t.Logf("pool, running workers number:%d", p.Running())
	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	curMem = mem.TotalAlloc/MiB - curMem
	t.Logf("memory usage:%d MB", curMem)
}

func TestGoPoolWaitToGetWorkerPreMalloc(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPool(ctx, WithSize(GoPoolSize), WithPreAlloc(true))
	defer p.Release(ctx)

	for i := 0; i < n; i++ {
		wg.Add(1)
		_ = p.Submit(ctx, func() {
			demoPoolFunc(Param)
			wg.Done()
		})
	}
	wg.Wait()
	t.Logf("pool, running workers number:%d", p.Running())
	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	curMem = mem.TotalAlloc/MiB - curMem
	t.Logf("memory usage:%d MB", curMem)
}

// TestGoPoolWithFuncWaitToGetWorker is used to test waiting to get worker.
func TestGoPoolWithFuncWaitToGetWorker(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPoolWithFunc(ctx, func(i InputParam) {
		demoPoolFunc(i)
		wg.Done()
	}, WithSize(GoPoolSize))
	defer p.Release(ctx)

	for i := 0; i < n; i++ {
		wg.Add(1)
		_ = p.Invoke(ctx, Param)
	}
	wg.Wait()
	t.Logf("pool with func, running workers number:%d", p.Running())
	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	curMem = mem.TotalAlloc/MiB - curMem
	t.Logf("memory usage:%d MB", curMem)
}

func TestGoPoolWithFuncWaitToGetWorkerPreMalloc(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPoolWithFunc(ctx, func(i InputParam) {
		demoPoolFunc(i)
		wg.Done()
	}, WithPreAlloc(true), WithSize(GoPoolSize))
	defer p.Release(ctx)

	for i := 0; i < n; i++ {
		wg.Add(1)
		_ = p.Invoke(ctx, Param)
	}
	wg.Wait()
	t.Logf("pool with func, running workers number:%d", p.Running())
	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	curMem = mem.TotalAlloc/MiB - curMem
	t.Logf("memory usage:%d MB", curMem)
}

// TestGoPoolGetWorkerFromCache is used to test getting worker from sync.Pool.
func TestGoPoolGetWorkerFromCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPool(ctx, WithSize(TestSize))
	defer p.Release(ctx)

	for i := 0; i < GoPoolSize; i++ {
		_ = p.Submit(ctx, demoFunc)
	}
	time.Sleep(2 * DefaultCleanIntervalTime)
	_ = p.Submit(ctx, demoFunc)
	t.Logf("pool, running workers number:%d", p.Running())
	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	curMem = mem.TotalAlloc/MiB - curMem
	t.Logf("memory usage:%d MB", curMem)
}

// TestGoPoolWithFuncGetWorkerFromCache is used to test getting worker from sync.Pool.
func TestGoPoolWithFuncGetWorkerFromCache(t *testing.T) {
	dur := 10
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPoolWithFunc(ctx, demoPoolFunc, WithSize(TestSize))
	defer p.Release(ctx)

	for i := 0; i < GoPoolSize; i++ {
		_ = p.Invoke(ctx, dur)
	}
	time.Sleep(2 * DefaultCleanIntervalTime)
	_ = p.Invoke(ctx, dur)
	t.Logf("pool with func, running workers number:%d", p.Running())
	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	curMem = mem.TotalAlloc/MiB - curMem
	t.Logf("memory usage:%d MB", curMem)
}

func TestGoPoolWithFuncGetWorkerFromCachePreMalloc(t *testing.T) {
	dur := 10
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPoolWithFunc(ctx, demoPoolFunc, WithPreAlloc(true), WithSize(TestSize))
	defer p.Release(ctx)

	for i := 0; i < GoPoolSize; i++ {
		_ = p.Invoke(ctx, dur)
	}
	time.Sleep(2 * DefaultCleanIntervalTime)
	_ = p.Invoke(ctx, dur)
	t.Logf("pool with func, running workers number:%d", p.Running())
	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	curMem = mem.TotalAlloc/MiB - curMem
	t.Logf("memory usage:%d MB", curMem)
}

// Contrast between goroutines without a pool and goroutines with go pool.

func TestNoPool(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			demoFunc()
			wg.Done()
		}()
	}

	wg.Wait()
	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	curMem = mem.TotalAlloc/MiB - curMem
	t.Logf("memory usage:%d MB", curMem)
}

func TestGoPool(t *testing.T) {
	defer Release(nil)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		_ = Submit(nil, func() {
			demoFunc()
			wg.Done()
		})
	}
	wg.Wait()

	t.Logf("pool, capacity:%d", Cap())
	t.Logf("pool, running workers number:%d", Running())
	t.Logf("pool, free workers number:%d", Free())

	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	curMem = mem.TotalAlloc/MiB - curMem
	t.Logf("memory usage:%d MB", curMem)
}

func TestPanicHandler(t *testing.T) {
	var panicCounter int64
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p0, err := NewPool(ctx, WithSize(10), WithPanicHandler(func(p interface{}) {
		defer wg.Done()
		atomic.AddInt64(&panicCounter, 1)
		t.Logf("catch panic with PanicHandler: %v", p)
	}))
	assert.NoErrorf(t, err, "create new pool failed: %v", err)
	defer p0.Release(ctx)
	wg.Add(1)
	_ = p0.Submit(ctx, func() {
		panic("Oops!")
	})
	wg.Wait()
	c := atomic.LoadInt64(&panicCounter)
	assert.EqualValuesf(t, 1, c, "panic handler didn't work, panicCounter: %d", c)
	assert.EqualValues(t, 0, p0.Running(), "pool should be empty after panic")
	p1, err := NewPoolWithFunc(ctx, func(p InputParam) { panic(p) }, WithSize(10), WithPanicHandler(func(_ interface{}) {
		defer wg.Done()
		atomic.AddInt64(&panicCounter, 1)
	}))
	assert.NoErrorf(t, err, "create new pool with func failed: %v", err)
	defer p1.Release(ctx)
	wg.Add(1)
	_ = p1.Invoke(ctx, "Oops!")
	wg.Wait()
	c = atomic.LoadInt64(&panicCounter)
	assert.EqualValuesf(t, 2, c, "panic handler didn't work, panicCounter: %d", c)
	assert.EqualValues(t, 0, p1.Running(), "pool should be empty after panic")
}

func TestPanicHandlerPreMalloc(t *testing.T) {
	var panicCounter int64
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p0, err := NewPool(ctx, WithSize(10), WithPreAlloc(true), WithPanicHandler(func(p interface{}) {
		defer wg.Done()
		atomic.AddInt64(&panicCounter, 1)
		t.Logf("catch panic with PanicHandler: %v", p)
	}))
	assert.NoErrorf(t, err, "create new pool failed: %v", err)
	defer p0.Release(ctx)
	wg.Add(1)
	_ = p0.Submit(ctx, func() {
		panic("Oops!")
	})
	wg.Wait()
	c := atomic.LoadInt64(&panicCounter)
	assert.EqualValuesf(t, 1, c, "panic handler didn't work, panicCounter: %d", c)
	assert.EqualValues(t, 0, p0.Running(), "pool should be empty after panic")
	p1, err := NewPoolWithFunc(ctx, func(p InputParam) { panic(p) }, WithSize(10), WithPanicHandler(func(_ interface{}) {
		defer wg.Done()
		atomic.AddInt64(&panicCounter, 1)
	}))
	assert.NoErrorf(t, err, "create new pool with func failed: %v", err)
	defer p1.Release(ctx)
	wg.Add(1)
	_ = p1.Invoke(ctx, "Oops!")
	wg.Wait()
	c = atomic.LoadInt64(&panicCounter)
	assert.EqualValuesf(t, 2, c, "panic handler didn't work, panicCounter: %d", c)
	assert.EqualValues(t, 0, p1.Running(), "pool should be empty after panic")
}

func TestPoolPanicWithoutHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p0, err := NewPool(ctx, WithSize(10))
	assert.NoErrorf(t, err, "create new pool failed: %v", err)
	defer p0.Release(ctx)
	_ = p0.Submit(ctx, func() {
		panic("Oops!")
	})

	p1, err := NewPoolWithFunc(ctx, func(p InputParam) {
		panic(p)
	}, WithSize(10))
	assert.NoErrorf(t, err, "create new pool with func failed: %v", err)
	defer p1.Release(ctx)
	_ = p1.Invoke(ctx, "Oops!")
}

func TestPoolPanicWithoutHandlerPreMalloc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p0, err := NewPool(ctx, WithSize(10), WithPreAlloc(true))
	assert.NoErrorf(t, err, "create new pool failed: %v", err)
	defer p0.Release(ctx)
	_ = p0.Submit(ctx, func() {
		panic("Oops!")
	})

	p1, err := NewPoolWithFunc(ctx, func(p InputParam) {
		panic(p)
	}, WithSize(10))

	assert.NoErrorf(t, err, "create new pool with func failed: %v", err)

	defer p1.Release(ctx)
	_ = p1.Invoke(ctx, "Oops!")
}

func TestPurgePool(t *testing.T) {
	size := 500
	ch := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, err := NewPool(ctx, WithSize(size))
	assert.NoErrorf(t, err, "create TimingPool failed: %v", err)
	defer p.Release(ctx)

	for i := 0; i < size; i++ {
		j := i + 1
		_ = p.Submit(ctx, func() {
			<-ch
			d := j % 100
			time.Sleep(time.Duration(d) * time.Millisecond)
		})
	}
	assert.Equalf(t, size, p.Running(), "pool should be full, expected: %d, but got: %d", size, p.Running())

	close(ch)
	time.Sleep(5 * DefaultCleanIntervalTime)
	assert.Equalf(t, 0, p.Running(), "pool should be empty after purge, but got %d", p.Running())

	ch = make(chan struct{})
	f := func(i InputParam) {
		<-ch
		d := i.(int) % 100
		time.Sleep(time.Duration(d) * time.Millisecond)
	}

	p1, err := NewPoolWithFunc(ctx, f, WithSize(size))
	assert.NoErrorf(t, err, "create TimingPoolWithFunc failed: %v", err)
	defer p1.Release(ctx)

	for i := 0; i < size; i++ {
		_ = p1.Invoke(ctx, i)
	}
	assert.Equalf(t, size, p1.Running(), "pool should be full, expected: %d, but got: %d", size, p1.Running())

	close(ch)
	time.Sleep(5 * DefaultCleanIntervalTime)
	assert.Equalf(t, 0, p1.Running(), "pool should be empty after purge, but got %d", p1.Running())
}

func TestPurgePreMallocPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, err := NewPool(ctx, WithSize(10), WithPreAlloc(true))
	assert.NoErrorf(t, err, "create TimingPool failed: %v", err)
	defer p.Release(ctx)
	_ = p.Submit(ctx, demoFunc)
	time.Sleep(3 * DefaultCleanIntervalTime)
	assert.EqualValues(t, 0, p.Running(), "all p should be purged")
	p1, err := NewPoolWithFunc(ctx, demoPoolFunc, WithSize(10))
	assert.NoErrorf(t, err, "create TimingPoolWithFunc failed: %v", err)
	defer p1.Release(ctx)
	_ = p1.Invoke(ctx, 1)
	time.Sleep(3 * DefaultCleanIntervalTime)
	assert.EqualValues(t, 0, p.Running(), "all p should be purged")
}

func TestNonblockingSubmit(t *testing.T) {
	poolSize := 10
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, err := NewPool(ctx, WithSize(poolSize), WithNonblocking(true))
	assert.NoErrorf(t, err, "create TimingPool failed: %v", err)
	defer p.Release(ctx)
	for i := 0; i < poolSize-1; i++ {
		assert.NoError(t, p.Submit(ctx, longRunningFunc), "nonblocking submit when pool is not full shouldn't return error")
	}
	ch := make(chan struct{})
	ch1 := make(chan struct{})
	f := func() {
		<-ch
		close(ch1)
	}
	// p is full now.
	assert.NoError(t, p.Submit(ctx, f), "nonblocking submit when pool is not full shouldn't return error")
	assert.EqualError(t, p.Submit(ctx, demoFunc), ErrPoolOverload.Error(),
		"nonblocking submit when pool is full should get an ErrPoolOverload")
	// interrupt f to get an available worker
	close(ch)
	<-ch1
	assert.NoError(t, p.Submit(ctx, demoFunc), "nonblocking submit when pool is not full shouldn't return error")
}

func TestMaxBlockingSubmit(t *testing.T) {
	poolSize := 10
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, err := NewPool(ctx, WithSize(poolSize), WithMaxBlockingTasks(1))
	assert.NoErrorf(t, err, "create TimingPool failed: %v", err)
	defer p.Release(ctx)
	for i := 0; i < poolSize-1; i++ {
		assert.NoError(t, p.Submit(ctx, longRunningFunc), "submit when pool is not full shouldn't return error")
	}
	ch := make(chan struct{})
	f := func() {
		<-ch
	}
	// p is full now.
	assert.NoError(t, p.Submit(ctx, f), "submit when pool is not full shouldn't return error")
	var wg sync.WaitGroup
	wg.Add(1)
	errCh := make(chan error, 1)
	go func() {
		// should be blocked. blocking num == 1
		if err := p.Submit(ctx, demoFunc); err != nil {
			errCh <- err
		}
		wg.Done()
	}()
	time.Sleep(1 * time.Second)
	// already reached max blocking limit
	assert.EqualError(t, p.Submit(ctx, demoFunc), ErrPoolOverload.Error(),
		"blocking submit when pool reach max blocking submit should return ErrPoolOverload")
	// interrupt f to make blocking submit successful.
	close(ch)
	wg.Wait()
	select {
	case <-errCh:
		t.Fatalf("blocking submit when pool is full should not return error")
	default:
	}
}

func TestNonblockingSubmitWithFunc(t *testing.T) {
	poolSize := 10
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, err := NewPoolWithFunc(ctx, func(i InputParam) {
		longRunningPoolFunc(i)
		wg.Done()
	}, WithNonblocking(true), WithSize(poolSize))
	assert.NoError(t, err, "create TimingPool failed: %v", err)
	defer p.Release(ctx)
	ch := make(chan struct{})
	wg.Add(poolSize)
	for i := 0; i < poolSize-1; i++ {
		assert.NoError(t, p.Invoke(ctx, ch), "nonblocking submit when pool is not full shouldn't return error")
	}
	// p is full now.
	assert.NoError(t, p.Invoke(ctx, ch), "nonblocking submit when pool is not full shouldn't return error")
	assert.EqualError(t, p.Invoke(ctx, nil), ErrPoolOverload.Error(),
		"nonblocking submit when pool is full should get an ErrPoolOverload")
	// interrupt f to get an available worker
	close(ch)
	wg.Wait()
	assert.NoError(t, p.Invoke(ctx, nil), "nonblocking submit when pool is not full shouldn't return error")
}

func TestMaxBlockingSubmitWithFunc(t *testing.T) {
	poolSize := 10
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, err := NewPoolWithFunc(ctx, longRunningPoolFunc, WithMaxBlockingTasks(1), WithSize(poolSize))
	assert.NoError(t, err, "create TimingPool failed: %v", err)
	defer p.Release(ctx)
	for i := 0; i < poolSize-1; i++ {
		assert.NoError(t, p.Invoke(ctx, Param), "submit when pool is not full shouldn't return error")
	}
	ch := make(chan struct{})
	// p is full now.
	assert.NoError(t, p.Invoke(ctx, ch), "submit when pool is not full shouldn't return error")
	var wg sync.WaitGroup
	wg.Add(1)
	errCh := make(chan error, 1)
	go func() {
		// should be blocked. blocking num == 1
		if err := p.Invoke(ctx, Param); err != nil {
			errCh <- err
		}
		wg.Done()
	}()
	time.Sleep(1 * time.Second)
	// already reached max blocking limit
	assert.EqualErrorf(t, p.Invoke(ctx, Param), ErrPoolOverload.Error(),
		"blocking submit when pool reach max blocking submit should return ErrPoolOverload: %v", err)
	// interrupt one func to make blocking submit successful.
	close(ch)
	wg.Wait()
	select {
	case <-errCh:
		t.Fatalf("blocking submit when pool is full should not return error")
	default:
	}
}

func TestRebootDefaultPool(t *testing.T) {
	defer Release(nil)
	Reboot(nil) // should do nothing inside
	var wg sync.WaitGroup
	wg.Add(1)
	_ = Submit(nil, func() {
		demoFunc()
		wg.Done()
	})
	wg.Wait()
	assert.NoError(t, ReleaseTimeout(nil, time.Second))
	assert.EqualError(t, Submit(nil, nil), ErrPoolClosed.Error(), "pool should be closed")
	Reboot(nil)
	wg.Add(1)
	assert.NoError(t, Submit(nil, func() { wg.Done() }), "pool should be rebooted")
	wg.Wait()
}

func TestRebootNewPool(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, err := NewPool(ctx, WithSize(10))
	assert.NoErrorf(t, err, "create Pool failed: %v", err)
	defer p.Release(ctx)
	wg.Add(1)
	_ = p.Submit(ctx, func() {
		demoFunc()
		wg.Done()
	})
	wg.Wait()
	assert.NoError(t, p.ReleaseTimeout(ctx, time.Second))
	assert.EqualError(t, p.Submit(ctx, nil), ErrPoolClosed.Error(), "pool should be closed")
	p.Reboot(ctx)
	wg.Add(1)
	assert.NoError(t, p.Submit(ctx, func() { wg.Done() }), "pool should be rebooted")
	wg.Wait()

	p1, err := NewPoolWithFunc(ctx, func(i InputParam) {
		demoPoolFunc(i)
		wg.Done()
	}, WithSize(10))
	assert.NoErrorf(t, err, "create TimingPoolWithFunc failed: %v", err)
	defer p1.Release(ctx)
	wg.Add(1)
	_ = p1.Invoke(ctx, 1)
	wg.Wait()
	assert.NoError(t, p1.ReleaseTimeout(ctx, time.Second))
	assert.EqualError(t, p1.Invoke(ctx, nil), ErrPoolClosed.Error(), "pool should be closed")
	p1.Reboot(ctx)
	wg.Add(1)
	assert.NoError(t, p1.Invoke(ctx, 1), "pool should be rebooted")
	wg.Wait()
}

func TestInfinitePool(t *testing.T) {
	c := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPool(ctx, WithSize(-1))
	_ = p.Submit(ctx, func() {
		_ = p.Submit(ctx, func() {
			<-c
		})
	})
	c <- struct{}{}
	if n := p.Running(); n != 2 {
		t.Errorf("expect 2 workers running, but got %d", n)
	}
	if n := p.Free(); n != -1 {
		t.Errorf("expect -1 of free workers by unlimited pool, but got %d", n)
	}
	p.Tune(10)
	if capacity := p.Cap(); capacity != -1 {
		t.Fatalf("expect capacity: -1 but got %d", capacity)
	}
	var err error
	_, err = NewPool(ctx, WithSize(-1), WithPreAlloc(true))
	assert.EqualErrorf(t, err, ErrInvalidPreAllocSize.Error(), "")
}

func testPoolWithDisablePurge(t *testing.T, p *Pool, numWorker int, waitForPurge time.Duration) {
	sig := make(chan struct{})
	var wg1, wg2 sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	wg1.Add(numWorker)
	wg2.Add(numWorker)
	for i := 0; i < numWorker; i++ {
		_ = p.Submit(ctx, func() {
			wg1.Done()
			<-sig
			wg2.Done()
		})
	}
	wg1.Wait()

	runningCnt := p.Running()
	assert.EqualValuesf(t, numWorker, runningCnt, "expect %d workers running, but got %d", numWorker, runningCnt)
	freeCnt := p.Free()
	assert.EqualValuesf(t, 0, freeCnt, "expect %d free workers, but got %d", 0, freeCnt)

	// Finish all tasks and sleep for a while to wait for purging, since we've disabled purge mechanism,
	// we should see that all workers are still running after the sleep.
	close(sig)
	wg2.Wait()
	time.Sleep(waitForPurge + waitForPurge/2)

	runningCnt = p.Running()
	assert.EqualValuesf(t, numWorker, runningCnt, "expect %d workers running, but got %d", numWorker, runningCnt)
	freeCnt = p.Free()
	assert.EqualValuesf(t, 0, freeCnt, "expect %d free workers, but got %d", 0, freeCnt)

	err := p.ReleaseTimeout(ctx, waitForPurge + waitForPurge/2)
	assert.NoErrorf(t, err, "release pool failed: %v", err)

	runningCnt = p.Running()
	assert.EqualValuesf(t, 0, runningCnt, "expect %d workers running, but got %d", 0, runningCnt)
	freeCnt = p.Free()
	assert.EqualValuesf(t, numWorker, freeCnt, "expect %d free workers, but got %d", numWorker, freeCnt)
}

func TestWithDisablePurgePool(t *testing.T) {
	numWorker := 10
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPool(ctx, WithSize(numWorker), WithDisablePurge(true))
	testPoolWithDisablePurge(t, p, numWorker, DefaultCleanIntervalTime)
}

func TestWithDisablePurgeAndWithExpirationPool(t *testing.T) {
	numWorker := 10
	expiredDuration := time.Millisecond * 100
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPool(ctx, WithSize(numWorker), WithDisablePurge(true), WithExpiryDuration(expiredDuration))
	testPoolWithDisablePurge(t, p, numWorker, expiredDuration)
}

func testPoolFuncWithDisablePurge(t *testing.T, p *PoolWithFunc, numWorker int, wg1, wg2 *sync.WaitGroup, sig chan struct{}, waitForPurge time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	for i := 0; i < numWorker; i++ {
		_ = p.Invoke(ctx, i)
	}
	wg1.Wait()

	runningCnt := p.Running()
	assert.EqualValuesf(t, numWorker, runningCnt, "expect %d workers running, but got %d", numWorker, runningCnt)
	freeCnt := p.Free()
	assert.EqualValuesf(t, 0, freeCnt, "expect %d free workers, but got %d", 0, freeCnt)

	// Finish all tasks and sleep for a while to wait for purging, since we've disabled purge mechanism,
	// we should see that all workers are still running after the sleep.
	close(sig)
	wg2.Wait()
	time.Sleep(waitForPurge + waitForPurge/2)

	runningCnt = p.Running()
	assert.EqualValuesf(t, numWorker, runningCnt, "expect %d workers running, but got %d", numWorker, runningCnt)
	freeCnt = p.Free()
	assert.EqualValuesf(t, 0, freeCnt, "expect %d free workers, but got %d", 0, freeCnt)

	err := p.ReleaseTimeout(ctx, waitForPurge + waitForPurge/2)
	assert.NoErrorf(t, err, "release pool failed: %v", err)

	runningCnt = p.Running()
	assert.EqualValuesf(t, 0, runningCnt, "expect %d workers running, but got %d", 0, runningCnt)
	freeCnt = p.Free()
	assert.EqualValuesf(t, numWorker, freeCnt, "expect %d free workers, but got %d", numWorker, freeCnt)
}

func TestWithDisablePurgePoolFunc(t *testing.T) {
	numWorker := 10
	sig := make(chan struct{})
	var wg1, wg2 sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	wg1.Add(numWorker)
	wg2.Add(numWorker)
	p, _ := NewPoolWithFunc(ctx, func(_ InputParam) {
		wg1.Done()
		<-sig
		wg2.Done()
	}, WithDisablePurge(true), WithSize(numWorker))
	testPoolFuncWithDisablePurge(t, p, numWorker, &wg1, &wg2, sig, DefaultCleanIntervalTime)
}

func TestWithDisablePurgeAndWithExpirationPoolFunc(t *testing.T) {
	numWorker := 2
	sig := make(chan struct{})
	var wg1, wg2 sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	wg1.Add(numWorker)
	wg2.Add(numWorker)
	expiredDuration := time.Millisecond * 100
	p, _ := NewPoolWithFunc(ctx, func(_ InputParam) {
		wg1.Done()
		<-sig
		wg2.Done()
	}, WithDisablePurge(true), WithExpiryDuration(expiredDuration), WithSize(numWorker))
	testPoolFuncWithDisablePurge(t, p, numWorker, &wg1, &wg2, sig, expiredDuration)
}

func TestInfinitePoolWithFunc(t *testing.T) {
	c := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPoolWithFunc(ctx, func(i InputParam) {
		demoPoolFunc(i)
		<-c
	}, WithSize(-1))
	_ = p.Invoke(ctx, 10)
	_ = p.Invoke(ctx, 10)
	c <- struct{}{}
	c <- struct{}{}
	if n := p.Running(); n != 2 {
		t.Errorf("expect 2 workers running, but got %d", n)
	}
	if n := p.Free(); n != -1 {
		t.Errorf("expect -1 of free workers by unlimited pool, but got %d", n)
	}
	p.Tune(10)
	if capacity := p.Cap(); capacity != -1 {
		t.Fatalf("expect capacity: -1 but got %d", capacity)
	}
	var err error
	_, err = NewPoolWithFunc(ctx, demoPoolFunc, WithSize(-1), WithPreAlloc(true))
	if err != ErrInvalidPreAllocSize {
		t.Errorf("expect ErrInvalidPreAllocSize but got %v", err)
	}
}

func TestReleaseWhenRunningPool(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPool(ctx, WithSize(1))
	wg.Add(2)
	go func() {
		t.Log("start aaa")
		defer func() {
			wg.Done()
			t.Log("stop aaa")
		}()
		for i := 0; i < 30; i++ {
			j := i
			_ = p.Submit(ctx, func() {
				t.Log("do task", j)
				time.Sleep(1 * time.Second)
			})
		}
	}()

	go func() {
		t.Log("start bbb")
		defer func() {
			wg.Done()
			t.Log("stop bbb")
		}()
		for i := 100; i < 130; i++ {
			j := i
			_ = p.Submit(ctx, func() {
				t.Log("do task", j)
				time.Sleep(1 * time.Second)
			})
		}
	}()

	time.Sleep(3 * time.Second)
	p.Release(ctx)
	t.Log("wait for all goroutines to exit...")
	wg.Wait()
}

func TestReleaseWhenRunningPoolWithFunc(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPoolWithFunc(ctx, func(i InputParam) {
		t.Log("do task", i)
		time.Sleep(1 * time.Second)
	}, WithSize(1))
	wg.Add(2)
	go func() {
		t.Log("start aaa")
		defer func() {
			wg.Done()
			t.Log("stop aaa")
		}()
		for i := 0; i < 30; i++ {
			_ = p.Invoke(ctx, i)
		}
	}()

	go func() {
		t.Log("start bbb")
		defer func() {
			wg.Done()
			t.Log("stop bbb")
		}()
		for i := 100; i < 130; i++ {
			_ = p.Invoke(ctx, i)
		}
	}()

	time.Sleep(3 * time.Second)
	p.Release(ctx)
	t.Log("wait for all goroutines to exit...")
	wg.Wait()
}

func TestRestCodeCoverage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	_, err := NewPool(nil, WithSize(-1), WithExpiryDuration(-1))
	t.Log(err)
	_, err = NewPool(ctx, WithSize(1), WithExpiryDuration(-1))
	t.Log(err)
	_, err = NewPoolWithFunc(nil, demoPoolFunc, WithExpiryDuration(-1), WithSize(-1))
	t.Log(err)
	_, err = NewPoolWithFunc(ctx, demoPoolFunc, WithExpiryDuration(-1), WithSize(-1))
	t.Log(err)

	options := Options{}
	options.ExpiryDuration = time.Duration(10) * time.Second
	options.Nonblocking = true
	options.PreAlloc = true
	poolOpts, _ := NewPool(nil, WithSize(1), WithOptions(options))
	t.Logf("Pool with options, capacity: %d", poolOpts.Cap())

	p0, _ := NewPool(ctx, WithSize(TestSize), WithLogger(log.New(os.Stderr, "", log.LstdFlags)))
	defer func() {
		_ = p0.Submit(ctx, demoFunc)
	}()
	defer p0.Release(ctx)
	for i := 0; i < n; i++ {
		_ = p0.Submit(ctx, demoFunc)
	}
	t.Logf("pool, capacity:%d", p0.Cap())
	t.Logf("pool, running workers number:%d", p0.Running())
	t.Logf("pool, free workers number:%d", p0.Free())
	p0.Tune(TestSize)
	p0.Tune(TestSize / 10)
	t.Logf("pool, after tuning capacity, capacity:%d, running:%d", p0.Cap(), p0.Running())

	pprem, _ := NewPool(ctx, WithSize(TestSize), WithPreAlloc(true))
	defer func() {
		_ = pprem.Submit(ctx, demoFunc)
	}()
	defer pprem.Release(ctx)
	for i := 0; i < n; i++ {
		_ = pprem.Submit(ctx, demoFunc)
	}
	t.Logf("pre-malloc pool, capacity:%d", pprem.Cap())
	t.Logf("pre-malloc pool, running workers number:%d", pprem.Running())
	t.Logf("pre-malloc pool, free workers number:%d", pprem.Free())
	pprem.Tune(TestSize)
	pprem.Tune(TestSize / 10)
	t.Logf("pre-malloc pool, after tuning capacity, capacity:%d, running:%d", pprem.Cap(), pprem.Running())

	p, _ := NewPoolWithFunc(ctx, demoPoolFunc, WithSize(TestSize))
	defer func() {
		_ = p.Invoke(ctx, Param)
	}()
	defer p.Release(ctx)
	for i := 0; i < n; i++ {
		_ = p.Invoke(ctx, Param)
	}
	time.Sleep(DefaultCleanIntervalTime)
	t.Logf("pool with func, capacity:%d", p.Cap())
	t.Logf("pool with func, running workers number:%d", p.Running())
	t.Logf("pool with func, free workers number:%d", p.Free())
	p.Tune(TestSize)
	p.Tune(TestSize / 10)
	t.Logf("pool with func, after tuning capacity, capacity:%d, running:%d", p.Cap(), p.Running())

	ppremWithFunc, _ := NewPoolWithFunc(ctx, demoPoolFunc, WithPreAlloc(true), WithSize(TestSize))
	defer func() {
		_ = ppremWithFunc.Invoke(ctx, Param)
	}()
	defer ppremWithFunc.Release(ctx)
	for i := 0; i < n; i++ {
		_ = ppremWithFunc.Invoke(ctx, Param)
	}
	time.Sleep(DefaultCleanIntervalTime)
	t.Logf("pre-malloc pool with func, capacity:%d", ppremWithFunc.Cap())
	t.Logf("pre-malloc pool with func, running workers number:%d", ppremWithFunc.Running())
	t.Logf("pre-malloc pool with func, free workers number:%d", ppremWithFunc.Free())
	ppremWithFunc.Tune(TestSize)
	ppremWithFunc.Tune(TestSize / 10)
	t.Logf("pre-malloc pool with func, after tuning capacity, capacity:%d, running:%d", ppremWithFunc.Cap(),
		ppremWithFunc.Running())
}

func TestPoolTuneScaleUp(t *testing.T) {
	c := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPool(ctx, WithSize(2))
	for i := 0; i < 2; i++ {
		_ = p.Submit(ctx, func() {
			<-c
		})
	}
	if n := p.Running(); n != 2 {
		t.Errorf("expect 2 workers running, but got %d", n)
	}
	// test pool tune scale up one
	p.Tune(3)
	_ = p.Submit(ctx, func() {
		<-c
	})
	if n := p.Running(); n != 3 {
		t.Errorf("expect 3 workers running, but got %d", n)
	}
	// test pool tune scale up multiple
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.Submit(ctx, func() {
				<-c
			})
		}()
	}
	p.Tune(8)
	wg.Wait()
	if n := p.Running(); n != 8 {
		t.Errorf("expect 8 workers running, but got %d", n)
	}
	for i := 0; i < 8; i++ {
		c <- struct{}{}
	}
	p.Release(ctx)

	// test PoolWithFunc
	pf, _ := NewPoolWithFunc(ctx, func(_ InputParam) {
		<-c
	}, WithSize(2))
	for i := 0; i < 2; i++ {
		_ = pf.Invoke(ctx, 1)
	}
	if n := pf.Running(); n != 2 {
		t.Errorf("expect 2 workers running, but got %d", n)
	}
	// test pool tune scale up one
	pf.Tune(3)
	_ = pf.Invoke(ctx, 1)
	if n := pf.Running(); n != 3 {
		t.Errorf("expect 3 workers running, but got %d", n)
	}
	// test pool tune scale up multiple
	var pfwg sync.WaitGroup
	for i := 0; i < 5; i++ {
		pfwg.Add(1)
		go func() {
			defer pfwg.Done()
			_ = pf.Invoke(ctx, 1)
		}()
	}
	pf.Tune(8)
	pfwg.Wait()
	if n := pf.Running(); n != 8 {
		t.Errorf("expect 8 workers running, but got %d", n)
	}
	for i := 0; i < 8; i++ {
		c <- struct{}{}
	}
	close(c)
	pf.Release(ctx)
}

func TestReleaseTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	p, _ := NewPool(ctx, WithSize(10))
	for i := 0; i < 5; i++ {
		_ = p.Submit(ctx, func() {
			time.Sleep(time.Second)
		})
	}
	assert.NotZero(t, p.Running())
	err := p.ReleaseTimeout(ctx, 2 * time.Second)
	assert.NoError(t, err)

	var pf *PoolWithFunc
	pf, _ = NewPoolWithFunc(ctx, func(i InputParam) {
		dur := i.(time.Duration)
		time.Sleep(dur)
	}, WithSize(10))
	for i := 0; i < 5; i++ {
		_ = pf.Invoke(ctx, time.Second)
	}
	assert.NotZero(t, pf.Running())
	err = pf.ReleaseTimeout(ctx, 2 * time.Second)
	assert.NoError(t, err)
}

func TestDefaultPoolReleaseTimeout(t *testing.T) {
	Reboot(nil) // should do nothing inside
	for i := 0; i < 5; i++ {
		_ = Submit(nil, func() {
			time.Sleep(time.Second)
		})
	}
	assert.NotZero(t, Running())
	err := ReleaseTimeout(nil, 2 * time.Second)
	assert.NoError(t, err)
}

func TestMultiPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	_, err := NewMultiPool(ctx, 10, 8, WithSize(-1))
	assert.ErrorIs(t, err, ErrInvalidLoadBalancingStrategy)

	mp, err := NewMultiPool(ctx, 10, RoundRobin, WithSize(5))
	testFn := func() {
		for i := 0; i < 50; i++ {
			err = mp.Submit(longRunningFunc)
			assert.NoError(t, err)
		}
		assert.EqualValues(t, mp.Waiting(), 0)
		_, err = mp.WaitingByIndex(-1)
		assert.ErrorIs(t, err, ErrInvalidPoolIndex)
		_, err = mp.WaitingByIndex(11)
		assert.ErrorIs(t, err, ErrInvalidPoolIndex)
		assert.EqualValues(t, 50, mp.Running())
		_, err = mp.RunningByIndex(-1)
		assert.ErrorIs(t, err, ErrInvalidPoolIndex)
		_, err = mp.RunningByIndex(11)
		assert.ErrorIs(t, err, ErrInvalidPoolIndex)
		assert.EqualValues(t, 0, mp.Free())
		_, err = mp.FreeByIndex(-1)
		assert.ErrorIs(t, err, ErrInvalidPoolIndex)
		_, err = mp.FreeByIndex(11)
		assert.ErrorIs(t, err, ErrInvalidPoolIndex)
		assert.EqualValues(t, 50, mp.Cap())
		assert.False(t, mp.IsClosed())
		for i := 0; i < 10; i++ {
			n, _ := mp.WaitingByIndex(i)
			assert.EqualValues(t, 0, n)
			n, _ = mp.RunningByIndex(i)
			assert.EqualValues(t, 5, n)
			n, _ = mp.FreeByIndex(i)
			assert.EqualValues(t, 0, n)
		}
		atomic.StoreInt32(&stopLongRunningFunc, 1)
		assert.NoError(t, mp.ReleaseTimeout(3*time.Second))
		assert.Zero(t, mp.Running())
		assert.True(t, mp.IsClosed())
		atomic.StoreInt32(&stopLongRunningFunc, 0)
	}
	testFn()

	mp.Reboot()
	testFn()

	mp, err = NewMultiPool(ctx, 10, LeastTasks, WithSize(5))
	testFn()

	mp.Reboot()
	testFn()

	mp.Tune(10)
}

func TestMultiPoolWithFunc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	_, err := NewMultiPoolWithFunc(ctx, 10, longRunningPoolFunc, 8, WithSize(-1))
	assert.ErrorIs(t, err, ErrInvalidLoadBalancingStrategy)

	mp, err := NewMultiPoolWithFunc(ctx, 10, longRunningPoolFunc, RoundRobin, WithSize(5))
	testFn := func() {
		for i := 0; i < 50; i++ {
			err = mp.Invoke(i)
			assert.NoError(t, err)
		}
		assert.EqualValues(t, mp.Waiting(), 0)
		_, err = mp.WaitingByIndex(-1)
		assert.ErrorIs(t, err, ErrInvalidPoolIndex)
		_, err = mp.WaitingByIndex(11)
		assert.ErrorIs(t, err, ErrInvalidPoolIndex)
		assert.EqualValues(t, 50, mp.Running())
		_, err = mp.RunningByIndex(-1)
		assert.ErrorIs(t, err, ErrInvalidPoolIndex)
		_, err = mp.RunningByIndex(11)
		assert.ErrorIs(t, err, ErrInvalidPoolIndex)
		assert.EqualValues(t, 0, mp.Free())
		_, err = mp.FreeByIndex(-1)
		assert.ErrorIs(t, err, ErrInvalidPoolIndex)
		_, err = mp.FreeByIndex(11)
		assert.ErrorIs(t, err, ErrInvalidPoolIndex)
		assert.EqualValues(t, 50, mp.Cap())
		assert.False(t, mp.IsClosed())
		for i := 0; i < 10; i++ {
			n, _ := mp.WaitingByIndex(i)
			assert.EqualValues(t, 0, n)
			n, _ = mp.RunningByIndex(i)
			assert.EqualValues(t, 5, n)
			n, _ = mp.FreeByIndex(i)
			assert.EqualValues(t, 0, n)
		}
		atomic.StoreInt32(&stopLongRunningPoolFunc, 1)
		assert.NoError(t, mp.ReleaseTimeout(3*time.Second))
		assert.Zero(t, mp.Running())
		assert.True(t, mp.IsClosed())
		atomic.StoreInt32(&stopLongRunningPoolFunc, 0)
	}
	testFn()

	mp.Reboot()
	testFn()

	mp, err = NewMultiPoolWithFunc(ctx, 10, longRunningPoolFunc, LeastTasks, WithSize(5))
	testFn()

	mp.Reboot()
	testFn()

	mp.Tune(10)
}
