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

package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alolainc/gopool"
)

var sum int32

func myFunc(i interface{}) {
	n := i.(int32)
	atomic.AddInt32(&sum, n)
	fmt.Printf("run with %d\n", n)
}

func demoFunc() {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("Hello World!")
}

func main() {
	defer gopool.Release()

	runTimes := 1000

	// Use the common pool.
	var wg sync.WaitGroup
	syncCalculateSum := func() {
		demoFunc()
		wg.Done()
	}
	for i := 0; i < runTimes; i++ {
		wg.Add(1)
		_ = gopool.Submit(syncCalculateSum)
	}
	wg.Wait()
	fmt.Printf("running goroutines: %d\n", gopool.Running())
	fmt.Printf("finish all tasks.\n")

	// Use the pool with a function,
	// set 10 to the capacity of goroutine pool and 1 second for expired duration.
	p, _ := gopool.NewPoolWithFunc(10, func(i interface{}) {
		myFunc(i)
		wg.Done()
	})
	defer p.Release()
	// Submit tasks one by one.
	for i := 0; i < runTimes; i++ {
		wg.Add(1)
		_ = p.Invoke(int32(i))
	}
	wg.Wait()
	fmt.Printf("running goroutines: %d\n", p.Running())
	fmt.Printf("finish all tasks, result is %d\n", sum)
	if sum != 499500 {
		panic("the final result is wrong!!!")
	}

	// Use the MultiPool and set the capacity of the 10 goroutine pools to unlimited.
	// If you use -1 as the pool size parameter, the size will be unlimited.
	// There are two load-balancing algorithms for pools: gopool.RoundRobin and gopool.LeastTasks.
	mp, _ := gopool.NewMultiPool(10, -1, gopool.RoundRobin)
	defer mp.ReleaseTimeout(5 * time.Second)
	for i := 0; i < runTimes; i++ {
		wg.Add(1)
		_ = mp.Submit(syncCalculateSum)
	}
	wg.Wait()
	fmt.Printf("running goroutines: %d\n", mp.Running())
	fmt.Printf("finish all tasks.\n")

	// Use the MultiPoolFunc and set the capacity of 10 goroutine pools to (runTimes/10).
	mpf, _ := gopool.NewMultiPoolWithFunc(10, runTimes/10, func(i interface{}) {
		myFunc(i)
		wg.Done()
	}, gopool.LeastTasks)
	defer mpf.ReleaseTimeout(5 * time.Second)
	for i := 0; i < runTimes; i++ {
		wg.Add(1)
		_ = mpf.Invoke(int32(i))
	}
	wg.Wait()
	fmt.Printf("running goroutines: %d\n", mpf.Running())
	fmt.Printf("finish all tasks, result is %d\n", sum)
	if sum != 499500*2 {
		panic("the final result is wrong!!!")
	}
}
