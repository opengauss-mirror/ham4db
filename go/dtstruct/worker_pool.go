/*
	Copyright 2021 SANGFOR TECHNOLOGIES

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package dtstruct

import (
	"gitee.com/opengauss/ham4db/go/agent/common/constant"
	"gitee.com/opengauss/ham4db/go/core/log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerPool used to manage all task running async
type WorkerPool struct {
	wpWtGrp  *sync.WaitGroup
	exitOnce *sync.Once
	workerMu struct {
		sync.RWMutex
		workers  map[string]*Worker // key: worker name, value: worker
		running  map[string]*uint32 // key: worker name, value: number of worker running
		stopping bool               // if worker pool is stopped
	}
	poolExit    chan os.Signal // used to catch system exit signal
	postProcess func(string)   // process after worker's function done
}

// NewWorkerPool create new worker pool
func NewWorkerPool() *WorkerPool {
	workerPool := &WorkerPool{wpWtGrp: &sync.WaitGroup{}, exitOnce: &sync.Once{}, poolExit: make(chan os.Signal, 1)}
	workerPool.workerMu.workers = make(map[string]*Worker, constant.WorkerDefaultQuantity)
	workerPool.workerMu.running = make(map[string]*uint32, constant.WorkerDefaultQuantity)

	// init process function, delete key from running map if there's no running worker for that key
	workerPool.postProcess = func(name string) {
		workerPool.workerMu.Lock()
		defer workerPool.workerMu.Unlock()
		if name != constant.WorkerNameDoneChecker {
			if val, ok := workerPool.workerMu.running[name]; ok {
				if *val = *val - 1; *val == 0 {
					delete(workerPool.workerMu.running, name)
				}
			} else {
				log.Error("worker with name:%s is not running", name)
			}
		}
	}

	// init done checker worker used to check workers when worker pool quit
	_, _ = workerPool.NewWorker(constant.WorkerNameDoneChecker, func(workerExit chan struct{}) {
		for {
			doneCheck := time.Tick(constant.WorkerPoolDoneCheckInterval * time.Second)
			select {
			case <-doneCheck:
				if len(workerPool.workerMu.running) == 1 {
					if _, ok := workerPool.workerMu.workers[constant.WorkerNameDoneChecker]; ok {
						return
					}
				} else {
					log.Warning("worker pool: %s", workerPool.showWorkerRun())
				}
			}
		}
	})
	return workerPool
}

// showWorkerRun show all running worker and count of running instance for it, output after sorted by name
func (w *WorkerPool) showWorkerRun() string {
	w.workerMu.RLock()
	defer w.workerMu.RUnlock()
	var running []string
	for name, count := range w.workerMu.running {
		running = append(running, strings.Join([]string{name, strconv.FormatUint(uint64(*count), 10)}, ":"))
	}
	sort.Strings(running)
	return strings.Join(running, ",")
}

// NewWorker create new worker and put it to worker pool
func (w *WorkerPool) NewWorker(name string, f func(workerExit chan struct{})) (*Worker, error) {
	w.workerMu.Lock()
	defer w.workerMu.Unlock()

	// failed if worker pool is stopped now
	if w.workerMu.stopping {
		return nil, log.Errorf("worker pool is stopping")
	}

	// failed if worker is already exist with the same name
	if _, ok := w.workerMu.workers[name]; ok {
		return nil, log.Errorf("already has worker with same name: %s", name)
	}

	// create new worker and put it to worker pool
	w.workerMu.workers[name] = &Worker{&sync.Once{}, w, name, make(chan struct{}), f}
	return w.workerMu.workers[name], nil
}

// GetWorker get named worker from worker pool
func (w *WorkerPool) GetWorker(name string) *Worker {
	if worker, ok := w.workerMu.workers[name]; ok {
		return worker
	}
	log.Warning("no worker with name:%s", name)
	return nil
}

// RemoveWorker remove worker without running instance from worker pool
func (w *WorkerPool) RemoveWorker(name string) bool {
	if worker, ok := w.workerMu.workers[name]; ok {
		log.Info("trigger worker:%s to stop", name)
		worker.Exit()

		log.Info("waiting until all instances of worker:%s quit", name)
		checker := time.Tick(constant.WorkerQuitCheckInterval * time.Millisecond)
		for {
			<-checker
			if _, exist := w.workerMu.running[name]; !exist {
				break
			}
		}

		// remove worker from worker map and running map
		w.workerMu.Lock()
		delete(w.workerMu.running, name)
		delete(w.workerMu.workers, name)
		w.workerMu.Unlock()

		log.Info("worker:%s has already been removed from worker pool", name)
		return true
	}
	log.Warning("worker:%s is not exist, no need to remove", name)
	return true
}

// AsyncRun create new worker and run it async
func (w *WorkerPool) AsyncRun(name string, f func(workerExit chan struct{})) error {
	if worker, err := w.NewWorker(name, f); err == nil {
		return worker.AsyncRun()
	} else {
		return err
	}
}

// Exit quit worker pool by signal
func (w *WorkerPool) Exit(signal os.Signal) {
	w.poolExit <- signal
}

// GetExitChannel return worker pool's exit channel
func (w *WorkerPool) GetExitChannel() chan os.Signal {
	return w.poolExit
}

// WaitStop stop all worker and quit worker pool gracefully by once
func (w *WorkerPool) WaitStop() {
	w.exitOnce.Do(
		func() {
			// wait until system exit
			log.Infof("work pool will stop by signal: %v", <-w.poolExit)

			// run done check worker
			if err := w.workerMu.workers[constant.WorkerNameDoneChecker].AsyncRun(); err != nil {
				log.Error("got error when run done check worker, error:%s", err)
			}

			// set worker pool stop state is true
			w.workerMu.Lock()
			w.workerMu.stopping = true
			w.workerMu.Unlock()

			// trigger all worker to exit
			for name, worker := range w.workerMu.workers {
				if name != constant.WorkerNameDoneChecker {
					running := uint32(0)
					if val, ok := w.workerMu.running[name]; ok {
						running = *val
					}
					log.Infof("stop worker:%s, running:%d", name, running)
					close(worker.workerExit)
				}
			}

			// wait until all worker to exit
			w.wpWtGrp.Wait()
			log.Infof("worker pool has been stopped successfully")
		},
	)
}

// Worker used to exec function async
type Worker struct {
	exitOnce   *sync.Once          // control exit
	wp         *WorkerPool         // worker pool which hold this worker
	Name       string              // worker name, better to define in constant with "WorkerName" prefix
	workerExit chan struct{}       // channel used to trigger quit by worker pool
	f          func(chan struct{}) // what to do by this worker
}

// AsyncRun run worker async
func (w *Worker) AsyncRun() error {
	log.Infof("run worker:%s", w.Name)
	w.wp.workerMu.Lock()
	defer w.wp.workerMu.Unlock()

	// failed if worker pool is stopped
	if w.wp.workerMu.stopping {
		return log.Errorf("worker pool is stopping")
	}

	// increase instance count for this worker
	if val, ok := w.wp.workerMu.running[w.Name]; ok {
		atomic.AddUint32(val, 1)
	} else {
		count := uint32(1)
		w.wp.workerMu.running[w.Name] = &count
	}

	// increase wait group counter for worker pool and worker
	w.wp.wpWtGrp.Add(1)

	// do work async
	go func() {
		defer log.Infof("worker:%s done", w.Name)
		defer w.wp.postProcess(w.Name)
		defer w.wp.wpWtGrp.Done()
		w.f(w.workerExit)
	}()
	return nil
}

// Exit trigger worker to exit
func (w *Worker) Exit() {
	w.exitOnce.Do(func() {
		close(w.workerExit)
	})
}
