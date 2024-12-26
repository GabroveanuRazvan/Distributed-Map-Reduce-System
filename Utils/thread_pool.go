package Utils

import (
	"fmt"
	"sync"
)

type Job func()

type ThreadPool struct {
	workers   []Worker
	waitGroup *sync.WaitGroup
	jobSender chan<- Job
	verbose   bool
}

// NewThreadPool initializes a new thread pool with a fixed number of workers.
func NewThreadPool(numWorkers uint32, verbose bool) *ThreadPool {

	// panic in case when there are no workers
	if numWorkers == 0 {
		panic("Thread pool initialized with 0 workers\n")
	}

	// get an array of workers a reference to a waitgroup and an unbuffered channel
	workers := make([]Worker, numWorkers)
	waitGroup := &sync.WaitGroup{}
	jobSender := make(chan Job)

	// create and start the workers
	for i := uint32(0); i < numWorkers; i++ {

		workers[i] = NewWorker(i+1, waitGroup, verbose)
		workers[i].Start(jobSender)

	}

	// return the thread pool reference
	return &ThreadPool{
		workers:   workers,
		waitGroup: waitGroup,
		jobSender: jobSender,
		verbose:   verbose,
	}
}

// Go passes the given function through the channel, blocking the thread until a worker gets the job.
func (threadPool *ThreadPool) Go(job Job) {
	threadPool.jobSender <- job
}

// Wait closes the channel and waits for all the workers to finish their jobs.
func (threadPool *ThreadPool) Wait() {

	close(threadPool.jobSender)
	numWorkers := len(threadPool.workers)

	for i := 0; i < numWorkers; i++ {
		threadPool.waitGroup.Wait()
	}

}

type Worker struct {
	id      uint32
	wg      *sync.WaitGroup
	verbose bool
}

// NewWorker returns a new worker with a wait group reference.
func NewWorker(id uint32, group *sync.WaitGroup, verbose bool) Worker {
	return Worker{
		id:      id,
		wg:      group,
		verbose: verbose,
	}
}

// Start  starts the worker by incrementing the wait group and waiting to receive jobs through the channel.
func (worker Worker) Start(jobReceiver <-chan Job) {

	if worker.verbose {
		fmt.Printf("Worker %d started and is waiting for jobs...\n", worker.id)
	}

	worker.wg.Add(1)

	// Start the thread and wait for jobs while the channel is opened
	go func() {

		defer worker.wg.Done()

		for job := range jobReceiver {
			// once a job is received, process and run it

			if worker.verbose {
				fmt.Printf("Worker %d got a new job!\n", worker.id)
			}

			job()

			if worker.verbose {
				fmt.Printf("Worker %d finished its job.\n", worker.id)
			}

		}

		if worker.verbose {
			fmt.Printf("Worker %d closed.\n", worker.id)
		}

	}()

}
