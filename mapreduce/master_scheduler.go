package mapreduce

import (
	"log"
	"sync"
)

// Schedules map operations on remote workers. This will run until InputFilePathChan
// is closed. If there is no worker available, it'll block.
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) int {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////

	var (
		wg        sync.WaitGroup
		filePath  string
		worker    *RemoteWorker
		operation *Operation
	)

	log.Printf("Scheduling %v operations\n", proc)
	master.failedOperationChan = make(chan *Operation)
	master.counter = 0
	master.completedOperations = 0
	for filePath = range filePathChan {
		operation = &Operation{proc, master.counter, filePath}
		master.counter++

		worker = <-master.idleWorkerChan
		wg.Add(1)
		go master.runOperation(worker, operation, &wg)
	}
	for operation = range master.failedOperationChan {
		worker = <-master.idleWorkerChan
		wg.Add(1)
		go master.runOperation(worker, operation, &wg)
	}
	log.Printf("Waiting for operations to end\n")
	wg.Wait()

	log.Printf("%vx %v operations completed\n", master.counter, proc)
	return master.counter
}

// runOperation start a single operation on a RemoteWorker and wait for it to return or fail.
func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation, wg *sync.WaitGroup) {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////

	var (
		err  error
		args *RunArgs
	)

	log.Printf("Running %v (ID: '%v' File: '%v' Worker: '%v')\n", operation.proc, operation.id, operation.filePath, remoteWorker.id)

	args = &RunArgs{operation.id, operation.filePath}
	err = remoteWorker.callRemoteWorker(operation.proc, args, new(struct{}))

	if err != nil {
		log.Printf("Operation %v '%v' Failed. Error: %v\n", operation.proc, operation.id, err)
		wg.Done()
		master.failedOperationChan <- operation
		master.failedWorkerChan <- remoteWorker
	} else {
		wg.Done()
		master.completedOperations++
		if master.completedOperations == master.counter {
			close(master.failedOperationChan)
		}
		master.idleWorkerChan <- remoteWorker
	}
}
