package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup

	for i := 0;i < ntasks;i++{
		filename := ""
		if phase == mapPhase{
			filename = mr.files[i]
		}

		wg.Add(1)
		go func(jobName string,filename string,phase jobPhase,taskNumber int,numOtherPhase int){
			defer wg.Done()
			for{
				worker := <-mr.registerChannel
				taskArgs := DoTaskArgs{mr.jobName,filename,phase,taskNumber,numOtherPhase}

				ok := call(worker,"Worker.DoTask",&taskArgs,nil)
				go func(workerName string){
					mr.registerChannel <- workerName
				}(worker)
				if ok{
					break
				}
			}
		}(mr.jobName,filename,phase,i,nios)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
