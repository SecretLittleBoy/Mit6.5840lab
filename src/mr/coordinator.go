package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)
var mu sync.Mutex
type CoordinatorPhase int
const (
	MapPhase CoordinatorPhase = iota
	ReducePhase
	ExitPhase
)
type TaskInfo struct {
	TaskReference *Task
	startTime time.Time
	isFinished bool
	isExcuting bool
}

type Coordinator struct {
	// Your definitions here.
	InputFiles    []string
	nReduce int
	intermediateFilenamesForPerReduce [][]string // [reduceTaskNumber][intermediateFilenames]。第一维度长度是nReduce，第二维度是这个reduceTaskNumber对应的所有intermediateFilenames
	CoordinatorPhase CoordinatorPhase
	Taskqueue chan *Task
	TaskInfos map[int]*TaskInfo
}


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) TaskExample(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	reply.TaskType = mapTask
	reply.TaskNumber = 1
	reply.FilenameForMap = "pg-*.txt"
	reply.NReduce = 2
	reply.IntermediateFilenames = []string{"mr-0-0", "mr-0-1"}
	reply.IntermediateFilenames = []string{"mr-1-0", "mr-1-1"}
	return nil
}
func (c *Coordinator) GetTask(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	if(len(c.Taskqueue) > 0) {
		*reply = *<-c.Taskqueue
		c.TaskInfos[reply.TaskNumber].startTime = time.Now()
		c.TaskInfos[reply.TaskNumber].isExcuting = true
	} else if(c.CoordinatorPhase != ExitPhase){
		reply.TaskType = wait
	} else {
		reply.TaskType = exit
	}
	log.Println("Coordinator Get(put) task: ", reply)
	return nil
}
func (c *Coordinator) FinishTask(args *Task, reply *ExampleReply) error {
	log.Println("Coordinator FinishTask first line:Finish task ing: ", args)
	mu.Lock()
	defer mu.Unlock()
	log.Println("Coordinator Finish task ing: ", args)
	if(args.TaskType != c.TaskInfos[args.TaskNumber].TaskReference.TaskType || c.TaskInfos[args.TaskNumber].isFinished) {
		return nil
	}
	c.TaskInfos[args.TaskNumber].isFinished = true
	c.TaskInfos[args.TaskNumber].isExcuting = false
	if args.TaskType == mapTask {
		for i, filename := range args.IntermediateFilenames {
			c.intermediateFilenamesForPerReduce[i] = append(c.intermediateFilenamesForPerReduce[i], filename)
		}
		if(c.allTasksFinished()) {
			c.CoordinatorPhase = ReducePhase
			c.createReduceTasks()
		}
	}else if args.TaskType == reduceTask {
		if(c.allTasksFinished()) {
			c.CoordinatorPhase = ExitPhase
		}
	}
	return nil
}
func (c *Coordinator) allTasksFinished() bool {
	for _, taskInfo := range c.TaskInfos {
		if !taskInfo.isFinished {
			return false
		}
	}
	return true
}
//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	return c.CoordinatorPhase == ExitPhase
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles: files,
		nReduce: nReduce,
		intermediateFilenamesForPerReduce: make([][]string, nReduce),
		CoordinatorPhase: MapPhase,
		Taskqueue: make(chan *Task, max(len(files), nReduce)),
		TaskInfos: make(map[int]*TaskInfo),
	}
	c.createMapTasks()
	c.server()
	go c.catchTaskTimeout()
	return &c
}
func (c *Coordinator) createMapTasks() {
	for i, filename := range c.InputFiles {
		task :=Task{
			TaskType: mapTask,
			TaskNumber: i,
			FilenameForMap: filename,
			NReduce: c.nReduce,
			IntermediateFilenames: []string{},
		}
		c.Taskqueue <- &task
		c.TaskInfos[i] = &TaskInfo{
			TaskReference: &task,
			isFinished: false,
			isExcuting: false,
		}
	}
}
func (c *Coordinator) createReduceTasks() {
	log.Println("createReduceTasks ing")
	for i := 0; i < c.nReduce; i++ {
		log.Println("createReduceTasking:i=", i)
		task :=Task{
			TaskType: reduceTask,
			TaskNumber: i,
			NReduce: c.nReduce,
			IntermediateFilenames: c.intermediateFilenamesForPerReduce[i],
		}
		c.Taskqueue <- &task
		log.Println("createReduceTasking:task=", task)
		c.TaskInfos[i] = &TaskInfo{
			TaskReference: &task,
			isFinished: false,
		}
	}
}

func (c *Coordinator) catchTaskTimeout() {
	for {
		time.Sleep(4 * time.Second)
		log.Println("catchTaskTimeout ing,mu.Lock() ing")
		mu.Lock()
		log.Println("catchTaskTimeout ing,mu.Lock() excuted")
		if c.CoordinatorPhase == ExitPhase {
			log.Println("catchTaskTimeout ing,mu.Unlock() ing")
			mu.Unlock()
			log.Println("catchTaskTimeout ing,mu.Unlock() excuted")
			return
		}
		for _, taskInfo := range c.TaskInfos {
			if taskInfo.isExcuting && !taskInfo.isFinished && time.Since(taskInfo.startTime) > 20 * time.Second {
				c.Taskqueue <- taskInfo.TaskReference
				taskInfo.startTime = time.Now()
			}
		}
		log.Println("catchTaskTimeout ing,mu.Unlock() ing")
		mu.Unlock()
		log.Println("catchTaskTimeout ing,mu.Unlock() excuted")
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}