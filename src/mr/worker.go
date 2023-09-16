package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type taskType int
const (
	mapTask =0
	reduceTask =1
	wait =2
	exit =3
)
type Task struct {
	FilenameForMap string
	TaskType taskType
	NReduce int
	TaskNumber int
	IntermediateFilenames []string
	ReducerOutputFilename string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	//CalltaskExample()
	// Your worker implementation here.
	for{
		task := CallgetTask()
		switch task.TaskType {
		case mapTask:
			mapper(&task, mapf)
		case reduceTask:
			reducer(&task, reducef)
		case wait:
			time.Sleep(5 * time.Second)
		case exit:
			return
		}
	}
}

func mapper(task *Task, mapf func(string, string) []KeyValue) {
	log.Println("worker in mapper ing...")
	content, err := ioutil.ReadFile(task.FilenameForMap)
	if err != nil {
		log.Fatal("Failed to read file: "+task.FilenameForMap, err)
		//todo , call coordinator to report error
	}
	kva := mapf(task.FilenameForMap, string(content))
	for i := 0; i < task.NReduce; i++ {
		intermediate := []KeyValue{}
		for _, kv := range kva {
			if ihash(kv.Key) % task.NReduce == i {
				intermediate = append(intermediate, kv)
			}
		}
		task.IntermediateFilenames = append(task.IntermediateFilenames, WriteIntermediate(task.TaskNumber, i, intermediate))
	}
	CallFinishTask(task)
}


func reducer(task *Task, reducef func(string, []string) string) {
	intermediate := *ReadIntermediate(task.IntermediateFilenames)
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	outputFileFullName := filepath.Join(dir, fmt.Sprintf("mr-out-%d", task.TaskNumber))
	outputFile, err := os.Create(outputFileFullName)
	if err != nil {
		log.Fatal("Failed to create file: "+outputFileFullName, err)
	}
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	outputFile.Close()
	task.ReducerOutputFilename = outputFileFullName
	CallFinishTask(task)
}

func WriteIntermediate(taskNumber int, reduceNumber int, intermediate []KeyValue) string {
	filename := fmt.Sprintf("mr-%d-%d", taskNumber, reduceNumber)
	dir,_ := os.Getwd()
	file, err := ioutil.TempFile(dir, filename)
	if err != nil {
		log.Fatal("Failed to create temp file: "+filename, err)
	}
	for _, kv := range intermediate {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
	file.Close()
	return file.Name()
}
func ReadIntermediate(filenames []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("Failed to open file: "+filename, err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatal("Failed to read file: "+filename, err)
		}
		file.Close()
		for _, line := range strings.Split(string(content), "\n") {
			if line == "" {
				continue
			}
			kv := KeyValue{}
			fmt.Sscanf(line, "%s %s", &kv.Key, &kv.Value)
			kva = append(kva, kv)
		}
	}
	return &kva
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
func CalltaskExample() Task {
	args := ExampleArgs{}
	reply := Task{}
	ok := call("Coordinator.TaskExample", &args , &reply)
	log.Println("worker TaskExample: ", reply)
	if !ok {
		reply.TaskType = wait
	}
	return reply
}

func CallgetTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	ok := call("Coordinator.GetTask", &args , &reply)
	log.Println("worker Get task: ", reply)
	if !ok {
		reply.TaskType = wait
	}
	return reply
}
func CallFinishTask(task *Task) {
	log.Println("worker finish task ing...")
	args := task
	reply := ExampleReply{}
	ok := call("Coordinator.FinishTask", &args , &reply)
	log.Println("worker finish task:",task,reply)
	if !ok {
		log.Fatal("Failed to finish task: ", task)
	}
}
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		log.Println("worker call: ", rpcname, args, reply)
		return true
	}
	
	fmt.Println(err)
	return false
}
