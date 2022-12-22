package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Map    = 1
	Reduce = 2
	Wait   = 3
	Done   = 4
)

type Coordinator struct {
	// Your definitions here.
	TaskChannel   chan string //record all task
	ReduceChannel chan int
	TaskList      map[string]int   //record all task with worker
	WorkerList    map[string]int64 //now how many worker is doing
	Intermediate  [][]string       //store intermediate data
	Stage         int
	nReduce       int
	//WaitChannel   map[int64]int //阻塞队列
}

var mu sync.Mutex

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
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
	ret := false
	mu.Lock()
	defer mu.Unlock()
	// Your code here.

	if c.Stage == Done {
		//if len(c.ReduceChannel) != 0 || len(c.WorkerList) != 0 {
		//	c.Stage = Reduce
		//} else {
		ret = true
		//}

		//fmt.Printf("The Task is Done\n")
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("Creating Coordinator...")
	c := Coordinator{
		TaskChannel:   make(chan string, len(files)),
		TaskList:      make(map[string]int, len(files)),
		WorkerList:    map[string]int64{},
		Intermediate:  make([][]string, len(files)),
		Stage:         Map,
		nReduce:       nReduce,
		ReduceChannel: make(chan int, nReduce),
		//WaitChannel:   make(map[int64]int),
	}

	//fmt.Println(c.nReduce)
	// Your code here.
	//Task split
	for i, filename := range files {
		c.TaskChannel <- filename
		c.TaskList[filename] = i

	}

	for i := 0; i < nReduce; i++ {
		c.ReduceChannel <- i
	}
	c.server()
	return &c
}

//AssignTask assign task filename to worker
func (c *Coordinator) AssignTask(args *ArgsWorkID, reply *Reply) error {
	//fmt.Println(len(c.TaskChannel))
	if c.Stage == Map {
		//avoid parallel race
		if len(c.TaskChannel) == 0 {
			mu.Lock()
			//if len(c.WorkerList) == 0 {
			//	reply.Stage = Reduce
			//} else {
			//	reply.Stage = Wait
			//}
			reply.Stage = Wait

			mu.Unlock()
			//c.WaitChannel[args.WorkID]=1

		} else {
			mu.Lock()
			//assign task
			//取不出来会阻塞
			tmp := <-c.TaskChannel
			reply.Filename = tmp
			reply.NReduce = c.nReduce
			reply.Stage = Map
			reply.MapTaskID = c.TaskList[tmp]
			//fmt.Printf("reply.MapTaskID:%d\n", reply.MapTaskID)
			//delete(c.WaitChannel,args.WorkID)
			//record wordId with content
			c.WorkerList[tmp] = args.WorkID
			mu.Unlock()
			//fmt.Printf("AssignTask.Map:worker ID:%v,stage:%d,reply.nReduce:%d,c.nReduce:%d\n", args.WorkID, c.Stage, reply.NReduce, c.nReduce)
			//fmt.Printf("AssignTask.Map:c.WorkerList:%d,c.TaskChannel:%d\n", len(c.WorkerList), len(c.TaskChannel))
		}

	} else if c.Stage == Reduce {
		if len(c.ReduceChannel) == 0 /**&& len(c.WorkerList) == 0**/ {
			mu.Lock()
			//fmt.Println("go to check all task is finish")
			//for len(c.WorkerList) != 0 || len(c.TaskList) != 0 {
			//	time.Sleep(time.Second)
			//}
			//c.Stage = Done
			reply.Stage = Done
			//reply.ReduceTaskID=-1
			mu.Unlock()
			//time.Sleep(time.Second)
			//fmt.Printf("The Task is Done\n")
			return nil
		} else {
			//取不出来会阻塞
			//if len(c.ReduceChannel) == 0 {
			//	reply.Stage = Wait
			//	//c.WaitChannel[args.WorkID]=1
			//	return nil
			//}
			mu.Lock()
			reply.Stage = Reduce
			reduceID := <-c.ReduceChannel
			//fmt.Println(c.Intermediate)
			tmp := []string{}
			for i := 0; i < len(c.Intermediate); i++ {
				f1 := c.Intermediate[i][reduceID]
				tmp = append(tmp, f1)
			}
			reply.Intermediate = tmp
			reply.ReduceTaskID = reduceID
			//delete(c.WaitChannel,args.WorkID)
			c.WorkerList[string(reduceID)] = args.WorkID
			c.TaskList[string(reduceID)] = int(args.WorkID)
			mu.Unlock()
			//fmt.Println(reduceID)
			//fmt.Printf("AssignTask.Reduce:c.WorkerList:%d,c.ReduceChannel:%d,reduceID:%d\n", len(c.WorkerList), len(c.ReduceChannel), reduceID)
		}

	} else if c.Stage == Done {
		//fmt.Printf("The Task is Done\n")
		mu.Lock()
		reply.Stage = Done
		mu.Unlock()
	}

	return nil
}

func (c *Coordinator) TaskDone(args *Reply, reply *Reply) error {

	if c.Stage == Map {
		mu.Lock()
		delete(c.TaskList, args.Filename)
		delete(c.WorkerList, args.Filename)
		reply.Stage = c.Stage
		//fmt.Printf("TaskDone:reply.MapTaskID:%d\n", args.MapTaskID)
		c.Intermediate[args.MapTaskID] = args.Intermediate
		//fmt.Printf("TaskDone.Map:task:%v is finish\n", args.Filename)
		mu.Unlock()
	} else if c.Stage == Reduce {
		mu.Lock()
		delete(c.TaskList, string(args.ReduceTaskID))
		delete(c.WorkerList, string(args.ReduceTaskID))
		reply.Stage = c.Stage
		//fmt.Printf("TaskDone.Reduce:task:%v is finish\n", args.ReduceTaskID)
		mu.Unlock()

	} else if c.Stage == Done {
		mu.Lock()
		reply.Stage = Done
		mu.Unlock()
		return nil
	}
	//等待所以map结束进入reduce阶段
	if len(c.TaskChannel) == 0 && len(c.ReduceChannel) >= c.nReduce {

		if len(c.WorkerList) != 0 {
			mu.Lock()
			reply.Stage = Wait
			mu.Unlock()
			return nil
		} else {
			//mu.Lock()
			//fmt.Printf("c.WorkerList:%d,c.TaskChannel:%d\n", len(c.WorkerList), len(c.TaskChannel))
			//fmt.Println("Goto reducing")
			//c.Stage = Reduce
			//reply.Stage = Reduce
			//mu.Unlock()
			//return nil
			//mu.Lock()

			if len(c.WorkerList) == 0 && c.Stage == Map {
				//fmt.Printf("c.WorkerList:%d,c.TaskChannel:%d\n", len(c.WorkerList), len(c.TaskChannel))
				//fmt.Println("Goto reducing", len(c.Intermediate[0]))
				mu.Lock()
				c.Stage = Reduce
				mu.Unlock()
			}

			//mu.Unlock()
		}

	}
	//fmt.Printf("Intermediate:", c.Intermediate[0:1])

	//如果reduce channel为空,让worker进程结束
	if /**len(c.WorkerList) == 0 &&**/ len(c.ReduceChannel) == 0 {
		//fmt.Printf("TaskDone.Done:c.WorkerList:%d,c.ReduceChannel:%d,c.Stage:%d\n", len(c.WorkerList), len(c.ReduceChannel), c.Stage)
		//fmt.Println("Job is Done")
		mu.Lock()
		reply.Stage = Done
		//c.Stage = Done
		mu.Unlock()
		//fmt.Printf("TaskDone.Done:c.WorkerList:%d,c.ReduceChannel:%d,c.Stage:%d\n", len(c.WorkerList), len(c.ReduceChannel), c.Stage)

		if len(c.ReduceChannel) == 0 && len(c.WorkerList) == 0 {
			mu.Lock()
			c.Stage = Done
			//fmt.Println("go change c stage to done")
			mu.Unlock()
		}

	}
	return nil
}
