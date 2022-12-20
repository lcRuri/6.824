package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ArgsWorkID struct {
	WorkID int64
}

//to get content
//daxie!!!!
type Reply struct {
	Filename     string
	NReduce      int
	Stage        int
	MapTaskID    int
	ReduceTaskID int
	Intermediate []string
}

//
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
	// Your worker implementation here.
	workID := time.Now().Unix()
	for {
		intermediate := []KeyValue{}
		reply := AskForTask(workID)

		switch reply.Stage {
		case Map:
			kva := MapWorker(reply.Filename, mapf)
			intermediate = append(intermediate, kva...)
			tmpFileName := StoreIntermediate(reply, intermediate)
			reply.Intermediate = tmpFileName
		case Reduce:
			//fmt.Printf("Reduce Work ID:%d\n", reply.ReduceTaskID)
			//fmt.Printf("reply intermediate:%v\n", reply)
			ReduceWorker(reply, reducef)
		case Wait:
			//fmt.Println("Waiting")
			time.Sleep(time.Second)
		case Done:
			//fmt.Printf("Worker Job is Done\n")
			time.Sleep(2 * time.Second)
			break
		}
		if reply.Stage == Done {
			//fmt.Printf("Worker Job is Done\n")
			break
		}
		JobDone(reply)

	}

	//fmt.Println(intermediate[:10])

}

func MapWorker(filename string, mapf func(string, string) []KeyValue) []KeyValue {

	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("os.Open(reply.filename) failed err:%v", err)

	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("ioutil.ReadAll(file) failed err:%v", err)
	}
	file.Close()

	kva := mapf(filename, string(content))

	//when finish call coordinator to delete info
	return kva

}

func ReduceWorker(reply Reply, reducef func(string, []string) string) {
	//read from intermediate
	//reply.Intermediate
	intermediate := readFromLocal(reply)

	dir, _ := os.Getwd()
	ofile, _ := ioutil.TempFile(dir, "mr-out-*")
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tmpName := fmt.Sprintf("mr-out-%d", reply.ReduceTaskID)
	os.Rename(ofile.Name(), tmpName)
}

func readFromLocal(reply Reply) (kva []KeyValue) {
	for _, filename := range reply.Intermediate {
		//fmt.Println(filename)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("readFromLocal os.Open(filename) err:%v\n", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	return
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func AskForTask(WorkerId int64) (reply Reply) {
	args := ArgsWorkID{WorkID: WorkerId}
	//reply := Reply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Printf("reply.Filename:%v,reply.nReduce:%d,reply.ReduceTaskID:%d,reply.Stage:%d\n", reply.Filename, reply.NReduce, reply.ReduceTaskID, reply.Stage)
		//fmt.Printf("reply:%v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply
}

func JobDone(msg Reply) (reply Reply) {
	reply = Reply{}
	ok := call("Coordinator.TaskDone", &msg, &reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Printf("reply.Filename:%v,reply.nReduce:%d,reply.ReduceTaskID:%d,reply.Stage:%d\n", reply.Filename, reply.NReduce, reply.ReduceTaskID, reply.Stage)
	} else {
		fmt.Printf("call failed!\n")
	}

	return
}

func StoreIntermediate(reply Reply, intermediates []KeyValue) []string {
	//sort.Sort(ByKey(intermediates))

	//Classify hashes according to key
	buffer := make([][]KeyValue, reply.NReduce)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % reply.NReduce
		buffer[slot] = append(buffer[slot], intermediate)
	}

	dir, _ := os.Getwd()
	mapoutName := make([]string, 0)
	//write to disk
	for i := 0; i < reply.NReduce; i++ {
		file, err := ioutil.TempFile(dir, "mr-tmp-*")
		enc := json.NewEncoder(file)
		for _, kv := range buffer[i] {
			err = enc.Encode(&kv)
			if err != nil {
				fmt.Printf("enc.Encode(&kv) err:%v\n", err)
			}
		}
		file.Close()
		tmpName := fmt.Sprintf("mr-%d-%d", reply.MapTaskID, i)
		os.Rename(file.Name(), tmpName)
		mapoutName = append(mapoutName, filepath.Join(dir, tmpName))
	}

	return mapoutName
}
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
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
		return true
	}

	fmt.Println(err)
	return false
}
