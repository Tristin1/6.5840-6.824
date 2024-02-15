package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		flag, reply := askForATask()
		//没有收到任务或者执行请求失败
		if !flag {
			// fmt.Println("请求任务过程中出现问题")
		} else {
			flag = handleTask(mapf, reducef, reply)
			if !flag {
				// fmt.Println("处理任务过程中遇到问题")
				time.Sleep(time.Second)
			}
		}
	}
}

func askForATask() (bool, AskTaskReply) {
	request := AskTaskRequest{1}
	reply := AskTaskReply{}
	ok := call("Coordinator.AskForATask", &request, &reply)
	return ok, reply
}

/*
先创建成临时文件，当文件已经全部写好的时候再用原子操作改为中间文件，防止程序在写文件中的中途crash
*/
func handleTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	reply AskTaskReply) bool {
	if reply.TaskType == MapTask {
		done := handleMapTask(mapf, reply)
		if !done {
			return done
		}
	} else if reply.TaskType == ReduceTask { // reduce Task
		done := handleReduceTask(reply, reducef)
		if !done {
			return done
		}
	} else {
		// fmt.Println("暂时请求不到任务")
		return false
	}
	return true
}

func handleReduceTask(reply AskTaskReply, reducef func(string, []string) string) bool {
	intermediate := readIntermediateFileToKV(reply.MapNum, reply.TaskNum)
	sort.Sort(ByKey(intermediate))
	oname := reduceIntermediateKv(reply, intermediate, reducef)
	flag := writeIntermediateKVToFinalFile(oname, reply)
	if !flag {
		fmt.Println("reduce Task中写入最终文件失败")
		return flag
	}
	JobDoneNotify(1, reply.TaskNum, 1)
	return true
}

func handleMapTask(mapf func(string, string) []KeyValue, reply AskTaskReply) bool {
	intermediate := dealWithFileContentToKV(mapf, reply.FileName)
	flag := createTempFileForNMapTask(reply)
	if !flag {
		fmt.Println("map Task中创建文件失败")
		return false
	}
	writeIntermediateKVToTempFileThroughHash(intermediate, reply)
	modifyTempToIntermediateFile(reply)
	JobDoneNotify(0, reply.TaskNum, 1)
	return true
}

func writeIntermediateKVToFinalFile(oname string, reply AskTaskReply) bool {
	err := os.Rename(oname, "mr-out-"+strconv.Itoa(reply.TaskNum))
	if err != nil {
		fmt.Println(err)
		return false
	}
	for i := 0; i < reply.MapNum; i++ {
		oname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskNum)
		err := os.Remove(oname)
		// fmt.Println(oname)
		if err != nil {
			fmt.Println(err)
		}
	}
	return true
}

func reduceIntermediateKv(reply AskTaskReply, intermediate []KeyValue, reducef func(string, []string) string) string {
	oname := "mr-out-" + strconv.Itoa(reply.TaskNum) + strconv.Itoa(os.Getpid())
	ofile, _ := os.Create(oname)
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	return oname
}

func readIntermediateFileToKV(MapNum, TaskNum int) []KeyValue {
	intermediate := []KeyValue{}
	for i := 0; i < MapNum; i++ {
		oname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(TaskNum)
		ofile, err := os.Open(oname)
		if err != nil {
			fmt.Println("cannot Open reduce file")
		}
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		err2 := ofile.Close()
		if err2 != nil {
			fmt.Println(err2)
		}
	}
	return intermediate
}

func modifyTempToIntermediateFile(reply AskTaskReply) {
	for i := 0; i < reply.ReduceNum; i++ {
		oname := "mr-" + strconv.Itoa(reply.TaskNum) + "-" + strconv.Itoa(i)
		if err := os.Rename(oname+"-tmp"+strconv.Itoa(os.Getpid()), oname); err != nil {
			fmt.Println("临时文件重命名失败" + oname)
		}
	}
}

func writeIntermediateKVToTempFileThroughHash(intermediate []KeyValue, reply AskTaskReply) {
	// 将k-v根据hash函数写入到reduceNum个文件当中
	for _, value := range intermediate {
		// fmt.Println(value)
		i := ihash(value.Key) % reply.ReduceNum
		oname := "mr-" + strconv.Itoa(reply.TaskNum) + "-" + strconv.Itoa(i) + "-tmp" + strconv.Itoa(os.Getpid())
		ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			fmt.Println("map Task中 写入文件时出现错误")
		}
		// fmt.Println(ofile)
		enc := json.NewEncoder(ofile)
		err2 := enc.Encode(&value)
		if err2 != nil {
			fmt.Println(err2)
			fmt.Println("encoder 失败")
		}
		err3 := ofile.Close()
		if err3 != nil {
			fmt.Println(err3)
		}
	}
}

func createTempFileForNMapTask(reply AskTaskReply) bool {
	for i := 0; i < reply.ReduceNum; i++ {
		// fmt.Println(i)
		oname := "mr-" + strconv.Itoa(reply.TaskNum) + "-" + strconv.Itoa(i) + "-tmp" + strconv.Itoa(os.Getpid())
		if _, err := os.Create(oname); err != nil {
			fmt.Println("创建文件失败")
			return false
		}
	}
	return true
}

func dealWithFileContentToKV(mapf func(string, string) []KeyValue, filename string) []KeyValue {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("worker 64 : cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("worker 68 : cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	return intermediate
}

func JobDoneNotify(TaskType, TaskNum, status int) {
	msg := JobDoneMsg{TaskType, TaskNum, status}
	re := JobDoneMsgRE{}
	// todo 根据re处理一些信息
	call("Coordinator.JobDoneResponse", &msg, &re)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	// 连接不上默认任务已经完成
	if err != nil {
		// log.Fatal("dialing:", err)
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}
