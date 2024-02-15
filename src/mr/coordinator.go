package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// maptask和reducetask的总数量
	mapTaskNum    int
	reduceTaskNum int

	// 已经完成的maptask和reducetask的数量，帮助快速判断reduceTask是不是还需要派发
	// 注意计数值需要用同步锁来保护
	mapCount    int
	reduceCount int
	mapMu       sync.Mutex
	reduceMu    sync.Mutex
	reduceCond  *sync.Cond

	// 生产者消费者模型 派发任务通道
	reduceCh chan int
	mapCh    chan int

	// 标记reduce task 和 map task，防止重复派发
	mapDone    []bool
	reduceDone []bool

	FileName map[int]string
}

func NewCoordinator(mapNum, reduceNum int) *Coordinator {
	return &Coordinator{
		mapTaskNum:    mapNum,
		reduceTaskNum: reduceNum,
		mapDone:       make([]bool, mapNum),
		reduceDone:    make([]bool, reduceNum),
		reduceCount:   0,
		reduceCh:      make(chan int, reduceNum),
		mapCh:         make(chan int, mapNum),
		FileName:      make(map[int]string),
	}
}

// Your code here -- RPC handlers for the worker to call.

// 服务端派发任务 -1代表目前没任务可派发，0：map Task 1：reduce task
func (c *Coordinator) AskForATask(request *AskTaskRequest, reply *AskTaskReply) error {
	var taskNum, taskType int
	select {
	case taskNum = <-c.reduceCh:
		taskType = 1
	case taskNum = <-c.mapCh:
		taskType = 0
		reply.FileName = c.FileName[taskNum]
	default:
		taskType = -1
	}
	reply.TaskType = taskType
	reply.TaskNum = taskNum
	reply.ReduceNum = c.reduceTaskNum
	reply.MapNum = c.mapTaskNum
	return nil
}
func (c *Coordinator) findNeededReduceTask() {
	c.reduceMu.Lock()
	for c.mapCount < c.mapTaskNum {
		c.reduceCond.Wait()
	}
	c.reduceMu.Lock()
	for i, value := range c.reduceDone {
		if !value {
			c.reduceCh <- i
		}
	}
	c.reduceMu.Unlock()
}

// 服务端任务标记	客户端任务完成后，会发送消息到服务端，服务端进行标记防止重复派发
func (c *Coordinator) JobDoneResponse(msg *JobDoneMsg, re *JobDoneMsgRE) error {
	if msg.TaskType == 0 {
		c.mapMu.Lock()
		c.mapDone[msg.TaskNum] = true
		c.mapCount++
		// 也可以再开一个线程用条件变量来实现
		if c.mapCount >= c.mapTaskNum && c.reduceCount < c.reduceTaskNum {
			for i, value := range c.reduceDone {
				if !value {
					c.reduceCh <- i
				}
			}
		}
		c.mapMu.Unlock()
	} else {
		c.reduceMu.Lock()
		c.reduceDone[msg.TaskNum] = true
		c.reduceCount++
		c.reduceMu.Unlock()
	}
	return nil
}

// 找到可派发的任务派发下去，过20s重复派发一次，因为有的任务可能crash了，crash后需要重新派发
func (c *Coordinator) findNeededExTask() {
	for {
		for i, value := range c.mapDone {
			if !value {
				c.mapCh <- i
			}
		}
		// 所有map都已经执行完毕 可以开始执行reduce任务了
		if c.mapCount == c.mapTaskNum && c.reduceCount < c.reduceTaskNum {
			for i, value := range c.reduceDone {
				if !value {
					c.reduceCh <- i
				}
			}
		}
		time.Sleep(20 * time.Second)
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.reduceCount == c.reduceTaskNum
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := NewCoordinator(len(files), nReduce)
	c.reduceCond = sync.NewCond(&c.reduceMu)
	for i, filename := range files[0:] {
		c.FileName[i] = filename
	}
	// Your code here.
	go c.findNeededExTask()
	c.server()
	return c
}
