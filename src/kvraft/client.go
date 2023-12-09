package kvraft

import (
	"6.5840/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId   int64
	seqNum     int
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	return ck
}

// fetch the current value for a Key.
// returns "" if the Key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	//println("client send get request", key)
	// You will have to modify this function.
	ck.seqNum++
	for {
		s, done := ck.sendOneGet(key, ck.lastLeader)
		if done {
			return s
		}
		for i := 0; i < len(ck.servers); i++ {
			s, done := ck.sendOneGet(key, i)
			if done {
				ck.lastLeader = i
				return s
			}
		}
		time.Sleep(time.Duration(20) * time.Millisecond)
	}
}

func (ck *Clerk) sendOneGet(key string, i int) (string, bool) {
	timeout := time.Duration(500) * time.Millisecond
	args := GetArgs{key, ck.clientId, ck.seqNum}
	reply := GetReply{}
	doneChan := make(chan bool)
	go func() {
		doneChan <- ck.servers[i].Call("KVServer.Get", &args, &reply)
	}()
	select {
	case ok := <-doneChan:
		if ok {
			if reply.Err == OK {
				ck.lastLeader = i
				return reply.Value, true
			}
		}
	case <-time.After(timeout):
		// println("get timeout")
	}
	return "", false
}

// shared by Put and AppendStr.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//println("client send request", key, value)
	ck.seqNum++
	for {
		if ck.sendOnePutAppend(key, value, op, ck.lastLeader) {
			return
		}
		for i := 0; i < len(ck.servers); i++ {
			//println("client", "key:", key, "value,", value)
			if ck.sendOnePutAppend(key, value, op, i) {
				return
			}
		}
		time.Sleep(time.Duration(20) * time.Millisecond)
	}
}

func (ck *Clerk) sendOnePutAppend(key string, value string, op string, i int) bool {
	// println("send to ", i)
	timeout := time.Duration(500) * time.Millisecond
	args := PutAppendArgs{key, value, op, ck.clientId, ck.seqNum}
	reply := PutAppendReply{}
	doneChan := make(chan bool)
	go func() {
		doneChan <- ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
	}()
	select {
	case ok := <-doneChan:
		if ok {
			if reply.Err == OK {
				ck.lastLeader = i
				// println("ok")
				return true
			}
		}
	case <-time.After(timeout):
		//println("send timeout")
	}
	return false
}
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, AddStr)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendStr)
}
