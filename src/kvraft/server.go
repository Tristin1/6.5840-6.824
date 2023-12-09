package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug1 = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug1 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string
	Key      string
	Value    string
	ClientId int64
	SeqId    int
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	// Your definitions here.
	KVState     map[string]string
	latestIndex int
	clientSeq   map[int64]*ClientMsg
}
type ClientMsg struct {
	SeqNum int
	Value  string
}

func newOp(opType, key, value string, clientId int64, seqId int) Op {
	return Op{
		OpType:   opType,
		Key:      key,
		Value:    value,
		ClientId: clientId,
		SeqId:    seqId,
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	raft.Debug(raft.DKVServer, "S%d get message, key :", kv.me)
	kv.mu.Lock()
	msg := kv.clientSeq[args.ClientId]
	if msg != nil && args.SeqId == msg.SeqNum {
		reply.Value = msg.Value
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := newOp(GetStr, args.Key, "", args.ClientId, args.SeqId)
	_, _, b := kv.rf.Start(op)
	if b == false {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			msg = kv.clientSeq[args.ClientId]
			if msg != nil && msg.SeqNum == args.SeqId {
				reply.Value = msg.Value
				reply.Err = OK
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// println("key: " + args.Key + " value: " + args.Value)
	raft.Debug(raft.DKVServer, "S%d get message, key :%d, value: %d", kv.me, args.Key, args.Value)
	kv.mu.Lock()
	msg := kv.clientSeq[args.ClientId]
	if msg != nil && args.SeqId <= msg.SeqNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := newOp(args.Op, args.Key, args.Value, args.ClientId, args.SeqId)
	start, term, b := kv.rf.Start(op)
	raft.Debug(raft.DKVServer, "S%d add message put append to log, index :%d, term :%d", kv.me, start, term)
	if b == false {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			msg = kv.clientSeq[args.ClientId]
			if msg != nil && msg.SeqNum == args.SeqId {
				reply.Err = OK
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) keepReadingCommit() {
	for apply := range kv.applyCh {
		kv.mu.Lock()
		op, isCommand := apply.Command.(Op)
		raft.Debug(raft.DKVServer, "S%d reveive msg", kv.me)
		if !isCommand {
			kv.dealWithSnapShot(apply)
		} else {
			raft.Debug(raft.DKVServer, "S%d reveive from raft, index: %d", kv.me, apply.CommandIndex)
			kv.latestIndex = apply.CommandIndex
			if msg, ok := kv.clientSeq[op.ClientId]; ok {
				if msg.SeqNum >= op.SeqId {
					kv.mu.Unlock()
					continue
				}
				msg.SeqNum = op.SeqId
			} else {
				kv.clientSeq[op.ClientId] = &ClientMsg{op.SeqId, ""}
			}
			kv.dealWithCommand(op)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) dealWithCommand(op Op) {
	if op.OpType == AppendStr {
		kv.KVState[op.Key] = kv.KVState[op.Key] + op.Value
		raft.Debug(raft.DKVServer, "S%d reveive append msg from raft, key: %s, value:%s", kv.me, op.Key, op.Value)
	} else if op.OpType == GetStr {
		kv.clientSeq[op.ClientId].Value = kv.KVState[op.Key]
		raft.Debug(raft.DKVServer, "S%d reveive get msg from raft, key: %s, value:%s", kv.me, op.Key, kv.KVState[op.Key])
	} else if op.OpType == AddStr {
		kv.KVState[op.Key] = op.Value
		raft.Debug(raft.DKVServer, "S%d reveive add msg from raft, key: %s, value:%s", kv.me, op.Key, kv.KVState[op.Key])
	}
}

func (kv *KVServer) dealWithSnapShot(apply raft.ApplyMsg) {
	kv.latestIndex = apply.SnapshotIndex
	r := bytes.NewBuffer(apply.Snapshot)
	d := labgob.NewDecoder(r)
	var KVState map[string]string
	var clientSeq map[int64]*ClientMsg
	if d.Decode(&KVState) != nil || d.Decode(&clientSeq) != nil {
		raft.Debug(raft.DKVServer, "S%d get snapshot and decode fail", kv.me)
	} else {
		kv.KVState = KVState
		kv.clientSeq = clientSeq
	}
}

func (kv *KVServer) keepSnapShot(persister *raft.Persister) {
	for {
		kv.mu.Lock()
		if len(persister.ReadRaftState()) > kv.maxraftstate {
			raftBytesBuffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(raftBytesBuffer)
			err := encoder.Encode(kv.KVState)
			if err != nil {
				raft.Debug(raft.DKVServer, "S%d persist snapshot error", kv.me)
				return
			}
			err = encoder.Encode(kv.clientSeq)
			if err != nil {
				raft.Debug(raft.DKVServer, "S%d persist clientSeq error", kv.me)
				return
			}
			raftState := raftBytesBuffer.Bytes()
			kv.rf.Snapshot(kv.latestIndex, raftState)
		}
		kv.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant Key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.KVState = make(map[string]string)
	kv.clientSeq = make(map[int64]*ClientMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.keepReadingCommit()
	if maxraftstate != -1 {
		go kv.keepSnapShot(persister)
	}
	return kv
}
