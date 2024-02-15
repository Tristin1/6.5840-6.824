package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	Key       string
	Value     string
	ClientId  int64
	SeqId     int
	Shard     map[string]string
	Config    shardctrler.Config
	ShardId   int
	ConfigNum int
	ClientSeq map[int64]*ClientMsg
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	latestIndex  int
	mck          *shardctrler.Clerk
	clientId     int64
	seqNum       int
	// Your definitions here.
	KVState       map[int]map[string]string
	clientSeq     map[int64]*ClientMsg
	config        shardctrler.Config
	transferShard [shardctrler.NShards]bool
	hasRecorded   map[int]map[int]bool
}

type ClientMsg struct {
	SeqNum int
	Value  string
	Error  Err
	Shard  map[string]string
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	Debug2(kv, DClientRequest, "S%d group: %d shardId: %d get message, key :%s", kv.me, kv.gid, args.ShardId, args.Key)
	kv.mu.Lock()
	if kv.config.Shards[args.ShardId] != kv.gid || kv.transferShard[args.ShardId] {
		if kv.config.Shards[args.ShardId] != kv.gid {
			Debug2(kv, DClientRequest, "S%d group: %d shardId: %d reject get message, key: %s, because not this Gid", kv.me, kv.gid, args.ShardId, args.Key)
		} else {
			Debug2(kv, DClientRequest, "S%d group: %d shardId: %d get message, key :%s because this shard is transferring", kv.me, kv.gid, args.ShardId, args.Key)
		}
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	msg := kv.clientSeq[args.ClientId]
	if msg != nil && args.SeqNum == msg.SeqNum {
		reply.Value = msg.Value
		reply.Err = OK
		Debug2(kv, DClientRequest, "S%d group: %d shardId: %d reject get msg because of duplicate", kv.me, kv.gid, args.ShardId)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := newOp(GetStr, args.Key, "", args.ClientId, args.SeqNum)
	op.ShardId = args.ShardId
	_, _, b := kv.rf.Start(op)
	if b == false {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			msg = kv.clientSeq[args.ClientId]
			if msg != nil && (msg.SeqNum == args.SeqNum || msg.Error == ErrWrongGroup) {
				if msg.Error == ErrWrongGroup {
					reply.Err = ErrWrongGroup
					msg.Error = OK
					Debug2(kv, DClientRequest, "S%d group: %d shardId: %d reject get msg because of duplicate", kv.me, kv.gid, args.ShardId)
				} else {
					reply.Err = OK
					reply.Value = msg.Value
					Debug2(kv, DClientRequest, "S%d group: %d shardId: %d reject get msg success", kv.me, kv.gid, args.ShardId)
				}
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	Debug2(kv, DClientRequest, "S%d group: %d shardId: %d put message, key :%s, value %s", kv.me, kv.gid, args.ShardId, args.Key, args.Value)
	kv.mu.Lock()
	if kv.config.Shards[args.ShardId] != kv.gid || kv.transferShard[args.ShardId] {
		if kv.config.Shards[args.ShardId] != kv.gid {
			Debug2(kv, DClientRequest, "S%d group: %d shardId: %d reject put message, key: %s, value %s because not this Gid", kv.me, kv.gid, args.ShardId, args.Key, args.Value)
		} else {
			Debug2(kv, DClientRequest, "S%d group: %d shardId: %d put message, key :%s , value %s because this shard is transferring", kv.me, kv.gid, args.ShardId, args.Key, args.Value)
		}
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	msg := kv.clientSeq[args.ClientId]
	if msg != nil && args.SeqNum == msg.SeqNum {
		reply.Err = OK
		kv.mu.Unlock()
		Debug2(kv, DClientRequest, "S%d group: %d shardId: %d reject put msg because of duplicate", kv.me, kv.gid, args.ShardId)
		return
	}
	kv.mu.Unlock()
	op := newOp(args.Op, args.Key, args.Value, args.ClientId, args.SeqNum)
	op.ShardId = args.ShardId
	_, _, b := kv.rf.Start(op)
	if b == false {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			msg = kv.clientSeq[args.ClientId]
			if msg != nil && (msg.SeqNum == args.SeqNum || msg.Error == ErrWrongGroup) {
				if msg.Error == ErrWrongGroup {
					reply.Err = ErrWrongGroup
					msg.Error = OK
					Debug2(kv, DClientRequest, "S%d group: %d shardId: %d reject put msg because of errwrong group", kv.me, kv.gid, args.ShardId)
				} else {
					reply.Err = OK
					Debug2(kv, DClientRequest, "S%d group: %d shardId: %d put msg success", kv.me, kv.gid, args.ShardId)
				}
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

func (kv *ShardKV) Shard(args *ShardArgs, reply *ShardReply) {
	kv.mu.Lock()
	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrWrongConfig
		Debug2(kv, DConfig, "S%d group: %d shardId: %d need to send to group: %d rejected, because this configNum too small, thisNum :%d, requesterNum:%d", kv.me, kv.gid, args.ShardId, kv.config.Num, args.ConfigNum)
		kv.mu.Unlock()
		return
	}
	msg := kv.clientSeq[args.ClientId]
	if msg != nil && args.SeqNum == msg.SeqNum {
		reply.Shard = msg.Shard
		reply.ClientSeq = kv.clientSeq
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{}
	op.OpType = ReShard
	op.ClientId = args.ClientId
	op.SeqId = args.SeqNum
	op.ShardId = args.ShardId
	start, term, b := kv.rf.Start(op)
	Debug2(kv, DConfig, "S%d group: %d shardId: %d need to send to group: %d, and get to raft log: at term: %d, index: %d", kv.me, kv.gid, args.ShardId, args.Gid, term, start)
	if b == false {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			msg = kv.clientSeq[args.ClientId]
			if msg != nil && msg.SeqNum == args.SeqNum {
				reply.Shard = msg.Shard
				reply.ClientSeq = kv.clientSeq
				reply.Err = OK
				Debug2(kv, DKVServer, "S%d group: %d shardId: %d move shard to Gid : %d", kv.me, kv.gid, op.ShardId, args.Gid)
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

func (kv *ShardKV) keepReadingCommit() {
	for apply := range kv.applyCh {
		kv.mu.Lock()
		op, isCommand := apply.Command.(Op)
		Debug2(kv, DKVServer, "S%d group: %d receive msg", kv.me, kv.gid)
		if !isCommand {
			kv.dealWithSnapShot(apply)
		} else {
			Debug2(kv, DKVServer, "S%d group: %d receive from raft, index: %d", kv.me, kv.gid, apply.CommandIndex)
			kv.latestIndex = apply.CommandIndex
			// Shard 和 Config是自己加进来的不用经过判重表
			if op.OpType != Shard && op.OpType != Config {
				if msg, ok := kv.clientSeq[op.ClientId]; ok {
					if msg.SeqNum >= op.SeqId {
						Debug2(kv, DKVServer, "S%d group: %d receive from raft, index: %d duplicate and rejected", kv.me, kv.gid, apply.CommandIndex)
						kv.mu.Unlock()
						continue
					}
				} else {
					cm := &ClientMsg{}
					kv.clientSeq[op.ClientId] = cm
				}
			}
			kv.dealWithCommand(op)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) dealWithSnapShot(apply raft.ApplyMsg) {
	kv.latestIndex = apply.SnapshotIndex
	r := bytes.NewBuffer(apply.Snapshot)
	d := labgob.NewDecoder(r)
	var KVState map[int]map[string]string
	var clientSeq map[int64]*ClientMsg
	var config shardctrler.Config
	var recorded map[int]map[int]bool
	var transferShard [shardctrler.NShards]bool
	if d.Decode(&KVState) != nil || d.Decode(&clientSeq) != nil || d.Decode(&config) != nil || d.Decode(&recorded) != nil || d.Decode(&transferShard) != nil {
		raft.Debug(raft.DKVServer, "S%d get snapshot and decode fail", kv.me)
	} else {
		kv.KVState = KVState
		kv.clientSeq = clientSeq
		kv.config = config
		kv.hasRecorded = recorded
		kv.transferShard = transferShard
	}
}

func (kv *ShardKV) dealWithCommand(op Op) {
	if op.OpType == AppendStr {
		if kv.transferShard[op.ShardId] == true {
			kv.clientSeq[op.ClientId].Error = ErrWrongGroup
			return
		}
		kv.clientSeq[op.ClientId].SeqNum = op.SeqId
		if _, exists := kv.KVState[op.ShardId]; !exists {
			kv.KVState[op.ShardId] = make(map[string]string)
		}
		kv.KVState[op.ShardId][op.Key] = kv.KVState[op.ShardId][op.Key] + op.Value
		Debug2(kv, DKVServer, "S%d group: %d shardId: %d receive append msg from raft, key: %s, value:%s, clientId: %d, seqNum:%d", kv.me, kv.gid, op.ShardId, op.Key, op.Value, op.ClientId, op.SeqId)
	} else if op.OpType == GetStr {
		if kv.transferShard[op.ShardId] == true {
			kv.clientSeq[op.ClientId].Error = ErrWrongGroup
			return
		}
		kv.clientSeq[op.ClientId].SeqNum = op.SeqId
		kv.clientSeq[op.ClientId].Value = kv.KVState[op.ShardId][op.Key]
		Debug2(kv, DKVServer, "S%d group: %d shardId %d receive get msg from raft, key: %s, value:%s", kv.me, kv.gid, op.ShardId, op.Key, kv.KVState[op.ShardId][op.Key])
	} else if op.OpType == AddStr {
		if kv.transferShard[op.ShardId] == true {
			kv.clientSeq[op.ClientId].Error = ErrWrongGroup
			return
		}
		kv.clientSeq[op.ClientId].SeqNum = op.SeqId
		if _, exists := kv.KVState[op.ShardId]; !exists {
			kv.KVState[op.ShardId] = make(map[string]string)
		}
		kv.KVState[op.ShardId][op.Key] = op.Value
		Debug2(kv, DKVServer, "S%d group: %d shardId: %d receive add msg from raft, key: %s, value:%s", kv.me, kv.gid, op.ShardId, op.Key, kv.KVState[op.ShardId][op.Key])
	} else if op.OpType == Config {
		if kv.config.Num < op.Config.Num {
			kv.config = op.Config
			Debug2(kv, DKVServer, "S%d group: %d receive new config, update config from : %d to:%d", kv.me, kv.gid, kv.config.Num, op.Config.Num)
		} else {
			Debug2(kv, DKVServer, "S%d group: %d receive new config but reject because old config, update config from : %d to:%d", kv.me, kv.gid, kv.config.Num, op.Config.Num)
		}
	} else if op.OpType == Shard {
		if _, ok := kv.hasRecorded[op.ConfigNum]; !ok {
			kv.hasRecorded[op.ConfigNum] = make(map[int]bool)
		}
		if !kv.hasRecorded[op.ConfigNum][op.ShardId] {
			for clientId, value := range op.ClientSeq {
				if msg, ok := kv.clientSeq[clientId]; ok {
					if msg.SeqNum < value.SeqNum {
						cli := &ClientMsg{}
						cli.SeqNum = value.SeqNum
						cli.Value = value.Value
						cli.Error = value.Error
						kv.clientSeq[clientId] = cli
					}
				} else {
					cli := &ClientMsg{}
					cli.SeqNum = value.SeqNum
					cli.Value = value.Value
					cli.Error = value.Error
					kv.clientSeq[clientId] = cli
				}
			}
			originalMap := op.Shard
			newMap := make(map[string]string)
			for key, value := range originalMap {
				newMap[key] = value
			}
			kv.KVState[op.ShardId] = newMap
			kv.hasRecorded[op.ConfigNum][op.ShardId] = true
			kv.transferShard[op.ShardId] = false
			Debug2(kv, DKVServer, "S%d group: %d shardId: %d receive shard from raft", kv.me, kv.gid, op.ShardId)
		} else {
			Debug2(kv, DKVServer, "S%d group: %d shardId: %d receive shard old shard and rejected", kv.me, kv.gid, op.ShardId)
		}
	} else if op.OpType == ReShard {
		m, ok := kv.KVState[op.ShardId]
		if !ok {
			m = make(map[string]string)
		}
		kv.clientSeq[op.ClientId].Shard = m
		kv.transferShard[op.ShardId] = true
		kv.clientSeq[op.ClientId].SeqNum = op.SeqId
		Debug2(kv, DKVServer, "S%d group: %d shardId: %d need to move ", kv.me, kv.gid, op.ShardId)
	}
}

func (kv *ShardKV) keepSnapShot(persister *raft.Persister) {
	for {
		kv.mu.Lock()
		if len(persister.ReadRaftState()) > kv.maxraftstate {
			raftBytesBuffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(raftBytesBuffer)
			err := encoder.Encode(kv.KVState)
			if err != nil {
				Debug2(kv, DKVServer, "S%d persist snapshot error", kv.me)
				return
			}
			err = encoder.Encode(kv.clientSeq)
			if err != nil {
				Debug2(kv, DKVServer, "S%d persist ClientSeq error", kv.me)
				return
			}
			err = encoder.Encode(kv.config)
			if err != nil {
				Debug2(kv, DKVServer, "S%d persist config error", kv.me)
				return
			}
			err = encoder.Encode(kv.hasRecorded)
			if err != nil {
				Debug2(kv, DKVServer, "S%d persist recorded error", kv.me)
				return
			}
			err = encoder.Encode(kv.transferShard)
			if err != nil {
				Debug2(kv, DKVServer, "S%d persist transferShard error", kv.me)
				return
			}
			raftState := raftBytesBuffer.Bytes()
			kv.rf.Snapshot(kv.latestIndex, raftState)
		}
		kv.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

func (kv *ShardKV) keepReadingConfig() {
	for {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			nextConfigNum := kv.config.Num + 1
			newConfig := kv.mck.Query(nextConfigNum)
			kv.requestNewShards(newConfig)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) requestNewShards(newConfig shardctrler.Config) {
	if newConfig.Num > kv.config.Num {
		if newConfig.Num > 1 {
			for shardsId, gid := range newConfig.Shards {
				_, isLeader := kv.rf.GetState()
				if gid == kv.gid && isLeader {
					if kv.config.Shards[shardsId] != kv.gid {
						kv.sendRequestToGetShard(shardsId, newConfig.Num)
					}
				}
			}
		}
		op := Op{}
		op.Config = newConfig
		op.OpType = Config
		kv.rf.Start(op)
		Debug2(kv, DClientRequest, "S%d group: %d add new config : %d", kv.me, kv.gid, newConfig.Num)
	}
}

func (kv *ShardKV) sendRequestToGetShard(shardsId, configNum int) {
	belongGid := kv.config.Shards[shardsId]
	kv.seqNum++
	for {
		Debug2(kv, DKVServer, "S%d group: %d shardId: %d send request to Gid:%d to get shard", kv.me, kv.gid, shardsId, belongGid)
		if servers, ok := kv.config.Groups[belongGid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply ShardReply
				args := ShardArgs{ShardId: shardsId, SeqNum: kv.seqNum, ClientId: kv.clientId, Gid: kv.gid, ConfigNum: kv.config.Num}
				ok := srv.Call("ShardKV.Shard", &args, &reply)
				if ok && (reply.Err == OK) {
					op := Op{}
					originalMap := reply.Shard
					newMap := make(map[string]string)
					for key, value := range originalMap {
						newMap[key] = value
					}
					op.Shard = newMap
					op.ShardId = shardsId
					op.ConfigNum = configNum
					op.ClientSeq = reply.ClientSeq
					op.OpType = Shard
					kv.rf.Start(op)
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) CheckIsLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// Gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[Gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.KVState = make(map[int]map[string]string)
	kv.clientSeq = make(map[int64]*ClientMsg)
	kv.hasRecorded = make(map[int]map[int]bool)
	kv.config = shardctrler.Config{}
	kv.clientId = nrand()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.keepReadingCommit()
	go kv.keepReadingConfig()
	if maxraftstate != -1 {
		go kv.keepSnapShot(persister)
	}
	return kv
}
