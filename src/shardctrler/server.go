package shardctrler

import (
	"6.5840/raft"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs   []Config // indexed by config num
	configNum int
	clientSeq map[int64]*ClientMsg
}
type ClientMsg struct {
	SeqNum int
	Config Config
}
type Op struct {
	// Your data here.
	OpType   string
	ClientId int64
	SeqNum   int
	ArgsNum  int
	Servers  map[int][]string
	GIDs     []int
	Shard    int
	Gid      int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {

	sc.mu.Lock()
	msg := sc.clientSeq[args.ClientId]
	if msg != nil && args.SeqNum == msg.SeqNum {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	op := newOp(JOIN, args.ClientId, args.SeqNum, 0)
	op.Servers = args.Servers
	_, _, b := sc.rf.Start(op)
	if b == false {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		if sc.waitAns(args, reply, msg) {
			return
		}
	}
}

func (sc *ShardCtrler) waitAns(args *JoinArgs, reply *JoinReply, msg *ClientMsg) bool {
	sc.mu.Lock()
	if _, isLeader := sc.rf.GetState(); isLeader {
		msg := sc.clientSeq[args.ClientId]
		if msg != nil && msg.SeqNum == args.SeqNum {
			reply.Err = OK
			sc.mu.Unlock()
			return true
		}
		sc.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return true
	}
	time.Sleep(5 * time.Millisecond)
	return false
}

func (sc *ShardCtrler) newJoinConf(Servers map[int][]string) Config {
	original := sc.configs[sc.configNum-1]
	newConf := Config{
		Num:    sc.configs[sc.configNum-1].Num,
		Shards: original.Shards, // 数组会自动深度复制
		Groups: make(map[int][]string),
	}
	// 手动深度复制映射
	for key, value := range original.Groups {
		// 复制切片
		groupCopy := make([]string, len(value))
		copy(groupCopy, value)
		// 将复制后的切片赋值给新的映射
		newConf.Groups[key] = groupCopy
	}
	for key, value := range Servers {
		groupCopy := make([]string, len(value))
		copy(groupCopy, value)
		// 将复制后的切片赋值给新的映射
		newConf.Groups[key] = groupCopy
	}
	// 创建一个 int 类型的切片用于存储 map 的键
	var keys []int

	// 遍历 map，将键添加到切片中
	for k := range newConf.Groups {
		keys = append(keys, k)
	}
	// 对切片进行排序
	sort.Ints(keys)
	index := 0
	for index < len(newConf.Shards) {
		for _, key := range keys {
			newConf.Shards[index] = key
			index++
			if index >= len(newConf.Shards) {
				break
			}
		}
	}
	return newConf
}

func newOp(opType string, clientId int64, seqNum int, argsNum int) Op {
	return Op{
		OpType:   opType,
		ClientId: clientId,
		SeqNum:   seqNum,
		ArgsNum:  argsNum,
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	msg := sc.clientSeq[args.ClientId]
	if msg != nil && args.SeqNum == msg.SeqNum {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	op := newOp(LEAVE, args.ClientId, args.SeqNum, 0)
	op.GIDs = args.GIDs
	_, _, b := sc.rf.Start(op)
	if b == false {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		if sc.waitLeaveAns(args, reply, msg) {
			return
		}
	}
}

func (sc *ShardCtrler) waitLeaveAns(args *LeaveArgs, reply *LeaveReply, msg *ClientMsg) bool {
	sc.mu.Lock()
	if _, isLeader := sc.rf.GetState(); isLeader {
		msg = sc.clientSeq[args.ClientId]
		if msg != nil && msg.SeqNum == args.SeqNum {
			reply.Err = OK
			sc.mu.Unlock()
			return true
		}
		sc.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return true
	}
	time.Sleep(5 * time.Millisecond)
	return false
}

func (sc *ShardCtrler) newLeaveConf(GIDs []int) Config {
	original := sc.configs[sc.configNum-1]
	newConf := Config{
		Num:    sc.configs[sc.configNum-1].Num,
		Shards: original.Shards, // 数组会自动深度复制
		Groups: make(map[int][]string),
	}

	// 手动深度复制映射
	for key, value := range original.Groups {
		// 复制切片
		groupCopy := make([]string, len(value))
		copy(groupCopy, value)
		// 将复制后的切片赋值给新的映射
		newConf.Groups[key] = groupCopy
	}
	for _, value := range GIDs {
		delete(newConf.Groups, value)
	}
	var keys []int

	// 遍历 map，将键添加到切片中
	for k := range newConf.Groups {
		keys = append(keys, k)
	}
	// 对切片进行排序
	sort.Ints(keys)
	if len(newConf.Groups) != 0 {
		index := 0
		for index < len(newConf.Shards) {
			for _, key := range keys {
				newConf.Shards[index] = key
				index++
				if index >= len(newConf.Shards) {
					break
				}
			}
		}
	}
	return newConf
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	msg := sc.clientSeq[args.ClientId]
	if msg != nil && args.SeqNum == msg.SeqNum {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	op := newOp(MOVE, args.ClientId, args.SeqNum, 0)
	op.Gid = args.GID
	op.Shard = args.Shard
	_, _, b := sc.rf.Start(op)
	if b == false {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		if sc.waitMoveAns(args, reply, msg) {
			return
		}
	}
}

func (sc *ShardCtrler) waitMoveAns(args *MoveArgs, reply *MoveReply, msg *ClientMsg) bool {
	sc.mu.Lock()
	if _, isLeader := sc.rf.GetState(); isLeader {
		msg = sc.clientSeq[args.ClientId]
		if msg != nil && msg.SeqNum == args.SeqNum {
			reply.Err = OK
			sc.mu.Unlock()
			return true
		}
		sc.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return true
	}
	time.Sleep(5 * time.Millisecond)
	return false
}

func (sc *ShardCtrler) newMoveConf(Shard, GID int) Config {
	original := sc.configs[sc.configNum-1]
	newConf := Config{
		Num:    sc.configs[sc.configNum-1].Num,
		Shards: original.Shards, // 数组会自动深度复制
		Groups: make(map[int][]string),
	}

	// 手动深度复制映射
	for key, value := range original.Groups {
		// 复制切片
		groupCopy := make([]string, len(value))
		copy(groupCopy, value)
		// 将复制后的切片赋值给新的映射
		newConf.Groups[key] = groupCopy
	}
	newConf.Shards[Shard] = GID
	return newConf
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	sc.mu.Lock()
	msg := sc.clientSeq[args.ClientId]
	if msg != nil && args.SeqNum == msg.SeqNum {
		reply.Err = OK
		reply.Config = sc.clientSeq[args.ClientId].Config
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	op := newOp(QUERY, args.ClientId, args.SeqNum, args.Num)
	_, _, b := sc.rf.Start(op)

	if b == false {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		if sc.waitQueryAns(args, reply, msg) {
			return
		}
	}
}

func (sc *ShardCtrler) waitQueryAns(args *QueryArgs, reply *QueryReply, msg *ClientMsg) bool {
	sc.mu.Lock()
	if _, isLeader := sc.rf.GetState(); isLeader {
		if msg == nil {
		}
		if msg != nil && msg.SeqNum != args.SeqNum {
		}
		msg = sc.clientSeq[args.ClientId]
		if msg != nil && msg.SeqNum == args.SeqNum {
			reply.Err = OK
			reply.Config = msg.Config
			sc.mu.Unlock()
			return true
		}
		sc.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return true
	}
	time.Sleep(5 * time.Millisecond)
	return false
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) keepReadingCommit() {
	for apply := range sc.applyCh {
		sc.mu.Lock()
		op, isCommand := apply.Command.(Op)
		Debug(dController, "S%d reveive msg", sc.me)
		if !isCommand {
		} else {
			Debug(dController, "S%d reveive from raft, index: %d", sc.me, apply.CommandIndex)
			if msg, ok := sc.clientSeq[op.ClientId]; ok {
				if msg.SeqNum >= op.SeqNum {
					sc.mu.Unlock()
					continue
				}
				msg.SeqNum = op.SeqNum
			} else {
				sc.clientSeq[op.ClientId] = &ClientMsg{op.SeqNum, Config{}}
			}
			sc.dealWithCommand(op)
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) dealWithCommand(op Op) {
	if op.OpType == QUERY {
		if op.ArgsNum < 0 || op.ArgsNum >= len(sc.configs) {
			sc.clientSeq[op.ClientId].Config = sc.configs[sc.configNum-1]
		} else {
			sc.clientSeq[op.ClientId].Config = sc.configs[op.ArgsNum]
		}
	} else if op.OpType == LEAVE {
		newConf := sc.newLeaveConf(op.GIDs)
		sc.modifyConfig(newConf)
		//print("leave: ")
		//for _, value := range newConf.Shards {
		//	print(value, " ")
		//}
		//println(len(sc.configs))
	} else if op.OpType == MOVE {
		newConf := sc.newMoveConf(op.Shard, op.Gid)
		//print("move: ")
		//for _, value := range newConf.Shards {
		//	print(value, " ")
		//}
		//println(len(sc.configs))
		sc.modifyConfig(newConf)
	} else if op.OpType == JOIN {
		newConf := sc.newJoinConf(op.Servers)
		sc.modifyConfig(newConf)
		//print("join: ")
		//for _, value := range newConf.Shards {
		//	print(value, " ")
		//}
		//println(len(sc.configs) - 1)
	}
}

func (sc *ShardCtrler) modifyConfig(config Config) {
	config.Num = sc.configNum
	sc.configNum++
	configs := append(sc.configs, config)
	sc.configs = configs
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configNum = 1
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.clientSeq = make(map[int64]*ClientMsg)
	go sc.keepReadingCommit()
	return sc
}
