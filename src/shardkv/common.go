package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongConfig = "ErrWrongConfig"
)

const (
	GetStr    = "GetStr"
	AppendStr = "AppendStr"
	AddStr    = "AddStr"
	Snapshot  = "SnapShot"
	Config    = "Config"
	Shard     = "Shard"
	ReShard   = "ReShard"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	ShardId  int
	SeqNum   int
	ClientId int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ShardId  int
	SeqNum   int
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardArgs struct {
	ConfigNum int
	ShardId   int
	Gid       int
	SeqNum    int
	ClientId  int64
}

type ShardReply struct {
	Err       Err
	Shard     map[string]string
	ClientSeq map[int64]*ClientMsg
}
