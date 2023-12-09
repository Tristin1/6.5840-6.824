package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	GetStr    = "GetStr"
	AppendStr = "AppendStr"
	AddStr    = "AddStr"
	Snapshot  = "SnapShot"
)

type OperationType string
type Err string

// Put or AppendStr
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "AppendStr"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqId    int
}

type GetReply struct {
	Err   Err
	Value string
}
