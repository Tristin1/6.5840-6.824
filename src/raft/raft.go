package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	isLeader  bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persist Info
	peerInfo *PeerInfo // 持久化信息
	log      []Entry

	elapsedTime int64 // 过期时间

	// used to track log to commit
	commitIndex int
	lastApplied int

	// only used when is leader
	leaderInfo LeaderInfo

	peerNum int
}
type PeerInfo struct {
	CurrentTerm int
	VotedFor    int
}
type LeaderInfo struct {
	nextIndex  []int
	matchIndex []int
}
type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

func (rf *Raft) generateElapsedTime() {
	rand.Seed(time.Now().UnixNano())                                   // 初始化随机数种子
	randomMilliSeconds := rand.Intn(500) + 500                         // 生成一个1000到1999之间的随机数，代表毫秒
	currentMilliSeconds := time.Now().UnixMilli()                      // 获取当前时间的毫秒数
	newMilliSeconds := currentMilliSeconds + int64(randomMilliSeconds) // 将随机数加到当前的毫秒数上
	rf.elapsedTime = newMilliSeconds
}

func (rf *Raft) updateTerm(term int) {
	if term > rf.peerInfo.CurrentTerm {
		rf.peerInfo.CurrentTerm = term
		rf.peerInfo.VotedFor = -1
		if rf.isLeader {
			rf.generateElapsedTime()
			rf.isLeader = false
		}
	}
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	var Term int = rf.peerInfo.CurrentTerm
	var isleader bool = rf.isLeader
	// Your code here (2A).
	rf.mu.Unlock()
	return Term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) newRequestVoteArgs(CurrentTerm int) *RequestVoteArgs {
	request := &RequestVoteArgs{CurrentTerm, rf.me, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term}
	return request
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Entry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) judgeCurrentTermLarger(lastLogTerm, lastLogIndex int) bool {
	return rf.log[len(rf.log)-1].Term > lastLogTerm || (rf.log[len(rf.log)-1].Term == lastLogTerm && len(rf.log)-1 > lastLogIndex)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// todo 判断日志
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.peerInfo.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.peerInfo.CurrentTerm
		Debug(dElection, "S%d reject vote from % d because currentTerm larger", rf.me, args.CandidateId)
		return
	} else if args.Term >= rf.peerInfo.CurrentTerm {
		if rf.peerInfo.CurrentTerm == args.Term && rf.peerInfo.VotedFor != -1 {
			Debug(dElection, "S%d reject vote because this term has voted", rf.me)
			reply.VoteGranted = false
			return
		}

		rf.updateTerm(args.Term)
		// todo 判断日志是否一样新
		// 日志更新的规则：最新的entry的term更大，或者相同term但是长度更长
		// 先更新term
		// 判断日志
		currentTermLarger := rf.judgeCurrentTermLarger(args.LastLogTerm, args.LastLogIndex)
		if currentTermLarger {
			Debug(dElection, "S%d reject vote from % d because log larger", rf.me, args.CandidateId)
			reply.VoteGranted = false
			return
		}
		Debug(dElection, "S%d vote to %d", rf.me, args.CandidateId)
		rf.peerInfo.VotedFor = args.CandidateId
		reply.VoteGranted = true
		return
	}
}

// follower和leader有相同term，并且prev成功匹配则返回true，否则返回false
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.peerInfo.CurrentTerm
	if rf.peerInfo.CurrentTerm > args.Term {
		reply.Success = false
		Debug(dAppend, "S%d receive appendentries from leader %d, but currentTerm Larger", rf.me, args.LeaderId)
		return
	}
	// 如果当前term落后则更新term
	if args.Term > rf.peerInfo.CurrentTerm {
		rf.updateTerm(args.Term)
	}
	// 如果prevLogIndex匹配不了返回false
	rf.generateElapsedTime()
	if len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		Debug(dAppend, "S%d receive appendentries from leader %d, but log not match", rf.me, args.LeaderId)
		reply.Success = false
		return
	}

	reply.Success = true
	rf.log = rf.log[0 : args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	Debug(dAppend, "S%d receive appendentries from leader %d, add log from %d to index %d", rf.me, args.LeaderId, args.PrevLogIndex+1, len(rf.log))
	if len(rf.log) < args.LeaderCommit {
		rf.commitIndex = len(rf.log)
	} else {
		rf.commitIndex = args.LeaderCommit
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeatBeat() {
	currentTerm := rf.peerInfo.CurrentTerm
	// todo 多个线程可能同时跑
	for rf.isLeader && currentTerm == rf.peerInfo.CurrentTerm {
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//var log []Entry
			// var lenLog int
			if rf.isLeader {
				// leaderInfo = rf.leaderInfo
				currentTerm = rf.peerInfo.CurrentTerm
				// lenLog = len(rf.log)
				// log = make([]Entry, len(rf.log))
				//log = rf.log
			} else {
				return
			}
		}()
		for i := 0; i < rf.peerNum; i++ {
			if i == rf.me {
				continue
			}
			go rf.sendOneHeartBeat(currentTerm, i)
		}
		ms := 30 + (rand.Int63() % 50)
		//ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// rf.mu.Lock()
	}
	// rf.mu.Unlock()
}

func (rf *Raft) sendOneHeartBeat(currentTerm int, i int) {
	func(i int) {
		reply := &AppendEntriesReply{}
		request := rf.newAppendEntriesArgs(i, currentTerm)
		Success := rf.sendAppendEntries(i, request, reply)
		// 网络故障
		if !Success {
			Debug(dNetworkFail, "S%d try to send heartBeat but network fail", rf.me)
			return
		}
		rf.mu.Lock()
		if reply.Success {
			lens := request.PrevLogIndex + len(request.Entries)
			rf.leaderInfo.nextIndex[i] = lens + 1
			rf.leaderInfo.matchIndex[i] = lens
			Debug(dHeartBeat, "S%d heartbeat success from %d nextLog %d", rf.me, i, lens+1)
		} else {
			if reply.Term > currentTerm {
				Debug(dHeartBeat, "S%d heartbeat fail and updateTerm", rf.me)
				rf.updateTerm(reply.Term)
			} else {
				// todo continue
				Debug(dHeartBeat, "S%d heartbeat success but log not match ", rf.me)
				rf.leaderInfo.nextIndex[i]--
				go rf.sendOneHeartBeat(currentTerm, i)
			}
		}
		rf.mu.Unlock()
	}(i)
}

func (rf *Raft) newAppendEntriesArgs(i int, currentTerm int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dHeartBeat, "S%d send heartbeat to %d, from log %d to log %d", rf.me, i, rf.leaderInfo.nextIndex[i], len(rf.log))
	var entry []Entry = make([]Entry, 0)
	request := &AppendEntriesArgs{currentTerm, rf.me, 0, 0, rf.commitIndex, entry}
	if rf.leaderInfo.nextIndex[i] > len(rf.log) {
		return nil
	}

	request.Entries = rf.log[rf.leaderInfo.nextIndex[i]:]
	// println("prev", leaderInfo.nextIndex[i], rf.peerInfo.CurrentTerm)
	prevEntry := rf.log[rf.leaderInfo.nextIndex[i]-1]
	request.PrevLogTerm = prevEntry.Term
	request.PrevLogIndex = prevEntry.Index
	// rf.mu.Unlock()
	return request
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately.x there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.isLeader {
		return -1, -1, false
	}
	index := len(rf.log)
	Term := rf.peerInfo.CurrentTerm
	entry := Entry{Term, index, command}
	rf.log = append(rf.log, entry)
	Debug(dClientAdd, "S%d receive msg from client, add at index %d", rf.me, len(rf.log)-1)
	// Your code here (2B).
	return index, Term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) TimeOut() bool {
	return time.Now().UnixMilli() >= rf.elapsedTime
}
func (rf *Raft) checkTimeOutAndTryToBeLeader() {
	for rf.killed() == false {
		// few happen 如果是leader的情况下不需要考虑超时问题
		// 不是leader，并且超时了就需要重新选取
		if rf.TimeOut() && !rf.isLeader {
			Debug(dElection, "S%d start election", rf.me)
			rf.voteToSelfAndRequestForVote()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 只有在这个函数中才有可能成为leader
func (rf *Raft) voteToSelfAndRequestForVote() {
	rf.mu.Lock()
	// defer rf.mu.Lock()
	// 重新检查timeout，可能在加锁到前面一个检查之间timeout发生了变化
	if !rf.TimeOut() {
		rf.mu.Unlock()
		return
	}
	// 重新计时
	var countMu sync.Mutex
	candidateFlag := true
	// cond := sync.NewCond(&countMu)
	rf.generateElapsedTime()
	rf.peerInfo.CurrentTerm++
	rf.peerInfo.VotedFor = rf.me
	count := 1
	currentTerm := rf.peerInfo.CurrentTerm
	// log := rf.log
	// todo 持久化peerInfo
	// 给各个节点发送rpc请求票
	for i := 0; i < rf.peerNum; i++ {
		if i != rf.me {
			go func(currentTerm, i int) {
				Debug(dElection, "S%d request votes from %d, current term: %d", rf.me, i, currentTerm)
				// 只对term做快照就可以？
				request := rf.newRequestVoteArgs(currentTerm)
				args := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, request, args)
				if !ok {
					return
				}
				if args.VoteGranted {
					countMu.Lock()
					count++
					countMu.Unlock()
				} else {
					rf.mu.Lock()
					if args.Term > rf.peerInfo.CurrentTerm {
						Debug(dElection, "S%d election fail, because get Larger term ", rf.me)
						rf.updateTerm(args.Term)
						candidateFlag = false
					}
					rf.mu.Unlock()
				}
			}(currentTerm, i)
		}
	}
	rf.mu.Unlock()
	for currentTerm == rf.peerInfo.CurrentTerm && candidateFlag && count < (rf.peerNum+1)/2 && !rf.TimeOut() {
		ms := 2
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	// count 大于 peers 的一半 则说明当选为leader 可以开始发送心跳了
	// 有可能在选举过程中其它节点成为了leader，则该节点不能成为leader
	// 这里必须加锁，只能变为当前term的leader
	rf.mu.Lock()
	if currentTerm == rf.peerInfo.CurrentTerm && count >= (rf.peerNum+1)/2 {
		rf.convertToLeader()
		go rf.sendHeatBeat()
	}
	rf.mu.Unlock()
}

func (rf *Raft) convertToLeader() {
	Debug(dElection, "S%d is elected", rf.me)
	rf.isLeader = true
	nextIndex := make([]int, rf.peerNum)
	for i, _ := range nextIndex {
		nextIndex[i] = len(rf.log)
	}
	matchIndex := make([]int, rf.peerNum)
	leaderInfo := LeaderInfo{nextIndex, matchIndex}
	rf.leaderInfo = leaderInfo
}

func (rf *Raft) sendApply(ch chan ApplyMsg) {
	for {
		rf.mu.Lock()
		if rf.isLeader {
			l := len(rf.peers)
			var m int
			for i, _ := range rf.log {
				count := 0
				for _, val := range rf.leaderInfo.matchIndex {
					if val >= i {
						count++
					}
				}
				if count >= (l)/2 {
					m = i
				} else {
					break
				}
			}
			rf.commitIndex = m
			Debug(dCOMMITUPDATE, "S%d update commitId to %d", rf.me, m)
		}
		for rf.commitIndex > rf.lastApplied {
			msg := ApplyMsg{}
			rf.lastApplied++
			msg.CommandValid = true
			msg.CommandIndex = rf.lastApplied
			msg.Command = rf.log[rf.lastApplied].Command
			Debug(dCOMMITUPDATE, "S%d send message Index at %d to client", rf.me, rf.lastApplied)
			ch <- msg

		}
		rf.mu.Unlock()
		// ms := 10
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// println("len = ", len(peers))
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	// start ticker goroutine to start elections
	// 第零个位置放一个虚拟节点
	rf.peerNum = len(rf.peers)
	rf.readPersist(persister.ReadRaftState())
	pi := &PeerInfo{0, 0}
	rf.peerInfo = pi
	rf.generateElapsedTime()
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{0, 0, nil})
	leaderInfo := LeaderInfo{nil, nil}
	rf.leaderInfo = leaderInfo
	Debug(dINIT, "S%d created", rf.me)
	go rf.checkTimeOutAndTryToBeLeader()
	go rf.sendApply(applyCh)

	return rf
}
