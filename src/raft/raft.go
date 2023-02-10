package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"encoding/gob"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//当每个Raft对等体意识到连续的日志条目被提交时，对等体应该通过传递给Make()的applyCh向同一服务器上的服务(或测试器)发送一个ApplyMsg。
//setCommandValid为true，表示ApplyMsg包含一个新提交的日志条目。
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type ApplyMsg struct {
	CommandValid bool        //命令是否有效
	Command      interface{} //命令本身
	CommandIndex int         //命令的索引

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Leader = iota + 1
	Candidate
	Follower
)

var wg sync.WaitGroup

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state 锁
	peers     []*labrpc.ClientEnd // RPC end points of all peers 所有 Raft peers（包括这一个）的网络标识符数组
	persister *Persister          // Object to hold this peer's persisted state 对象来保存此对等体的持久状态
	me        int                 // this peer's index into peers[] 属于这个peer的网络标识符的的下标
	dead      int32               // set by Kill() 设置原来kill raft实例

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//图2描述了Raft服务器必须维护的状态。
	State          int //当前节点的身份
	lastActiveTime time.Time
	leaderId       int

	CurrentTerm int //server能看到的最新任期
	VoteFor     int //候选者Id(在当前任期里面收到的投票，没有为null)
	//wg          sync.WaitGroup
	LogEntry []LogEntry //日志条目 每个日志条目包含对状态机的命令当日志从leader那接收到

	//Volatile State
	CommitIndex int //已知的索引最高的日志条目将被提交
	LastApplied int //被应用的状态机索引最高的日志条目

	// 仅Leader，易失状态（成为leader时重置）
	nextIndex  []int //	每个follower的log同步起点索引（初始为leader log的最后一项）
	matchIndex []int // 每个follower的log同步进度（初始为0），和nextIndex强关联

	applyCh chan ApplyMsg

	lastIncludeIndex int
	lastIncludeTerm  int
	snapshot         []byte
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	if rf.State == Leader {
		return rf.CurrentTerm, true
	}
	return rf.CurrentTerm, false
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.LogEntry)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//返回值是error，不为空说明有错
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludeTerm) != nil || d.Decode(&lastIncludeIndex) != nil {
		DPrintf("readPersist failed")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VoteFor = voteFor
		rf.LogEntry = log
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm

	}
}

type InstallSnapshot struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte //快照块的原始字节，从偏移量开始
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//服务想要切换到快照。 只有在 Raft 没有这样做时才这样做
//拥有更新的信息，因为它在 applyCh 上传达快照。
//强制执行快照？

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
//index是leader提交后出现的，可能follower还没到那个索引
//Snapshot 进行快照操作 自己调用

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() == true {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//快照包含的最后一条日志的索引比index大
	if rf.lastIncludeIndex >= index || index > rf.LastApplied {
		DPrintf("[%d] has snapshot index=%d log", rf.me, rf.lastIncludeIndex)
		return
	}
	DPrintf("snapshot index:%d rf.lastIncludeIndex:%d", index, rf.lastIncludeIndex)
	oldLastIncludeIndex := rf.lastIncludeIndex
	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = rf.LogEntry[index-1-oldLastIncludeIndex].Term
	rf.snapshot = snapshot
	//删除index之前的所有日志
	rf.LogEntry = rf.LogEntry[index-oldLastIncludeIndex:]

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.LogEntry)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, snapshot)

	DPrintf("[%d] is Snapshot,lastIncludeIndex:%d,lastIncludeTerm:%d", rf.me, rf.lastIncludeIndex, rf.lastIncludeTerm)
	DPrintf("%d Log:%v", rf.me, rf.LogEntry)

}

//LeaderInstallSnapshot leader给follower发送自己的压缩的日志
func (rf *Raft) LeaderInstallSnapshot(server int) {

	rf.mu.Lock()
	args := &InstallSnapshot{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludeIndex,
		LastIncludedTerm:  rf.lastIncludeTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{Term: 0}

	ok := rf.sendInstallSnapshot(server, args, reply)
	DPrintf("[%d]->[%d] sendInstallSnapshot", rf.me, server)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.State != Leader || args.Term != rf.CurrentTerm {
			DPrintf("snapshot not leader")
			return
		}

		if reply.Term > rf.CurrentTerm {
			rf.State = Follower
			rf.CurrentTerm = reply.Term
			rf.lastActiveTime = time.Now()
			rf.persist()
			DPrintf("snapshot reply false reply.Term:%d > rf.CurrentTerm:%d", reply.Term, rf.CurrentTerm)
			return
		}

		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = args.LastIncludedIndex
			DPrintf("snapshot update [%d]matchInt:%d", server, rf.matchIndex[server])
		}

		if args.LastIncludedIndex+1 > rf.nextIndex[server] {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
			DPrintf("snapshot update [%d]nextInt:%d", server, rf.nextIndex[server])

		}
	} else {
		DPrintf("snapshot false")
	}
	rf.persist()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshot, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshot, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		DPrintf("InstallSnapshot false args.Term:%d rf.CurrentTerm:%d", args.Term, rf.CurrentTerm)
		return
	}

	reply.Term = args.Term
	rf.State = Follower
	rf.VoteFor = -1
	rf.CurrentTerm = args.Term
	rf.lastActiveTime = time.Now()
	rf.persist()

	//如果自身快照包含的最后一个日志>=leader快照包含的最后一个日志，就没必要接受了
	if rf.lastIncludeIndex >= args.LastIncludedIndex {
		DPrintf("InstallSnapshot false rf.lastIncludeIndex:%d >= args.LastIncludedIndex:%d", rf.lastIncludeIndex, args.LastIncludedIndex)
		rf.mu.Unlock()
		return
	}

	// 将快照后的logs切割，快照前的直接applied
	index := args.LastIncludedIndex
	tempLog := make([]LogEntry, 0)

	for i := index + 1; i <= len(rf.LogEntry); i++ {
		tempLog = append(tempLog, rf.LogEntry[i])
	}

	rf.lastIncludeIndex = args.LastIncludedIndex
	rf.lastIncludeTerm = args.LastIncludedTerm
	rf.LogEntry = tempLog

	if index > rf.CommitIndex {
		rf.CommitIndex = index
	}
	if index > rf.LastApplied {
		rf.LastApplied = index
	}
	rf.persist()
	DPrintf("[%d] CommitIndex:%d LastApplied:%d lastIncludeIndex:%d", rf.me, rf.CommitIndex, rf.LastApplied, rf.lastIncludeIndex)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.LogEntry)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, args.Data)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg
	DPrintf("[%d] Snapshot commit,SnapshotIndex:%d", rf.me, args.LastIncludedIndex)

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选者任期
	CandidateId  int //候选者Id
	LastLogIndex int //候选者最后的日志索引
	LastLogTerm  int //候选者最后的日志条目的期限
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //目前的期限
	VoteGranted bool //投票信息是否收到 true表示收到
}

type AppendEntries struct {
	Term     int //领导者的任期
	LeaderId int //so follower可以重定向客户端

	//用于日志复制，确保前面日志能够匹配
	PreLogIndex  int        //紧邻新日志条目的索引
	PreLogTerm   int        //上一页日志索引条目的任期
	Entries      []LogEntry //要存储的日志条目(检测心跳为空;为了提高效率，可以发送多个)
	LeaderCommit int        //领导者的提交索引
}

type ReceiveEntries struct {
	Term          int  //目前的任期，为了领导者去更新自己的任期
	Success       bool //如果追随者包含与 prevLogIndex 和 preLogTerm 匹配的条目，则为 true
	ConflictIndex int
	ConflictTerm  int
}

//
// example RequestVote RPC handler.
//
//RequestVote 请求投票 即发起投票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//某个节点请求投票，需要分发到这个网络里面的所有节点
	//网络中的节点调用了这个方法，希望得到投票
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		return
	}
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.State = Follower
		rf.VoteFor = -1

	}

	if rf.VoteFor == -1 || rf.VoteFor == args.CandidateId {
		lastLogTerm := 0
		if len(rf.LogEntry) != 0 {
			lastLogTerm = rf.LogEntry[len(rf.LogEntry)-1].Term
		}
		DPrintf("%d to %d --args.LastLogTerm:%d lastLogTerm:%d args.LastLogIndex:%d len(rf.LogEntry):%d", args.CandidateId, rf.me, args.LastLogTerm, lastLogTerm, args.LastLogIndex, len(rf.LogEntry))

		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.LogEntry)) {
			rf.VoteFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastActiveTime = time.Now()
		} else {
			reply.VoteGranted = false
		}
	}

	//选举完成后保存一下状态
	rf.persist()
	return
}

//发送RequestVote RPC到服务器的示例代码。
//Server是rf.peers[]中目标服务器的索引。
//在args中期望RPC参数。
//*reply用RPC reply填充，所以调用者应该传递&reply。
//传递给Call()的args和reply的类型必须与handler函数中声明的参数的类型相同(包括它们是否是指针)。
//
//labrpc包模拟了一个有损耗的网络，其中服务器可能不可达，请求和应答可能丢失。
//Call()发送请求并等待应答。如果应答在超时时间内到达，Call()返回true;否则，ecall()返回false。因此，Call()可能暂时不会返回。
//错误的返回可能是由一个死服务器、一个无法到达的活动服务器、一个丢失的请求或一个丢失的回复引起的。
//
//Call()保证返回(可能在延迟之后)*，除非服务器端的处理函数没有返回。因此，没有必要围绕Call()实现自己的超时。
//
//查看../labrpc/labrpc中的注释。了解更多细节。
//
//如果你在让RPC工作时遇到了麻烦，检查你是否在传递给RPC的结构中对所有字段名进行了大写，并且调用者使用&来传递应答结构的地址，而不是结构本身。
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	//需要进行群发
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//看看收到的选票结果
	return ok
}

//使用Raft的服务(例如k/v服务器)希望启动下一个命令的协议，以追加到Raft的日志中。
//如果这个服务器不是leader，返回false。否则启动协议并立即返回。
//因为领导人可能会失败或输掉选举，所以不能保证这个命令会被提交到Raft日志中。
//即使Raft实例已经被杀死，这个函数也应该优雅地返回。
//
//第一个返回值是命令提交后将出现的索引。第二个返回值是currentterm。如果该服务器认为它是leader，则第三个返回值为true。
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.State != Leader {
		return index, term, false
	}

	//如果是领导者
	//将当前的任期和命令提交到LogEntry中
	//通过这个将命令写入领导者里面
	//leader日志数组里面的实际的索引比认为的小1(因为是从0开始)
	rf.LogEntry = append(rf.LogEntry, LogEntry{Command: command, Term: rf.CurrentTerm})

	DPrintf("leader%d receive cmd:%v,term:%d,index:%d\n", rf.me, command, rf.CurrentTerm, len(rf.LogEntry)+rf.lastIncludeIndex)
	//返回命令提交后将出现的索引
	index = len(rf.LogEntry) + rf.lastIncludeIndex
	term = rf.CurrentTerm
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//测试人员不会在每次测试后停止由Raft创建的程序，
//但是它调用Kill()方法。你的代码可以使用killed()来检查Kill()是否被调用。atomic的使用避免了对锁的需求。
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//问题是长时间运行的gorout使用内存，可能会占用CPU时间，可能会导致以后的测试失败，并产生令人困惑的调试输出。
//任何具有长时间循环的goroutine都应该调用killed()来检查它是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
//如果这位peers没有收到心跳，他就会开始新的选举
func (rf *Raft) ticker() {
	//在节点没有死亡的情况下
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		//你的代码在这里检查是否领导人选举应该开始和随机睡眠时间使用
		//如果收到心跳的包，则为true，表示现在还有领导者，不进入选举，否则为false，进入选举
		//第一次需要等待
		//rf.wg.Wait()

		time.Sleep(time.Duration(rand.Intn(150)+150) * time.Millisecond)
		now := time.Now()

		timeout := time.Duration(rand.Int31n(600)+600) * time.Millisecond
		elapses := now.Sub(rf.lastActiveTime)
		//等待过程中没有收到leader的心跳或者选举的，发起选举
		if elapses <= timeout && rf.State != Leader {
			rf.State = Follower
			//time.Sleep(time.Millisecond * time.Duration(sleepTime))
			//DPrintf("[%d] waitTime is:%d, len rf.waitTime:%d,currentTerm:%d,State:%d", rf.me, sleepTime, len(rf.waitTime), rf.CurrentTerm, rf.State)
		}

		if rf.State == Follower && elapses > timeout {
			//DPrintf("%d time.Now().Sub(rf.lastActiveTime):%d", rf.me, time.Now().Sub(rf.lastActiveTime))
			rf.lastActiveTime = time.Now()
			//DPrintf("%d time.Now().Sub(rf.lastActiveTime):%d", rf.me, time.Now().Sub(rf.lastActiveTime))
			rf.mu.Lock()
			//更改自己的身份
			rf.State = Candidate
			//投票给自己
			rf.VoteFor = rf.me
			rf.CurrentTerm = rf.CurrentTerm + 1

			rf.persist()

			args := &RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.LogEntry),
			}
			if len(rf.LogEntry) == 0 {
				args.LastLogTerm = 0
			} else {
				args.LastLogTerm = rf.LogEntry[len(rf.LogEntry)-1].Term
			}

			rf.mu.Unlock()
			votes := 1
			//当前节点进入选举
			//sends out Request Vote messages to other nodes.
			DPrintf("[%d] attempting an election at term %d", rf.me, rf.CurrentTerm)
			Done := true
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if rf.State == Follower {
					DPrintf("Candidate%d become Follower term:%d ticker", rf.me, rf.CurrentTerm)
					break
				}
				if peerId == rf.me {
					continue
				}
				//需要等，不然for比go携程快，经典问题
				go rf.AskForVote(peerId, &votes, &Done, args)
				time.Sleep(10 * time.Millisecond)
			}

			//wg.Wait()
			if Done == true {
				votes = 1
				rf.State = Follower
				rf.lastActiveTime = time.Now()
			}
		}

	}
}
func (rf *Raft) AskForVote(peerId int, votes *int, Done *bool, args *RequestVoteArgs) {

	reply := &RequestVoteReply{VoteGranted: false}
	ok := rf.sendRequestVote(peerId, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.State != Candidate {
			DPrintf("%d is not candidate term:%d", rf.me, rf.CurrentTerm)
			if rf.State == Leader {
				*Done = false
			}
			return
		}
		DPrintf("reply.VoteGranted:%v,term:%d", reply.VoteGranted, args.Term)
		if reply.VoteGranted == true {
			*votes++
			DPrintf("%d vote to %d", peerId, rf.me)
			if *Done && *votes > len(rf.peers)/2 {
				rf.State = Leader
				DPrintf("[%d] we got enough votes, we are now leader (currentTerm=%d),votes:%d", rf.me, rf.CurrentTerm, *votes)
				//成为leader后需要初始化nextIndex和matchIndex
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.LogEntry) + 1
				}
				for i := 0; i < len(rf.peers); i++ {
					rf.matchIndex[i] = 0
				}

				*Done = false
				return
			}
		}
	} else {
		DPrintf("rf[%d].sendRequestVote(%d, args, reply) failed", rf.me, peerId)
		return
	}

}

func (rf *Raft) Listen() {

	for rf.killed() == false {

		for rf.State == Leader {

			//DPrintf("send heart")
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if rf.State != Leader {
					break
				}

				if peerId == rf.me {
					//DPrintf("not send to self")
					continue
				}

				if rf.nextIndex[peerId]-1 < rf.lastIncludeIndex {
					DPrintf("[%d]LeaderInstallSnapshot(%d) rf.nextIndex[peerId]:%d rf.lastIncludeIndex:%d", rf.me, peerId, rf.nextIndex[peerId], rf.lastIncludeIndex)
					go rf.LeaderInstallSnapshot(peerId)
				} else {
					rf.mu.Lock()
					args := &AppendEntries{
						Term:         rf.CurrentTerm,
						LeaderId:     rf.me,
						LeaderCommit: rf.CommitIndex,
						Entries:      make([]LogEntry, 0),
					}

					DPrintf("%d ---> %d,nextIndex:%d,args.PreLogIndex:%d", rf.me, peerId, rf.nextIndex[peerId], rf.nextIndex[peerId]-1)
					//todo
					args.PreLogIndex = rf.nextIndex[peerId] - 1

					//fmt.Printf("args:%v", args)
					//最新的新日志为第0个，那么之前日志就不存在
					if args.PreLogIndex-rf.lastIncludeIndex > 0 {
						args.PreLogTerm = rf.LogEntry[args.PreLogIndex-1-rf.lastIncludeIndex].Term
					} else if args.PreLogIndex == rf.lastIncludeIndex {
						args.PreLogTerm = rf.lastIncludeTerm
					}
					args.Entries = append(args.Entries, rf.LogEntry[rf.nextIndex[peerId]-1-rf.lastIncludeIndex:]...)

					DPrintf("PreLogIndex:%d PreLogTerm:%d Entries:%v", args.PreLogIndex, args.PreLogTerm, args.Entries)
					rf.mu.Unlock()

					go rf.Heart(peerId, args)
					time.Sleep(10 * time.Millisecond)
				}

			}
			rf.persist()
			time.Sleep(50 * time.Millisecond)
		}

	}

}

func (rf *Raft) Heart(peerId int, args *AppendEntries) {

	reply := &ReceiveEntries{Term: -1, Success: false}
	ok := rf.sendHeart(peerId, args, reply)

	DPrintf("LeaderHeart %d to %d,ok:%v", rf.me, peerId, ok)
	rf.mu.Lock()
	if ok {
		if reply.Term > rf.CurrentTerm {
			rf.State = Follower
			rf.CurrentTerm = reply.Term
			rf.VoteFor = -1
			rf.leaderId = -1

			rf.persist()

			rf.lastActiveTime = time.Now()
			DPrintf("find high term not leader")
			rf.mu.Unlock()
			return
		}
		if reply.Success == true {

			//可能将部分截断，和之前的nextIndex不一样，所以不能直接加上len(args.Entries)
			//todo
			rf.nextIndex[peerId] = args.PreLogIndex + len(args.Entries) + 1
			rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
			DPrintf("rf.nextIndex[%d]:%d,rf.matchIndex[%d]:%d", peerId, rf.nextIndex[peerId], peerId, rf.matchIndex[peerId])

			//更新commitIndex,
			sortArr := make([]int, 0)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				sortArr = append(sortArr, rf.matchIndex[i])
			}

			sort.Ints(sortArr)
			newCommitIndex := sortArr[len(rf.peers)/2]
			if newCommitIndex > rf.CommitIndex && rf.LogEntry[newCommitIndex-1-rf.lastIncludeIndex].Term == rf.CurrentTerm {
				rf.CommitIndex = newCommitIndex
				DPrintf("Leader%d CommitIndex Updated is %d", rf.me, rf.CommitIndex)
			}

			//fmt.Println(sortArr)
		} else {
			//nextIndexBefore := rf.nextIndex[peerId] // 仅为打印log

			if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term不同
				conflictTermIndex := -1
				for index := args.PreLogIndex - rf.lastIncludeIndex; index >= 1; index-- { // 找最后一个conflictTerm
					if rf.LogEntry[index-1].Term == reply.ConflictTerm {
						conflictTermIndex = index
						break
					}
				}
				if conflictTermIndex != -1 { // leader也存在冲突term的日志，则从term最后一次出现之后的日志开始尝试同步，因为leader/follower可能在该term的日志有部分相同
					rf.nextIndex[peerId] = conflictTermIndex + 1
				} else { // leader并没有term的日志，那么把follower日志中该term首次出现的位置作为尝试同步的位置，即截断follower在此term的所有日志
					rf.nextIndex[peerId] = reply.ConflictIndex
				}
			} else { // follower的prevLogIndex位置没有日志
				rf.nextIndex[peerId] = reply.ConflictIndex + 1
			}
			DPrintf("heart false rf.nextIndex[%d]:%d,rf.matchIndex[%d]:%d", peerId, rf.nextIndex[peerId], peerId, rf.matchIndex[peerId])
		}

	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeart(peerId int, args *AppendEntries, reply *ReceiveEntries) bool {
	ok := rf.peers[peerId].Call("Raft.LeaderHeart", args, reply)
	return ok

}

func (rf *Raft) LeaderHeart(args *AppendEntries, reply *ReceiveEntries) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		DPrintf("LeaderHeart args.Term:%d < rf.CurrentTerm:%d", args.Term, rf.CurrentTerm)
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.State = Follower
		rf.VoteFor = -1
		rf.leaderId = -1
		reply.Term = rf.CurrentTerm
		rf.persist()
	}

	rf.leaderId = args.LeaderId
	rf.lastActiveTime = time.Now()

	DPrintf("[%d]->[%d] len(rf.LogEntry):%d args.PreLogIndex:%d args.PreLogTerm:%d", args.LeaderId, rf.me, len(rf.LogEntry), args.PreLogIndex, args.PreLogTerm)
	//2B
	//处理和日志有关的
	if len(rf.LogEntry) < args.PreLogIndex-rf.lastIncludeIndex {
		reply.ConflictIndex = len(rf.LogEntry)
		return
	}

	// 如果本地有前一个日志的话，那么term必须相同，否则false
	if args.PreLogIndex-rf.lastIncludeIndex > 0 && rf.LogEntry[args.PreLogIndex-1-rf.lastIncludeIndex].Term != args.PreLogTerm {
		reply.ConflictTerm = rf.LogEntry[args.PreLogIndex-1-rf.lastIncludeIndex].Term
		for index := 1; index <= args.PreLogIndex-rf.lastIncludeIndex; index++ { // 找到冲突term的首次出现位置，最差就是PrevLogIndex
			if rf.LogEntry[index-1].Term == reply.ConflictTerm {
				reply.ConflictIndex = index
				break
			}
		}
		return
	}

	//会进行强制覆盖重写
	//important
	for i, entry := range args.Entries {
		index := args.PreLogIndex + i + 1 - rf.lastIncludeIndex
		if index > len(rf.LogEntry) {
			rf.LogEntry = append(rf.LogEntry, entry)
		} else {
			if rf.LogEntry[index-1].Term != entry.Term {
				DPrintf("log not same")
				rf.LogEntry = rf.LogEntry[:index-1]
				rf.LogEntry = append(rf.LogEntry, entry)
			}
		}
	}
	rf.persist()

	reply.Success = true

	//压缩之后本地日志长度改变
	//todo
	if rf.CommitIndex < args.LeaderCommit {
		rf.CommitIndex = args.LeaderCommit
		if len(rf.LogEntry) < rf.CommitIndex {
			rf.CommitIndex = len(rf.LogEntry) + rf.lastIncludeIndex
		}
	}
	DPrintf("%d receive from %d,dealing log success CommitIndex:%d LastApplied:%d", rf.me, args.LeaderId, rf.CommitIndex, rf.LastApplied)

	rf.persist()
	//DPrintf("[%d] receive from leader:%d", rf.me, args.CandidateId)
	return
}

func (rf *Raft) applyToService(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		appliedArr := make([]ApplyMsg, 0)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//DPrintf("[%d] try to commit log,len(rf.LogEntry):%d,commitIndex:%d,lastApplied:%d", rf.me, len(rf.LogEntry), rf.CommitIndex, rf.LastApplied)
			for rf.CommitIndex > rf.LastApplied {
				appliedArr = append(appliedArr, ApplyMsg{
					CommandValid: true,
					Command:      rf.LogEntry[rf.LastApplied-rf.lastIncludeIndex].Command,
					CommandIndex: (rf.LastApplied + 1),
				})

				DPrintf("[%d] Command:%v CommandIndex%d commit", rf.me, rf.LogEntry[rf.LastApplied-rf.lastIncludeIndex].Command, (rf.LastApplied + 1))
				rf.LastApplied += 1

			}

		}()

		DPrintf("%d Log:%v", rf.me, rf.LogEntry)
		for _, msg := range appliedArr {
			applyCh <- msg
			DPrintf("%d command:%v commit success", rf.me, msg.Command)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//服务或测试人员想要创建一个Raft服务器。所有Raft服务器(包括这个)的端口都在对等体[]中。
//这个服务器的端口是peers[me]。所有服务器的对等体[]数组的顺序都是相同的。
//Persister是服务器保存其持久状态的地方，如果有的话，它最初还保存最近保存的状态。
//applyCh是一个通道，测试人员或服务期望Raft在该通道上发送ApplyMsg消息。
//make()必须快速返回，因此它应该启动goroutinesor任何长时间运行的工作。
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	//raft实例初始化
	rf := &Raft{
		mu:               sync.Mutex{},
		peers:            peers,
		persister:        persister,
		me:               me,
		LogEntry:         make([]LogEntry, 0),
		CommitIndex:      0,
		LastApplied:      0,
		nextIndex:        make([]int, len(peers)),
		matchIndex:       make([]int, len(peers)),
		lastIncludeIndex: 0,
		lastIncludeTerm:  0,
		applyCh:          applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.State = Follower
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.lastActiveTime = time.Now()

	// initialize from state persisted before a crash
	//在崩溃前持续从状态初始化
	rf.readPersist(persister.ReadRaftState())

	gob.Register(LogEntry{})
	// start ticker goroutine to start elections
	//启动自动Goroutine以开始选举
	//进行领导在选举
	go rf.ticker()
	//监测是否收到心跳
	go rf.Listen()

	go rf.applyToService(applyCh)

	DPrintf("%d init", rf.me)
	return rf
}
