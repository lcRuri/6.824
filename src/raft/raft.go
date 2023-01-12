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
	"log"
	"math/rand"

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
	CommandValid bool
	Command      interface{}
	CommandIndex int

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
	State       int //当前节点的身份
	CurrentTerm int //server能看到的最新任期
	VoteFor     int //候选者Id(在当前任期里面收到的投票，没有为null)
	updatedTime time.Time
	waitTime    chan int
}

type RaftState struct {
	LeadersState *LeadersState
	ServerState  *ServerState
}

//LeadersState 选举后要重新初始化
type LeadersState struct {
	NextIndex  []int //下一个日志条目的索引 猜测是不是原来存leader的所有命令
	MatchIndex []int //最高日志条目索引 初始为0 单调增
}

//ServerState Server的状态
type ServerState struct {
	//Persistent state
	Logs []string //日志条目 每个日志条目包含对状态机的命令当日志从leader那接收到
	//Volatile State
	CommitIndex int //已知的索引最高的日志条目将被提交
	LastApplied int //被应用的状态机索引最高的日志条目

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int       //候选者任期
	CandidateId  int       //候选者Id
	LastLogIndex int       //候选者最后的日志索引
	LastLogTerm  time.Time //候选者最后的日志条目的期限
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
	Term         int
	LeaderId     int
	PreLogIndex  int
	preLogTerm   int
	Entries      []string
	LeaderCommit int
}

type ReceiveEntriesRPC struct {
	Term    int
	Success bool
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
	log.Printf("[%d] sending request vote to %d", rf.me, args.CandidateId)
	if rf.VoteFor == -1 && rf.CurrentTerm < args.Term {
		//如果接收节点在这个任期内还没有投票，那么它将投票给候选人
		rf.VoteFor = args.CandidateId
		rf.CurrentTerm = args.Term
		reply.VoteGranted = true
	}
	rf.mu.Unlock()

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
	if reply.VoteGranted == false {
		return false
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
		rf.waitTime <- rand.Intn(150) + 150
		//监测是否收到心跳
		go rf.Listen()
		//等待过程中没有收到leader的心跳或者选举的，发起选举
		for len(rf.waitTime) != 0 {
			sleepTime := <-rf.waitTime
			log.Printf("[%d] waitTime is:%d, len rf.waitTime:%d", rf.me, sleepTime, len(rf.waitTime))
			time.Sleep(time.Millisecond * time.Duration(sleepTime))
		}
		rf.mu.Lock()
		//更改自己的身份
		rf.State = Candidate
		//投票给自己
		rf.VoteFor = rf.me
		args := &RequestVoteArgs{
			Term:        rf.CurrentTerm,
			CandidateId: rf.me,
		}
		reply := &RequestVoteReply{}

		rf.mu.Unlock()
		votes := 1
		//当前节点进入选举
		//sends out Request Vote messages to other nodes.
		log.Printf("[%d] attempting an election at term %d,currentTerm is:%d", rf.me, rf.CurrentTerm+1, rf.CurrentTerm)
		for peerId, _ := range rf.peers {
			if peerId == rf.me {
				continue
			}
			go func() {
				ok := rf.sendRequestVote(peerId, args, reply)
				rf.mu.Lock()

				if ok {
					if reply.VoteGranted == true {
						votes++
						if votes > len(rf.peers)/2 {
							rf.CurrentTerm = rf.CurrentTerm + 1
							log.Printf("[%d] we got enough votes, we are now leader (currentTerm=%d)", rf.me, rf.CurrentTerm)
							rf.State = Leader
							rf.mu.Unlock()
							return
						}
						log.Printf("[%d] got vote from %d", rf.me, peerId)
					}
				} else {
					return
				}

				rf.mu.Unlock()
			}()
		}

	}
}

func (rf *Raft) Listen() {
	//如果当前节点的状态是领导者或者候选者
	for rf.State == Leader || rf.State == Candidate {
		for peerId, _ := range rf.peers {
			if peerId == rf.me {
				//log.Printf("%d State is %d", rf.me, rf.State)
				continue
			}
			go func() {
				args := &RequestVoteArgs{CandidateId: rf.me, Term: rf.CurrentTerm}
				reply := &RequestVoteReply{}
				rf.peers[peerId].Call("Raft.Heart", args, reply)
			}()

		}
	}

	//for rf.State == Follower {
	//	if rf.received {
	//
	//	}
	//}

}

//Heart 发送心跳
func (rf *Raft) Heart(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.waitTime <- rand.Intn(150) + 150
	//log.Printf("[%d] receive from leader:%d", rf.me, args.CandidateId)
	return
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
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		waitTime:  make(chan int, 1),
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.State = Follower
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.updatedTime = time.Now()

	// initialize from state persisted before a crash
	//在崩溃前持续从状态初始化
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	//启动自动Goroutine以开始选举
	//进行领导在选举
	go rf.ticker()
	DPrintf("%d init", rf.me)
	return rf
}
