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

import "sync"
import "time"
import "math/rand"
import "sync/atomic"
import "../labrpc"
import "fmt"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type Log struct {
	Entry []byte
	Term int
}
// type Persister struct {
// 	CurrentTerm int
// 	voteFor int
// 	log []Log
	
// }

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// voteNum   int
	//persistent
	currentTerm int
	voteFor int
	log []Log

	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int

	// electionTimeout 
	state int //0leader 1candidate 2follower
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionResetCh chan struct{}
	rand *rand.Rand
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term:=rf.currentTerm
	isleader:=rf.state==0
	rf.mu.Unlock()
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
type RequestAppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries Log
	LeaderCommit int
}
type RequestAppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) RequestAppendEntries() {
	// Your code here (2A, 2B).
	var args RequestAppendEntriesArgs
	var reply RequestAppendEntriesReply
	// args.Term=rf.currentTerm
	// args.LeaderId=rf.me
	// args.PrevLogIndex=
	rf.mu.Lock()
	for i,_:= range rf.peers{
		if i!=rf.me{
			rf.mu.Unlock()
			ok:=rf.sendRequestAppendEntries(i,&args,&reply)
			rf.mu.Lock()
			if ok{
				if reply.Success==true{
					rf.nextIndex[i]++
					rf.matchIndex[i]++
				}else if rf.currentTerm < reply.Term{
					rf.currentTerm=reply.Term
					rf.state=2
				}else{
					rf.nextIndex[i]--
					// rf.matchIndex[i]++
				}
			}
		}
	}
	rf.mu.Unlock()
}
func (rf *Raft) RequestHeartBeat() {
	// Your code here (2A, 2B).
	var args RequestAppendEntriesArgs
	// var reply RequestAppendEntriesReply
	rf.mu.Lock()
	args.Term=rf.currentTerm
	args.LeaderId=rf.me
	rf.mu.Unlock()
	// args.LeaderId=-1 //心跳标志
	// args.LeaderId=rf.me
	// args.PrevLogIndex=
	for i,_:= range rf.peers{
		if i!=rf.me{
			// 
			//改成并行
			go func(server int, args RequestAppendEntriesArgs) {
				var reply RequestAppendEntriesReply
				if rf.sendRequestAppendEntries(server, &args, &reply) {
					rf.mu.Lock()
					if args.Term!=rf.currentTerm{
						rf.mu.Unlock()
						return
					}
					if reply.Term > rf.currentTerm {
						// rf.becomeFollower(reply.Term)
						// rf.resetElectionTimer()
						rf.currentTerm=reply.Term
						rf.voteFor=-1
						rf.state=2
					}
					rf.mu.Unlock()
				}
			}(i, args)
		}
	}
	// rf.mu.Unlock()
}
func (rf *Raft) RequestAppendEntriesReply(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	//心跳
	if  len(args.Entries.Entry)==0 &&args.Term >= rf.currentTerm{
		// if args.Term > rf.currentTerm{ 一样也变回，让候选人此时变回follower
		if args.Term > rf.currentTerm{
			rf.voteFor=-1
			rf.currentTerm=args.Term
		}
		rf.voteFor = args.LeaderId
		rf.state=2
		rf.resetElectionTimer()
		// }
		// }
		reply.Term =rf.currentTerm
		reply.Success=true
		// rf.resetElectionTimer()
		fmt.Printf("%d收到了%d心跳\n",rf.me,args.LeaderId)
	}else if args.Term >= rf.currentTerm {
		// if args.Term > rf.currentTerm{
			if args.Term > rf.currentTerm{
				rf.voteFor=-1
				rf.currentTerm=args.Term
			}
			rf.voteFor = args.LeaderId
			rf.state=2
			rf.resetElectionTimer()
		// }
		if (len(rf.log)==0 && args.PrevLogIndex==-1) ||(len(rf.log)>=args.PrevLogIndex+1&&rf.log[args.PrevLogIndex].Term==args.PrevLogTerm){
			//todo:添加日志
			fmt.Printf("%d添加%d的日志\n",rf.me,args.LeaderId)
			reply.Term =rf.currentTerm
			reply.Success=true
		}else{
			reply.Term =rf.currentTerm
			reply.Success=false
		}
		// rf.resetElectionTimer()
	}else{
		reply.Term =rf.currentTerm
		reply.Success=false
	}
	// rf.resetElectionTimer()
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntriesReply", args, reply)
	return ok
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote() {
	// start := time.Now()
	// Your code here (2A, 2B).
	var args RequestVoteArgs
	// var reply RequestVoteReply
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	args.Term=rf.currentTerm
	args.CandidateId=rf.me
	if len(rf.log)!=0{
		args.LastLogIndex=len(rf.log)-1
	}
	// if args.LastLogIndex<0{
	// 	args.LastLogIndex=0
	// }
	if len(rf.log)!=0{
		args.LastLogTerm=rf.log[args.LastLogIndex].Term
	}
	rf.mu.Unlock()
	voteNum:=1
	var mu sync.Mutex 
	var wg sync.WaitGroup
	wg.Add(len(rf.peers)-1) // 有 1 个子任务要等待
	for i,_:= range rf.peers{
		if i!=rf.me{
			// rf.mu.Unlock()
			// ok:=rf.sendRequestVote(i,&args,&reply)
			// rf.mu.Lock()
			// if rf.currentTerm!=args.Term{
			// 	rf.mu.Unlock()
			// 	break
			// }
			// if ok{
			// 	if rf.currentTerm!=args.Term{
			// 		rf.mu.Unlock()
			// 		break
			// 	}
			// 	if reply.VoteGranted==true{
			// 		voteNum++
			// 	}else if rf.currentTerm < reply.Term{
			// 		rf.currentTerm=reply.Term
			// 		rf.voteFor=-1
			// 		rf.state=2
			// 		// rf.becomeFollower(reply.Term)//注意
			// 		// rf.resetElectionTimer()
			// 		rf.mu.Unlock()
			// 		return //真的是因为这个啊，及时退出
			// 	}
			// }
			// rf.mu.Unlock()
			go func(server int, args RequestVoteArgs) {
				defer wg.Done() // 完成时减少计数
				var reply RequestVoteReply
				if rf.sendRequestVote(server, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// if reply.Term > rf.currentTerm {
					// 	rf.becomeFollower(reply.Term)
					// 	rf.voteFor = -1
					// }
					if rf.currentTerm!=args.Term || rf.state!=1{
						return
					}
					// mu.Lock()
					// if voteNum>len(rf.peers)/2{
					// 	mu.Unlock()
					// 	return
					// }
					// mu.Unlock()
					if reply.VoteGranted==true{
						mu.Lock()
						voteNum++
						if voteNum>len(rf.peers)/2{
							mu.Unlock()
								//
							rf.state=0
							go rf.leaderBeatLoop()
							//默认认为log和leader一样新
							for i,_:= range rf.nextIndex{
								rf.nextIndex[i]=len(rf.log)
							}
							return
						}
						mu.Unlock()
					}else if rf.currentTerm < reply.Term{
						rf.currentTerm=reply.Term//注意
						rf.voteFor=-1
						rf.state=2
						// return //真的是因为这个啊，及时退出
					}
				}
			}(i, args)
		}
		//提前退出，防止等待过久
		// if voteNum > (len(rf.peers)/2){
		// 	break
		// }
	}
	// wg.Wait() // 阻塞等待
	// rf.mu.Lock()
	// fmt.Printf("sum:%d Term: %d  candidate%d  get:   %d\n",len(rf.peers),rf.currentTerm,rf.me,voteNum)
	// if voteNum > (len(rf.peers)/2) && rf.state ==1 && rf.currentTerm==args.Term{//得是candidate，可能在其他地方变了
	// 	// rf.mu.Lock()
	// 	//编程leader
	// 	rf.state=0
	// 	// go rf.RequestHeartBeat()
	// 	go rf.leaderBeatLoop()
	// 	//默认认为log和leader一样新
	// 	for i,_:= range rf.nextIndex{
	// 		rf.nextIndex[i]=len(rf.log)
	// 	}
	// 	// rf.mu.Unlock()
	// }
	// rf.mu.Unlock()
	// elapsed := time.Since(start)          // 返回 duration
	// ms := elapsed.Milliseconds()          // 转换成 int64 毫秒
	// fmt.Println("耗时毫秒:", ms)

}
func (rf *Raft) RequestVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	var LastLogIndex int
	var LastLogTerm int
	if len(rf.log)!=0{
		LastLogTerm=rf.log[len(rf.log)-1].Term
	}
	if len(rf.log)!=0{
		LastLogIndex=len(rf.log)-1
	}
	if args.Term > rf.currentTerm{
		// rf.becomeFollower(args.Term)
		rf.currentTerm=args.Term
		rf.voteFor=-1
		rf.state=2
	}
	if args.Term >= rf.currentTerm && (len(rf.log)==0||(args.LastLogTerm > LastLogTerm || ((args.LastLogTerm == LastLogTerm)&&(args.LastLogIndex>=LastLogIndex)))){
		//不知道对不对
		// if args.Term > rf.currentTerm{
		// 	rf.becomeFollower(args.Term)
		// 	rf.voteFor=-1
		// }
		if rf.voteFor==-1 || rf.voteFor==args.CandidateId{
			reply.Term =rf.currentTerm
			reply.VoteGranted=true
			rf.voteFor=args.CandidateId
			rf.resetElectionTimer()//售票重置有必要吗？
		}else{
			reply.Term =rf.currentTerm
			reply.VoteGranted=false
		}
	}else{
		reply.Term =rf.currentTerm
		reply.VoteGranted=false
	}
	// rf.resetElectionTimer()
	rf.mu.Unlock()
}

//
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
	ok := rf.peers[server].Call("Raft.RequestVoteReply", args, reply)
	return ok
}

// func (rf *Raft) becomeFollower(term int) {
//     rf.state = 2
//     rf.currentTerm = term
//     // rf.voteFor = -1
//     // rf.resetElectionTimer()
// }
//
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
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	
	// Your initialization code here (2A, 2B, 2C).
	rf.state=2
	rf.voteFor=-1
	rf.electionResetCh = make(chan struct{}, 1)
	rf.rand=rand.New(rand.NewSource(time.Now().UnixNano() + int64(me)*1e6))
	// defer rf.electionResetCh.close()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.electionLoop()

	return rf
}
func (rf *Raft) electionLoop() {
	timeout := rf.randElectionTimeout()
	deadline := time.Now().Add(timeout)

	for !rf.killed(){
		_,isleader:=rf.GetState() 
		if isleader{
			break
		}
		// 1) 是否收到重置信号（心跳/授票）
		select {
		case <-rf.electionResetCh:
			timeout = rf.randElectionTimeout()
			deadline = time.Now().Add(timeout)
		default:
		}

		// 2) 是否超时
		if time.Now().After(deadline) {
			fmt.Printf("%d 发起一轮投票\n",rf.me)
			rf.startElection() // 发起一轮拉票（见下）
			// 新一轮的 follower/candidate 等待窗口
			timeout = rf.randElectionTimeout()
			deadline = time.Now().Add(timeout)
		}

		// 3) 小步 sleep，既不忙等，也不依赖 Timer/Ticker
		// 可用 10~20ms，兼顾响应与 CPU 占用
		time.Sleep(5 * time.Millisecond)
	}
}
func (rf *Raft) leaderBeatLoop() {
	go rf.RequestHeartBeat()//马上发一次心跳稳固军心
	timeout := randLeaderTimeout()
	deadline := time.Now().Add(timeout)

	for !rf.killed() {
		_,isleader:=rf.GetState()
		if !isleader{
			break
		}
		if time.Now().After(deadline) {
			go rf.RequestHeartBeat()
			timeout = randLeaderTimeout()
			deadline = time.Now().Add(timeout)
		}

		// 3) 小步 sleep，既不忙等，也不依赖 Timer/Ticker
		// 可用 10~20ms，兼顾响应与 CPU 占用
		time.Sleep(5 * time.Millisecond)
	}
	if _,isleader:=rf.GetState();!isleader&&!rf.killed(){
		go rf.electionLoop()
	}
}
func randLeaderTimeout() time.Duration {
    return time.Duration(100) * time.Millisecond
}
func (rf *Raft)randElectionTimeout() time.Duration {
    return time.Duration(150+rf.rand.Intn(100)) * time.Millisecond
}
func (rf *Raft) resetElectionTimer() {
    select {
    case rf.electionResetCh <- struct{}{}:
    default:
    }
}
func (rf *Raft) startElection() {
    rf.mu.Lock()
    // 若已是 Leader 就不再竞选（可能上一轮刚当选）
    if rf.state == 0 {
        rf.mu.Unlock()
        return
    }
    rf.state = 1 // Candidate
    rf.currentTerm++
	//不知道有没有必要
	rf.voteFor = rf.me
	// rf.
    rf.mu.Unlock()
	rf.RequestVote()
}
//遇到更高的任期的请求或者回复，节点任期要变成该任期，还要变fellower，投票重置
//lab2A 这里面有很多条件要按照图二遵守
//投票一定要并行发requestVote,每次开一个goroutine来处理一个rpc发送，而却票数达到了大多数一定及时编程leader
//不然第二个测试条件网络延迟很高，一些rpc始终得不到回复然后其他fellower倒计时开始要新一轮投票了，而却任期比刚才要成为leader的那个候选节点大
//导致候选节点刚要当上leader就被这个任期更高的发送requestvote然后因为遇到更高任期就转fellower了，这样一直循环
//然后要注意刚当上leader要马上发心跳让其他节点在倒计时结束前重置时间，不然时间到了任期增加发送投票请求(更高的任期)，leader又被干下去