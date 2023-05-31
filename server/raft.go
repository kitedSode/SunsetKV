package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Status server状态
type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)

const CommitTickerTime = 15 * time.Millisecond
const AppendEntriesTickerTime = 35 * time.Millisecond

// ApplyMsg 用于提交日志或者快照
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	peers     []*Peer    // RPC end points of all peers
	persister *Persister // Object to hold this peer's persisted state
	me        int        // this peer's index into peers[]
	dead      int32      // set by Kill()

	// 所有服务器上持久存在的
	currentTerm int        // the latest termId(only for current server)
	votedFor    int        //	记录投票给了谁
	logs        []LogEntry // 日志条目数组，每个条目包含一个用户状态机执行的指令，和收到时的任期号

	//	所有服务器上经常变的
	commitIndex int // 已知的最大的已经被提交的日志条目的索引值
	lastApplied int // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

	//在领导人里经常改变的 （选举后重新初始化）
	nextIndex  []int //对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	matchIndex []int //对于每一个服务器，已经复制给他的日志的最高索引值

	// 自定义
	status       Status        // 用以确定当前服务器的状态(follower, candidate | leader)
	electionTime time.Duration // 在给定时间内如果没有收到leader的心跳则触发选举操作
	heartbeat    time.Time     // 最新收到心跳信息时的时间
	votesNum     int           // 用以统计选举的票数，每次选举前更改为1
	conn         net.Listener  // 用于rpc的tcp连接

	applyCh chan ApplyMsg // 将日志应用到状态机的channel

	lastIncludedIndex int // 快照最后应用到的日志索引
	lastIncludedTerm  int // 快照最后应用到的日志term
}

type LogEntry struct {
	Instructions interface{} // 状态机指令
	Term         int         // 存储日志时的任期号
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return -1, false
	}

	term = rf.currentTerm
	isleader = rf.status == Leader

	return term, isleader
}

// GetRaftStateSize 返回raft的持久化数据的大小给server
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// GetSnapshot 在server启动时将snapshot传给server以便其恢复之前的数据
func (rf *Raft) GetSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

// 持久化保存raft的数据
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getPersistData())
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.logs)
	encoder.Encode(rf.votedFor)
	//encoder.Encode(rf.commitIndex)
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var currentTerm int
	var logs []LogEntry
	var votefor int
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&logs) != nil ||
		d.Decode(&votefor) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		fmt.Println("decoder error!")
	} else {
		rf.currentTerm = currentTerm
		rf.logs = logs
		rf.votedFor = votefor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}

}

// RequestVoteArgs 选举参数
type RequestVoteArgs struct {
	Term         int // 候选人的任期号(候选人的currentTerm + 1)
	CandidateId  int // 请求选票的候选人的 Id
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

type RequestVoteReply struct {
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

// RequestVote 选举函数
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("server[%d] receive vote for args: %v\n", rf.me, args)
	//fmt.Printf("server[%d][Term: %d] receive voteRequest from server[%d][Term: %d]\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	if rf.killed() {
		//reply.VoteState = VoteKilled
		reply.VoteGranted = false
		reply.Term = -1
		return nil
	}
	//fmt.Printf("server[%d][Term: %d] receive voteRequest for server[%d][Term: %d]\n", rf.me, rf.currentTerm, args.Id, args.Term)
	// 如果两份日志最后条目的任期号不同，那么任期号大的日志更新
	// 如果两份日志最后条目的任期号相同，那么谁的日志更长，谁就更新
	// 如果当前服务器的日志条目比对方新则不予投票，并告知对方投票状态为Expire使对方的状态更改为follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.votesNum = 0
		rf.status = Follower
		rf.persist()
		//rf.renewHeartbeat()
	}

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLogTermByIndex(lastLogIndex)

	if args.Term < rf.currentTerm ||
		args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// 但满足以上选举条件限制时，当前服务器则向对方服务器投票并将自己的状态更改为follower
	// 此时会有两种情况:
	// 1. 当前服务的当前任期号 < 对方的当前任期号(对方的overtime < 自己的overtime，对方先执行选举操作，自己此时还是follower)。给予投票
	// 2. 当前服务的当前任期号 == 对方的当前任期号，则此时的状态都应该为Candidate。说明自己已经为自己投过票则不需要再向对方投票。也存在已经
	if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.renewHeartbeat()
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	return nil
}

func (rf *Raft) execElection() {
	rf.status = Candidate
	rf.votesNum = 1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLogTermByIndex(lastLogIndex)
	//fmt.Printf("server[%d][Term :%d] execElection and lastLogIndex is %d, lastLogTerm is %d\n", rf.me, rf.currentTerm, rf.getLastLogIndex(), lastLogTerm)
	//fmt.Printf("server[%d][Term :%d] execElection and lastLogIndex is %d, lastLogTerm is %d, len_log is %d\n", rf.me, rf.currentTerm, rf.getLastLogIndex(), lastLogTerm, len(rf.logs))
	rf.setElectionTime()
	//fmt.Fprintf(os.Stdout, "server[%d] execElection and currentTerm is %d, electionTimes is %d, status is %d, time is %v\n",
	//	rf.me, rf.currentTerm, rf.electionTimes, rf.status, time.Now())
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := RequestVoteReply{}
		go func(server int) {

			if !rf.sendRequestVote(server, args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.status != Candidate || rf.currentTerm != args.Term || reply.Term == -1 || rf.killed() {
				return
			}

			if reply.Term == rf.currentTerm {
				if reply.VoteGranted {
					half := len(rf.peers) / 2
					if rf.votesNum > half {
						return
					} else {
						rf.votesNum++
					}

					if rf.votesNum > half {
						rf.status = Leader
						// 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）.
						// 我们这里的索引值是从1开始所以len(rf.logs)正好对应了leader当前的日志条数,
						// 那么针对每个follower的nextIndex[i]都应初始化为len(rf.logs) + 1
						nextLogIndex := rf.getLastLogIndex() + 1
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = nextLogIndex
						}
						rf.votesNum = 0
						// 当成为leader时，我认为超时时间应该是所有服务器中最短的那一个
						// 如果有服务器的overtime < leader.overtime，那么这个服务器就会发起leader选举
						// 如果leader的overtime < 所有服务器的overtime，那么就不会出现服务器因为超时未收到信息而发起选举的情况
						fmt.Printf("server[%d] become leader, term[%d] and nextIndex is %d\n", rf.me, rf.currentTerm, rf.nextIndex[0]) //xqc
						rf.persist()
					}
					rf.renewHeartbeat()
				}
			} else if reply.Term > rf.currentTerm {
				//fmt.Printf("server[%d] election fail...\n", rf.me)
				rf.status = Follower
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.persist()
				rf.renewHeartbeat()
			}
		}(i)
	}
}
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte // 快照字节数据
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

// AppendEntriesArgs 附加日志args
type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader的id,以便于follower重定向请求
	PrevLogIndex int        // 新的日志条目紧随之前的索引值.比如向x服务器发送一条新的日志条目，那么prevLogIndex则为nextIndex[x] - 1
	PrevLogTerm  int        // prevLogIndex的任期号
	Entries      []LogEntry //准备存储的日志条目(表示心跳时为空;一次性发送多个是为了提高效率)
	LeaderCommit int        // 领导人已经提交的日志的索引值
}

// AppendEntriesReply 附加日志 reply
type AppendEntriesReply struct {
	Term int // 服务器当前的term，如果leader的term小于这个的话则更改自己的状态
	//State             AppendState // 添加日志的状态
	Success           bool // 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
	ExceptedNextIndex int  // 如果leader和follower在追加日志的index出现矛盾时，follower会返回lastApplied + 1来表示希望更新nextIndex
	// 添加成功的话leader就直接将这个设为nextIndex
}

func (rf *Raft) execAppendEntries() {
	//receives := new(int)
	//*receives = 1
	newCommitIndex := rf.getLastLogIndex()
	//fmt.Printf("leader[%d] ready to send append\n", rf.me)
	//rf.mu.Lock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := rf.getLogTermByIndex(prevLogIndex)
		if prevLogTerm == -1 { // 传给该server的下一条log的index小于lastIncludedIndex的话需要对其发送快照来保证统一性
			//time := time.Now()
			//fmt.Printf("leader[%d]'s lastIncludedIndex is %d and commitIndex is %d, but server[%d]'s is %d\n",
			//	rf.me, rf.lastIncludedIndex, rf.commitIndex, i, prevLogIndex)
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.persister.ReadSnapshot(),
			}
			go rf.sendInstallSnapshot(i, &args)
			continue
		}

		entries := make([]LogEntry, newCommitIndex-prevLogIndex)
		copy(entries, rf.logs[prevLogIndex-rf.lastIncludedIndex:])

		arg := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		reply := AppendEntriesReply{}

		go func(server int) {
			ok := rf.sendAppendEntries(server, arg, &reply)

			if !ok || reply.Term == -1 {
				return
			}

			rf.mu.Lock()

			if rf.killed() || arg.Term != rf.currentTerm || rf.status != Leader {
				rf.mu.Unlock()
				return
			}

			if reply.Term > rf.currentTerm {
				rf.status = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				rf.renewHeartbeat()
				rf.mu.Unlock()
				return
			}

			if reply.Success {

				//if len(arg.Entries) == 0 || reply.ExceptedNextIndex < rf.nextIndex[server]{
				//	//fmt.Println("fuck")
				//	rf.mu.Unlock()
				//	return
				//}

				if len(arg.Entries) == 0 || reply.ExceptedNextIndex < rf.nextIndex[server] {
					//if reply.ExceptedNextIndex < rf.nextIndex[server]{
					//	panic("really!!!!!!!!!!")
					//}
					rf.mu.Unlock()
					return
				}

				rf.nextIndex[server] = reply.ExceptedNextIndex
				rf.matchIndex[server] = reply.ExceptedNextIndex - 1
				theNewCommitIndex := rf.matchIndex[server]

				if rf.getLogTermByIndex(theNewCommitIndex) == rf.currentTerm {
					for ; theNewCommitIndex > rf.commitIndex; theNewCommitIndex-- {
						nums := 1
						for s := 0; s < len(rf.peers); s++ {
							if s == rf.me {
								continue
							}

							if rf.matchIndex[s] >= theNewCommitIndex {
								nums++
							}
						}

						if nums > len(rf.peers)/2 {
							break
						}
					}

					if theNewCommitIndex > rf.commitIndex {
						//fmt.Printf("leader[%d] will renew commitIndex from %d to %d\n", rf.me, rf.commitIndex, theNewCommitIndex)
						rf.commitIndex = theNewCommitIndex
					}
				}
			} else {
				rf.nextIndex[server] = reply.ExceptedNextIndex
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Printf("leader[%d] send appEntrie and Term = %v\n", rf.me, rf.currentTerm)
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	changed := false
	//fmt.Println(args)
	if rf.killed() {
		reply.Success = false
		reply.Term = -1
		reply.ExceptedNextIndex = -1
		return nil
	}

	rf.mu.Lock()
	// 出现网络分区时，server数量少的那一个分区绝对不可能选出新的leader(无法获得超过 n / 2的票数)但是可能会出现term很高的follower
	// 增加了commitIndex的检测，分区的commitIndex是不会增长的。
	//fmt.Printf("server[%d][commitIndex: %d] receive append leader's commitIdnex is %d, len_log is %d\n",
	//	rf.me, rf.commitIndex, args.LeaderCommit, len(args.Entries))
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ExceptedNextIndex = -1
		//fmt.Printf("server[%d][Term: %d][leader: %d][Term: %d]: Expired!\n",
		//	rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.mu.Unlock()
		return nil
	}

	if rf.status != Follower {
		rf.status = Follower
		rf.votedFor = args.LeaderId
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		changed = true
	}

	rf.renewHeartbeat()
	//TODO: 我认为只要被commit过的日志不能被重写
	//if args.PrevLogIndex < rf.lastIncludedIndex {
	//	reply.Success = false
	//	reply.ExceptedNextIndex = rf.getLastLogIndex() + 1
	//	reply.Term = rf.currentTerm
	//	if changed{
	//		rf.persist()
	//	}
	//	//fmt.Printf("server[%d][leader: %d]: over2\n", rf.me, args.LeaderId)
	//	rf.mu.Unlock()
	//	//fmt.Printf("server[%d][Committed: %d] AppendCommitted!!!, leader[%d]'sPrevLogIndex: %d, commitIndex: %d\n",
	//	//	rf.me, rf.commitIndex, args.LeaderId, args.PrevLogIndex, args.LeaderCommit)
	//	return
	//}

	if args.PrevLogIndex < rf.commitIndex {
		reply.Success = false
		reply.ExceptedNextIndex = rf.getLastLogIndex() + 1
		reply.Term = rf.currentTerm
		if changed {
			rf.persist()
		}
		//fmt.Printf("server[%d][leader: %d]: over2\n", rf.me, args.LeaderId)
		rf.mu.Unlock()
		//fmt.Printf("server[%d][Committed: %d] AppendCommitted!!!, leader[%d]'sPrevLogIndex: %d, commitIndex: %d\n",
		//	rf.me, rf.commitIndex, args.LeaderId, args.PrevLogIndex, args.LeaderCommit)
		return nil
	}

	// 如果leader的log[]不是空的
	if args.PrevLogIndex > 0 {
		if rf.getLastLogIndex() < args.PrevLogIndex {
			reply.ExceptedNextIndex = rf.getLastLogIndex() + 1
		} else if args.PrevLogTerm != rf.getLogTermByIndex(args.PrevLogIndex) {
			reply.ExceptedNextIndex = rf.lastApplied + 1
		}

		if reply.ExceptedNextIndex > 0 {
			reply.Success = false
			reply.Term = rf.currentTerm
			if changed {
				rf.persist()
			}
			//fmt.Printf("server[%d]'s log mismatch leader's index: %d,and nextIndex is %d\n", rf.me, args.PrevLogIndex, reply.ExceptedNextIndex)
			rf.mu.Unlock()
			return nil
		}
		//fmt.Printf("server[%d, Term: %d][LastLog:{Term: %d, Index: %d}] AppendMismatch leader[%d, Term: %d][PreLog:{Term: %d, Index: %d}]\n",
		//	rf.me, rf.currentTerm, rf.logs[len(rf.logs)-1].Term, len(rf.logs), args.LeaderId, args.Term, args.PrevLogTerm, args.PrevLogIndex)

		//if len(rf.logs) > 0 && len(rf.logs) >= args.PrevLogIndex{
		//	fmt.Printf("AppendMismatch! leader[%d]' PrevLogIndex is %d, and args's PrevLogTerm = %d;server[%d]'s len(logs) = %d, and lastLog's Term is %d and hope index is [%d]\n",
		//		args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, rf.me, len(rf.logs), rf.logs[args.PrevLogIndex - 1].Term, rf.lastApplied)
		//} else {
		//	fmt.Printf("AppendMismatch! leader[%d]' PrevLogIndex is %d, and args's PrevLogTerm = %d;server[%d]'s len(logs) = %d and hope lastApplidIndex is %d\n",
		//		args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, rf.me, len(rf.logs), rf.lastApplied)
		//}
	}

	// 如果follower的log[]比leader的要长，则要去去除长的部分
	if args.PrevLogIndex < rf.getLastLogIndex() {
		rf.logs = rf.logs[:args.PrevLogIndex-rf.lastIncludedIndex]
		//if args.PrevLogIndex == 0{
		//	panic(0)
		//}
		//fmt.Printf("server[%d][Term: %d]'s logs's length changed for [%d] because leader[%d][Term: %d] and finally command is %v\n",
		//	rf.me, rf.currentTerm, args.PrevLogIndex, args.LeaderId, args.Term, rf.logs[args.PrevLogIndex - 1])
		changed = true
	}

	if len(args.Entries) > 0 {
		changed = true
		//rf.logs = append(rf.logs, args.Entries...)
		//fmt.Printf("server[%d]' log will log renew from %d to %d and end is %v\n",
		//	rf.me, len(args.Entries), len(args.Entries) + len(rf.logs), args.Entries[len(args.Entries) - 1])
		for i := 0; i < len(args.Entries); i++ {
			rf.logs = append(rf.logs, args.Entries[i]) //fmt.Printf("server[%d] receive log[%v] and commitIndex = [%d]\n", rf.me, args.Entries[i].Instructions, rf.commitIndex)
		}
		//fmt.Printf("server[%d] receive %d commands\n", rf.me, len(args.Entries))
	}

	//if rf.commitIndex > rf.getLastLogIndex(){
	//	fmt.Printf("server[%d]'s commitIndex is %d, lastLogIndex is %d\n", rf.me, rf.commitIndex, rf.getLastLogIndex())
	//	rf.commitIndex = rf.getLastLogIndex()
	//}

	if rf.commitIndex < args.LeaderCommit {
		if rf.getLastLogIndex() < args.LeaderCommit {
			//fmt.Printf("server[%d]'s commitIndex is %d, lastLogIndex is %d; leader[%d]'s commitIndex is %d, prevIndex is %d, appendLength: %d\n",
			//	rf.me, rf.commitIndex, rf.getLastLogIndex(), args.LeaderId, args.LeaderCommit, args.PrevLogIndex, len(args.Entries))

			rf.commitIndex = rf.getLastLogIndex()
		} else {
			//fmt.Printf("server[%d]'s renew commitIndex from %d to %d\n",
			//	rf.me, rf.commitIndex, args.LeaderCommit)

			rf.commitIndex = args.LeaderCommit
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.ExceptedNextIndex = rf.getLastLogIndex() + 1

	if changed {
		rf.persist()
	}

	rf.renewHeartbeat()
	//fmt.Printf("server[%d][leader: %d]: over3\n", rf.me, args.LeaderId)
	rf.mu.Unlock()
	//fmt.Printf("server[%d] unlock in AppendEntries()\n", rf.me)
	return nil
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {

	//fmt.Printf("server[%d] exec Snapshot and index = %d, rf.commitIndex = %d, len(rf.logs) = %d, rf.lastIncludedIndex = %d\n",
	//	rf.me, index, rf.commitIndex, len(rf.logs), rf.lastIncludedIndex)
	rf.mu.Lock()
	//if rf.status == Follower{
	//	fmt.Printf("server[%d] lock in Start()\n", rf.me)
	//}
	if index > rf.commitIndex || rf.lastIncludedIndex >= index {
		rf.mu.Unlock()
		return
	}

	//fmt.Println("1...")
	// 创建新的日志更新为以index开头后的日志
	newLogs := make([]LogEntry, rf.getLastLogIndex()-index)
	copy(newLogs, rf.logs[rf.getServerLogIndex(index)+1:])
	//fmt.Println("2...")
	// 更新rf的最后应用index以及term
	rf.lastIncludedTerm = rf.getLogTermByIndex(index)
	rf.lastIncludedIndex = index
	//fmt.Println("3...")
	//fmt.Printf("server[%d]'s log before is %v\n", rf.me, rf.logs)
	rf.logs = newLogs
	//fmt.Printf("server[%d]'s log behind is %v\n", rf.me, rf.logs)
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	//fmt.Printf("server[%d] renew snapshot and lastIncluded is %d, commitIndex: %d, currentLogs: %v\n",
	//	rf.me, rf.lastIncludedIndex, rf.commitIndex, rf.logs)
	//fmt.Println("spend time:", time.Since(start).Milliseconds())
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {

	reply := InstallSnapshotReply{}
	//fmt.Println("leader's lastIncludedIndex is", args.LastIncludedIndex, "time is", args.Time)
	if !rf.peers[server].Call("Raft.InstallSnapshot", *args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != Leader || args.Term != rf.currentTerm || rf.lastIncludedIndex != args.LastIncludedIndex {
		return
	}

	if rf.currentTerm < reply.Term {
		rf.status = Follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
		rf.renewHeartbeat()
	} else {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		//fmt.Printf("leader[%d] renew server[%d]'s lastIncludedIndex to %d\n",
		//	rf.me, server, rf.nextIndex[server])

	}
}

// InstallSnapshot 当follower的要追加日志落后于leader的lastIncludedIndex时，
// follower必须将自己的快照更新到与leader一样时才能保证后续的日志追加
func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	//fmt.Println("exec.............")
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return nil
	}

	if rf.status != Follower {
		rf.status = Follower
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.persist()
	}

	reply.Term = rf.currentTerm
	//if args.LastIncludedIndex <= rf.lastIncludedIndex{
	//	reply.Success = false
	//	rf.renewHeartbeat()
	//	rf.mu.Unlock()
	//	return
	//}

	if args.LastIncludedIndex <= rf.commitIndex {
		reply.Success = false
		rf.renewHeartbeat()
		rf.mu.Unlock()
		return nil
	}

	//fmt.Printf("server[%d][lastIncludedIndex: %d] receive snapshot, commitIndex is %d, leader[%d]'s lastIncludedIndex is %d\n",
	//	rf.me, rf.lastIncludedIndex, rf.commitIndex, args.LeaderId, args.LastIncludedIndex)
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	rf.logs = make([]LogEntry, 0, 4)
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}

	//fmt.Printf("server[%d] receive newest snapshot and\n")
	reply.Success = true
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)
	rf.renewHeartbeat()

	//fmt.Printf("server[%d] receive newest snapshot and lastIncluded is %d\n",
	//	rf.me, rf.lastIncludedIndex)
	rf.mu.Unlock()
	rf.applyCh <- msg
	return nil
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	if rf.killed() {
		return index, term, false
	}

	rf.mu.Lock()
	//if rf.status == Follower{
	//	fmt.Printf("server[%d] lock in Start()\n", rf.me)
	//}

	defer rf.mu.Unlock()

	if rf.status != Leader {
		return index, term, false
	}
	term = rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{Instructions: command, Term: term})
	index = rf.getLastLogIndex()

	rf.persist()
	//fmt.Printf("leader[%d][Term :%d] receive command{%v} and len(log) = %d, commitIndex = %d, index = %d\n", rf.me, rf.currentTerm, command, rf.getLastLogIndex(), rf.commitIndex, index) //xqc
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	//fmt.Println("Kill", rf.me)
	rf.conn.Close()
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) goTicker() {
	go rf.electionTicker()
	go rf.appendEntriesTicker()
	go rf.commitTicker()
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		startTime := time.Now()
		time.Sleep(rf.electionTime)

		rf.mu.Lock()
		if rf.status != Leader && rf.heartbeat.Before(startTime) {
			// 定时器超时，执行选举操作
			rf.execElection()
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) appendEntriesTicker() {
	serverStatus := (*int32)(unsafe.Pointer(&rf.status))
	leaderStatus := (int32)(Leader)
	for !rf.killed() {
		time.Sleep(AppendEntriesTickerTime)
		if atomic.LoadInt32(serverStatus) != leaderStatus {
			continue
		}
		rf.mu.Lock()
		rf.execAppendEntries()
		rf.mu.Unlock()
	}
}

func (rf *Raft) commitTicker() {
	//atomic.CompareAndSwapInt32()
	for !rf.killed() {
		time.Sleep(CommitTickerTime)
		rf.mu.Lock()
		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			continue
		}

		applyMsgs := make([]ApplyMsg, rf.commitIndex-rf.lastApplied)
		index := 0
		//if rf.commitIndex > rf.getLastLogIndex(){
		//	//fmt.Printf("(commitTicker)server[%d]'s commitIndex is %d, lastLogIndex is %d\n", rf.me, rf.commitIndex, rf.getLastLogIndex())
		//	rf.commitIndex = rf.getLastLogIndex()
		//}

		lastLogIndex := rf.getLastLogIndex()
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < lastLogIndex {
			//fmt.Printf("server[%d] will renew lastApplied from %d to %d and len_log = %d\n", rf.me, rf.lastApplied, rf.commitIndex, len(rf.logs))
			rf.lastApplied++
			//if rf.lastApplied > 30{
			//	fmt.Println(rf.lastApplied)
			//}
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.getLogByIndex(rf.lastApplied).Instructions,
				CommandIndex: rf.lastApplied,
			}
			//applyMsgs = append(applyMsgs, applyMsg)
			applyMsgs[index] = applyMsg
			index++
		}

		rf.mu.Unlock()
		for i := 0; i < len(applyMsgs); i++ {
			rf.applyCh <- applyMsgs[i]
		}
	}
}

// 在状态变化之后执行!
func (rf *Raft) setElectionTime() {
	rand.Seed(time.Now().Unix() + int64(rf.me))
	rf.electionTime = time.Duration(180+rand.Intn(200)) * time.Millisecond
}

func (rf *Raft) renewHeartbeat() {
	rf.heartbeat = time.Now()
}

type ConnArgs struct {
	ID int
}

type ConnReply struct {
	OK bool
}

// Connect 用于连接新的rpc服务器（也可用于重置）
func (rf *Raft) Connect(args ConnArgs, reply *ConnReply) error {
	//fmt.Printf("args: %v\n", args)
	//fmt.Printf("rf[%d] recevive new connect [%d]\n", rf.me, args.ID)
	//fmt.Println("rf-peers:", rf.peers)
	for len(rf.peers) == 0 || rf.peers[args.ID] == nil {
		time.Sleep(50 * time.Millisecond)
	}
	//if rf.peers[args.ID].adder != args.Addr {
	//	rf.peers[args.ID].adder = args.Addr
	//}
	rf.peers[args.ID].ReConnect()
	reply.OK = true
	return nil
}

func (rf *Raft) Ping(args byte, reply *byte) error {
	return nil
}

func MakeRaftServer(peersAddr []string, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	//fmt.Printf("server[%d] create!\n", me)
	rf := &Raft{}
	rf.mu = sync.Mutex{}
	rf.mu.Lock()
	rf.dead = 1

	// 为raftServer添加rpc服务
	err := rpc.Register(rf)
	if err != nil {
		log.Fatalln(err)
	}
	conn, err := net.Listen("tcp", peersAddr[me])
	if err != nil {
		log.Fatalln(err)
	}
	go func() {
		for {
			client, err := conn.Accept()
			if err != nil {
				log.Fatalln(err)
			}

			//fmt.Println(client.RemoteAddr())
			go rpc.ServeConn(client)
		}
	}()

	rf.persister = persister
	rf.me = me
	rf.conn = conn

	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0, 128)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peersAddr))
	rf.matchIndex = make([]int, len(peersAddr))
	rf.status = Follower

	rf.setElectionTime()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 连接其他raftServer服务器
	rf.peers = MakePeer(peersAddr, me)
	PeersCreateConnect(rf.peers, rf.me)
	time.Sleep(2 * time.Second)
	rf.heartbeat = time.Now()
	rf.dead = 0
	rf.mu.Unlock()
	fmt.Printf("RaftServer [%d] is running!\n", me)

	//fmt.Printf("server[%d] restart and it's commitIndex = %d\n", me, rf.commitIndex)
	// start ticker goroutine to start elections
	rf.goTicker()

	//go func() {
	//	for i := 0; i < 10; i++ {
	//		time.Sleep(5 * time.Second)
	//		fmt.Printf("server[%d]'s leader is %d\n", rf.me, rf.votedFor)
	//	}
	//}()

	return rf
}

func (rf *Raft) GetCommitIndex() int {
	rf.mu.Lock()
	index := rf.commitIndex
	rf.mu.Unlock()

	return index
}
