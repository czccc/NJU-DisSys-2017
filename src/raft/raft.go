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
	"bytes"
	"encoding/gob"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	Follower  string = "Follower"
	Candidate        = "Candidate"
	Leader           = "Leader"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	applyCh   chan ApplyMsg

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// self state
	state         string
	isActive      bool
	lastHeartBeat time.Time

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == Leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("-> %v %v Reeceive Server %v Term %v Request Vote", rf.state, rf.me, args.CandidateId, args.Term)

	reply.Term = rf.currentTerm

	// don't vote for this Candidate because its term is old
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		DPrintf("-> %v %v Dont vote for Server %v Term %v for a old term", rf.state, rf.me, args.CandidateId, args.Term)
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.becomeFollower(args.Term)
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
	} else if len(rf.log) > 0 && rf.log[len(rf.log)-1].Term > args.LastLogTerm {
		reply.VoteGranted = false //! different term
	} else if len(rf.log) > args.LastLogIndex && rf.log[len(rf.log)-1].Term == args.LastLogTerm {
		reply.VoteGranted = false //! same term but different index
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
	}

	// // receive newer term, become Follower and vote fot it
	// if rf.currentTerm < args.Term {
	// 	if len(rf.log) > 0 && (rf.log[len(rf.log)-1].Term > args.LastLogTerm || len(rf.log) > args.LastLogIndex) {
	// 		reply.VoteGranted = false
	// 		DPrintf("-> %v %v log %v term %v Dont vote for Server %v Term %v for old log %v term %v", rf.state, rf.me, len(rf.log), rf.log[len(rf.log)-1].Term, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	// 		return
	// 	}
	// 	rf.becomeFollower(args.Term)
	// 	rf.votedFor = args.CandidateId
	// 	reply.Term = rf.currentTerm
	// 	reply.VoteGranted = true
	// 	DPrintf("-> %v %v Vote for Server %v Term %v", rf.state, rf.me, args.CandidateId, args.Term)
	// 	return
	// }

	// if rf.currentTerm == args.Term {
	// 	// have voted before and didn't vote for this Candidate
	// 	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
	// 		DPrintf("-> %v %v Dont vote for Server %v Term %v for vote before", rf.state, rf.me, args.CandidateId, args.Term)
	// 		reply.VoteGranted = false
	// 		reply.Term = rf.currentTerm
	// 		return
	// 	} else {
	// 		if len(rf.log) > 0 && rf.log[len(rf.log)-1].Term > args.LastLogTerm && len(rf.log) > args.LastLogIndex {
	// 			reply.VoteGranted = false
	// 			DPrintf("-> %v %v log %v term %v Dont vote for Server %v Term %v for old log %v term %v", rf.state, rf.me, len(rf.log), rf.log[len(rf.log)-1].Term, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	// 			return
	// 		}
	// 		rf.votedFor = args.CandidateId
	// 		reply.VoteGranted = true
	// 		DPrintf("-> %v %v Vote for Server %v Term %v", rf.state, rf.me, args.CandidateId, args.Term)
	// 		return
	// 	}
	// }

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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Append Entries Code

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm <= args.Term {
		DPrintf("--> Server %v - Term %v - log %v - Receive AppendEntries of leader %v, Term %v, log len : %v", rf.me, rf.currentTerm, len(rf.log), args.LeaderID, rf.currentTerm, len(args.Entries))
		// if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term)
		// }

		reply.Term = rf.currentTerm
		rf.lastHeartBeat = time.Now()

		logLen := len(rf.log)
		if args.PrevLogIndex == 0 {
			if len(rf.log) == 0 {
				reply.Success = true
				rf.log = append(args.Entries)
				return
				// add log
			} else {
				// for i := 0; i < len(args.Entries); i++ {
				// 	if i < logLen && rf.log[i] != args.Entries[i] {
				// 		reply.Success = false
				// 		return
				// 	}
				// }
				reply.Success = true
				rf.log = append(args.Entries)
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log))))
				}
				rf.persist()
				return
			}
		}
		if logLen < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.Success = false
			return
		} else {
			// for i := 0; i < len(args.Entries); i++ {
			// 	if args.PrevLogIndex+i-1 < logLen && rf.log[args.PrevLogIndex+i-1] != args.Entries[i] {
			// 		reply.Success = false
			// 		return
			// 	}
			// }
			reply.Success = true
			rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log))))
			}
			rf.persist()
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				msg := ApplyMsg{}
				msg.Index = rf.lastApplied
				msg.Command = rf.log[rf.lastApplied-1].Command
				rf.applyCh <- msg
			}
			return
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
	term := rf.currentTerm
	isLeader := (rf.state == Leader)

	if isLeader {
		rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
		index = len(rf.log)
		rf.persist()
		DPrintf("Server %v - Receive Command ! Append to index %v of %v", rf.me, index, len(rf.log))
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.isActive = false
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.isActive = true
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("Raft: Node created Server: %v Term: %d %v", rf.me, rf.currentTerm, rf.state)

	go rf.termTimeout()

	return rf
}

func (rf *Raft) becomeFollower(term int) {
	if rf.currentTerm != term {
		DPrintf("Server %v: %v\t->\t%v\tterm: %v -> %v", rf.me, rf.state, Follower, rf.currentTerm, term)
	}
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	DPrintf("Server %v: %v\t->\t%v\tterm: %v -> %v", rf.me, rf.state, Candidate, rf.currentTerm, rf.currentTerm+1)
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	DPrintf("Server %v: %v\t->\t%v  \tterm: %v", rf.me, rf.state, Leader, rf.currentTerm)
	rf.state = Leader
	rf.votedFor = rf.me
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
	rf.persist()

	HeartBeatInterval := 50 * time.Millisecond
	go func() {
		for {
			if _, isLeader := rf.GetState(); !isLeader {
				break
			} else if rf.isActive {
				rf.mu.Lock()
				rf.sendStartBeat()
				rf.mu.Unlock()
			}
			<-time.After(HeartBeatInterval)
		}
	}()

}

func (rf *Raft) termTimeout() {
	randomTimeout := (time.Duration(150 + rand.Intn(150))) * time.Millisecond
	currentTime := <-time.After(randomTimeout)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader && currentTime.Sub(rf.lastHeartBeat) >= randomTimeout {
		DPrintf("-> Server %v %v term: %v loss Heart Beat, reelection", rf.me, rf.state, rf.currentTerm)
		go rf.startElection()
	} else if rf.isActive {
		go rf.termTimeout()
	}
}

func (rf *Raft) startElection() {
	rf.becomeCandidate()
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  lastLogTerm,
	}
	go rf.termTimeout()
	voteReplies := make([]RequestVoteReply, len(rf.peers))
	getVoteNum := 1
	for i := range rf.peers {
		if i != rf.me {
			go func(rf *Raft, i int) {
				ok := rf.sendRequestVote(i, voteArgs, &voteReplies[i])
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != Candidate {
						return
					}
					if voteReplies[i].Term > rf.currentTerm {
						rf.becomeFollower(voteReplies[i].Term)
						return
					}
					if voteReplies[i].VoteGranted {
						getVoteNum++
					}
					if getVoteNum > len(rf.peers)/2 {
						rf.becomeLeader()
					}
				}
			}(rf, i)
		}
	}

}

func (rf *Raft) sendStartBeat() {
	// replies := make([]AppendEntriesReply, len(rf.peers))
	DPrintf("--> Leader %v - Term %d - log %v - Sending heartbeats to followers", rf.me, rf.currentTerm, len(rf.log))
	for i := range rf.peers {
		if i != rf.me {
			go func(rf *Raft, i int) {
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := 0
				if prevLogIndex > 0 {
					prevLogTerm = rf.log[prevLogIndex-1].Term
				}
				entries := append([]LogEntry{}, rf.log[prevLogIndex:]...)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}

				// if len(entries) > 0 {
				// DPrintf("Leader %v -> Server %v: Send %v Entries", rf.me, i, len(entries))
				// }

				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != Leader {
						return
					}
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
						return
					}
					if reply.Success {
						rf.matchIndex[i] = prevLogIndex + len(entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						copyMatchIndex := make([]int, len(rf.peers))
						copy(copyMatchIndex, rf.matchIndex)
						copyMatchIndex[rf.me] = len(rf.log)
						sort.Ints(copyMatchIndex)
						N := copyMatchIndex[len(rf.peers)/2]
						if N > rf.commitIndex && rf.log[N-1].Term == rf.currentTerm {
							rf.commitIndex = N
						}
						for rf.lastApplied < rf.commitIndex {
							rf.lastApplied++
							msg := ApplyMsg{}
							msg.Index = rf.lastApplied
							msg.Command = rf.log[rf.lastApplied-1].Command
							rf.applyCh <- msg
						}
					} else {
						rf.nextIndex[i]--
						if rf.nextIndex[i] < 1 {
							rf.nextIndex[i] = 1
						}
					}
				}
			}(rf, i)
		}
	}
}
