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
	"math/rand"
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

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
	log         []int

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
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term        int
	CandidateId int
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

	// don't vote for this Candidate because its term is old
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// receive newer term, become Follower and vote fot it
	if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	if rf.currentTerm == args.Term {
		// have voted before and didn't vote for this Candidate
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		} else {
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		}
	}

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
	Term     int
	LeaderID int
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
	if rf.currentTerm < args.Term {
		DPrintf("Server %v: Receive HeartBeat of leader: %v, Term: %v", rf.me, args.LeaderID, rf.currentTerm)
		rf.becomeFollower(args.Term)
		rf.lastHeartBeat = time.Now()
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm == args.Term {
		rf.lastHeartBeat = time.Now()
		DPrintf("Server %v: Receive HeartBeat of leader: %v, Term: %v", rf.me, args.LeaderID, rf.currentTerm)
		reply.Success = true
		reply.Term = rf.currentTerm
		return
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
	index := -1
	term := -1
	isLeader := true

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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("Raft: Node created Server: %v Term: %d %v", rf.me, rf.currentTerm, rf.state)

	go rf.termTimeout()

	return rf
}

func (rf *Raft) becomeFollower(term int) {
	DPrintf("Server %v: %v\t->\t%v\tterm: %v -> %v", rf.me, rf.state, Follower, rf.currentTerm, term)
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) becomeCandidate() {
	DPrintf("Server %v: %v\t->\t%v\tterm: %v -> %v", rf.me, rf.state, Candidate, rf.currentTerm, rf.currentTerm+1)
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) becomeLeader() {
	DPrintf("Server %v: %v\t->\t%v  \tterm: %v", rf.me, rf.state, Leader, rf.currentTerm)
	rf.state = Leader
	rf.votedFor = rf.me

	HeartBeatInterval := time.Duration(100 * time.Millisecond)
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
	randomTimeout := (time.Duration(500 + rand.Intn(100))) * time.Millisecond
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

	voteArgs := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
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
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me}
	replies := make([]AppendEntriesReply, len(rf.peers))
	DPrintf("-> Server %v: Term: %d %v - Sending heartbeats to followers", rf.me, rf.currentTerm, rf.state)
	for i := range rf.peers {
		if i != rf.me {
			go func(rf *Raft, i int) {
				ok := rf.sendAppendEntries(i, &args, &replies[i])
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != Leader {
						return
					}
					if replies[i].Term > rf.currentTerm {
						rf.becomeFollower(replies[i].Term)
						return
					}
				}
			}(rf, i)
		}
	}
}
