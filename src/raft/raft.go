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
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
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

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm int
	votedFor	int
	log			[]LogEntry

	// Volatile state
	commitIndex int
	lastApplied int

	// Volatile state, only needed by leaders
	nextIndex 	[]int			// leader's guess at what index to send next to that follower, gradually fall back if not identical
								// "I think you need entries from here"
	matchIndex 	[]int			// leader's confirmed knowledge of how far a follower's log matches its own
								// "I know for certain you have up to here"

	// bookkeeping
	state		string  		// follower, candidate, leader
	lastHeartbeat time.Time
}

type LogEntry struct {
	Term int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).

	// Isolated get
	rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.currentTerm, rf.state == "leader"
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
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
	// Your code here (3C).
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
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term 			int
	VoteGranted		bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// 1, Stale candidate --> reject
	if args.Term < rf.currentTerm { return }

	// 2, higher than itself --> update and demote
	if args.Term > rf.currentTerm { 
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.votedFor = -1 
	}

	// 3, already voted --> reject
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		//rf.lastHeartbeat = time.Now() --> not needed
		// Already voted for a different candidate this term — reject.
		// Do NOT reset heartbeat here: a rejected candidate is not evidence
		// the cluster is making progress. Timer stays as-is so we remain
		// ready to start a legitimate election if needed.
		return 
	}

	// 4, check 
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term
	logOk := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
			// more up to date than cur		// exact up to date term, but whoever has more entries is more up-to-date. 
	if !logOk {return}	// reject if not satisfied

	// Grant vote
	reply.VoteGranted = true

	// Update heartbeat time and votedfor
	rf.votedFor = args.CandidateId
	rf.lastHeartbeat = time.Now()
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Only the leader accepts new commands
	if rf.state != "leader" {
		return -1, rf.currentTerm, false
	}

	// If not leader, append to own log immediately
	entry := LogEntry{
		Term: rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	index := len(rf.log) - 1
	term := rf.currentTerm

	// Send heartbeats to inform the peers that it is the leader
	go rf.sendHeartbeats()
	return index, term, true
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.


		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)

		// NOT USED
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

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = "follower"
	rf.lastHeartbeat = time.Now()
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTimeoutLoop()

	return rf
}


// Election timeout loop to start elections when necessary
func (rf *Raft) electionTimeoutLoop() {
	// Loop until the node is killed
	for !rf.killed() {
		// 250~400 ms random waiting time, higher to allow heartbeat to arrive first
		sleepDuration := time.Duration(250 + rand.Intn(150)) * time.Millisecond
		time.Sleep(sleepDuration)

		// Lock RF to prevent concurrency
		// Start RF operations
		rf.mu.Lock()
		// Start election if RF is not leader or not received heartbeat during sleeping --> leader suspicious dead / no leader
		if rf.state != "leader" && time.Since(rf.lastHeartbeat) >= sleepDuration {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.state = "candidate"		// change back to candidate
	rf.currentTerm ++ 			// raise term
	rf.votedFor = rf.me			// vote for himself
	votes := 1   				// votes count
	term := rf.currentTerm 		// snapshot term

	// loop through all peers and request vote
	for i := range rf.peers {
		// skip myself
		if i == rf.me {
			continue
		}

		// wrap in go for parallel programming
		go func(peer int) {
			// construct requestVotes
			args := &RequestVoteArgs{
				Term: term,									// this term
				CandidateId: rf.me,							// himself
				LastLogIndex: len(rf.log) - 1,				// last Log
				LastLogTerm: rf.log[len(rf.log) - 1].Term,	// last Log Term
			}
			reply := &RequestVoteReply{}

			// call RPC
			ok := rf.peers[peer].Call("Raft.RequestVote", args, reply) 
			if !ok { return }

			// lock to prevent concurrency
			rf.mu.Lock()
			defer rf.mu.Unlock() // unlock automatically upon exit of function

			// Check stale data inbetween --> ignore (things might get updated during the loop, example: winning already)
			if rf.currentTerm != term || rf.state != "candidate" {
				return 
			}

			// See someone with higher term --> step down
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term		// update term
				rf.state = "follower"			// change back to follwer
				rf.votedFor = -1				// reinit votedFor
				return
			}

			// Increment vote if vote is granted
			if reply.VoteGranted {
				votes ++ 
				// if majority than win automatic
				if votes >= len(rf.peers)/2 + 1 {
					rf.becomeLeader()
				}
			}
		}(i)
	}
}


func (rf *Raft) becomeLeader() {
    rf.state = "leader"
    // Initialize leader-only state
    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
    for i := range rf.peers {
        rf.nextIndex[i] = len(rf.log) // start optimistic, then gradually fall back
        rf.matchIndex[i] = 0 // assume that we have confirmed nothing yet
    }
    go rf.heartbeatLoop()
}


func (rf *Raft) heartbeatLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		// check if still leader while doing this
		if rf.state != "leader" {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		
		// if not leader, send heartbeat to all
		rf.sendHeartbeats()
		// wait 100 ms before next round of heartbeat
		// shorter than election timeout -->
			// If heartbeats arrive slower than the timeout
			// followers will think the leader died and start unnecessary elections
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeats() {
	// get current terms and info without concurrency
	rf.mu.Lock()
	term := rf.currentTerm
	LeaderId := rf.me
	commitIndex := rf.commitIndex
	rf.mu.Unlock()

	// loop to send heartbeat
	for i := range rf.peers {
		// skip himself
		if i == rf.me { continue }

		// parallel programming
		go func(peer int) {
			// Lock to prevent concurrency
			rf.mu.Lock()

			// Check whether is still leader
			if rf.state != "leader" {
				rf.mu.Unlock()
				return
			}

			// fetch nextIndex --> leader's guess at what index to send next to that follower
			ni := rf.nextIndex[peer]
			// check prevLog
			prevLogIndex := ni - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			// make entries
			entries := make([]LogEntry, len(rf.log[ni:]))
			copy(entries, rf.log[ni:])
			rf.mu.Unlock()


			// Construct and call AppendEntries
			args := &AppendEntriesArgs{
				Term: term,
				LeaderId: LeaderId,
				PrevLogIndex: prevLogIndex,
                PrevLogTerm:  prevLogTerm,
                Entries:      entries,
                LeaderCommit: commitIndex,
			}
			reply := &AppendEntriesReply{}
			ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)

			// false response --> no good, ignore (maybe network error/timeout/server unreachable)
			if !ok { return }

			// Lock to prevent concurrency
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Stale reply — ignore
            if rf.state != "leader" || rf.currentTerm != term { return }

			// Find out himself is stale --> demote
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = "follower"
				rf.votedFor = -1
			}

			// On Success
			if reply.Success {
				// Update prevLogIndex and matchIndex

				// compute newly matches index
				newMatch := prevLogIndex + len(entries)

				// update matched index if necessary
				if newMatch > rf.matchIndex[peer] {
					rf.matchIndex[peer] = newMatch
				}
				// set nextIndex
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			} else { 	// On Failure
				// back up one and retry next heartbeat
				if rf.nextIndex[peer] > 1 {
					rf.nextIndex[peer] --
				}
			}

		}(i)
	}
}

type AppendEntriesArgs struct {
	Term 		int				// current Term
	LeaderId	int				// heartbeat from node LeaderID
	PrevLogIndex	int			// Use Prev: because for consistency check during append/correction
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term 		int
	Success 	bool
}


// Heartbeat + replicate logs
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// stale leader, ignore
	if args.Term < rf.currentTerm { return }

	// valid heartbeat - reset election timer
	rf.lastHeartbeat = time.Now()		// reset last heartbeat
	rf.state = "follower"				// reset to follower
	rf.currentTerm = args.Term			// update term
	reply.Term = rf.currentTerm

	// no entry at PrevLogIndex in the log --> reject
	if args.PrevLogIndex >= len(rf.log) { return }
	// entry at PrevLogIndex is wrong --> reject
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { return }

	// append new entries and correct existing wrong entries, to be identical to leader
	// loop through all entries
	for i, entry := range args.Entries {
		// look for current index
		idx := args.PrevLogIndex + 1 + i
			// curr = prev + 1
			// i -> position index
		
		// For past log --> check conflict
		if idx < len(rf.log) {
			// Conflict --> Correction
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx]	// truncate from conflict point, and keep the logs before conflict
				rf.log = append(rf.log, args.Entries[i:]...)
				break // all corrected and appended, therefore break
			}
			// No conflict --> skip
			// else: do nothing
		} else {
			// Past end of our log 
			// Append all remaining entries from leader
			rf.log = append(rf.log, args.Entries[i:]...)
			break // all appended, therefore break
		}
	}

	// Logic to set commitIndex
	if args.LeaderCommit > rf.commitIndex {
		// New index after appending
		lastNewIndex := args.PrevLogIndex + len(args.Entries)

		// Clamp to what we actually have — leader may be ahead of what it sent us
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit  // leader is behind our new entries
		} else {
			rf.commitIndex = lastNewIndex		 // leader is ahead, cap at what we have
		}
	}

	reply.Success = true	// return true
}
