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

	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	ZeroIndex        = 0
	ZeroTerm         = 0
	None             = -1
	Follower         = 0
	Candidater       = 1
	Leader           = 2
	HeartBeatTimeout = 500
	LeaderTrue       = true
	LeaderFalse      = false
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

func getMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func getMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func entriesStr(entries []*Entry) string {
	d, _ := json.Marshal(entries)
	return string(d)
}

// Entry for raft log
type Entry struct {
	Command   interface{}
	Index     int
	Term      int
	Committed bool
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term     int  // Current Term
	state    int  // Current State
	isLeader bool // IsLeader
	voteFor  int  // Vote for which peer in this term

	lastHeartBeatTime time.Time // Last receive heart beat time

	applyCh       chan ApplyMsg
	entries       []*Entry // Raft logs
	commitedIndex int      // Leader's commited log index
	nextIndex     []int    // Next index for followers
}

// getMajorityCount
func (rf *Raft) getMajorityCount() int {
	return len(rf.peers)/2 + 1
}

// updateHeartBeatTime
func (rf *Raft) updateHeartBeatTime() {
	rf.lastHeartBeatTime = time.Now()
}

// getPeerCount
func (rf *Raft) getPeerCount() int {
	return len(rf.peers)
}

// getPeerInfo get term, isLeadr, state, vote info
func (rf *Raft) getPeerInfo() (int, bool, int, int) {
	return rf.term, rf.isLeader, rf.state, rf.voteFor
}

func (rf *Raft) getPeerInfoStr() string {
	return fmt.Sprintf("[%d %d %d]", rf.term, rf.state, rf.voteFor)
}

// GetPeerInfo call getPeerInfo with lock
func (rf *Raft) GetPeerInfo() (int, bool, int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getPeerInfo()
}

// modifyPeerInfo
func (rf *Raft) modifyPeerInfo(t int, l bool, s, v int) {
	if t < rf.term {
		Debugf(dError, "S%d modifyPeerInfo error Term %d CurrentTerm %d",
			t, rf.term)
		return
	}
	rf.term, rf.isLeader, rf.state, rf.voteFor = t, l, s, v
}

// getEntryInfo at idx
func (rf *Raft) getEntryInfo(idx int) (int, int) {
	if idx == ZeroIndex {
		return ZeroIndex, ZeroTerm
	}
	e := rf.entries[idx-1]
	return e.Index, e.Term
}

// getLastEntryInfo
func (rf *Raft) getLastEntryInfo() (int, int) {
	return rf.getEntryInfo(len(rf.entries))
}

// heartBeatTimeout
func (rf *Raft) heartBeatTimeout() bool {
	return time.Since(rf.lastHeartBeatTime)/time.Millisecond > HeartBeatTimeout
}

// couldVote for request peer
func (rf *Raft) couldVote(req *RequestVoteArgs) bool {
	lastLogInex, lastLogTerm := rf.getLastEntryInfo()
	if (req.Term > rf.term || (req.Term == rf.term && rf.voteFor == None)) &&
		((req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogInex) || req.LastLogTerm > lastLogTerm) {
		return true
	}
	return false
}

// matchLog
func (rf *Raft) matchLog(index, term int) bool {
	if index > len(rf.entries) {
		return false
	}
	i, t := rf.getEntryInfo(index)
	return i == index && t == term
}

// mergeLog
func (rf *Raft) mergeLog(prevLogIndex int, entries []*Entry) {
	i, j := 0, prevLogIndex
	for j < len(rf.entries) && i < len(entries) {
		if !rf.matchLog(entries[i].Index, entries[i].Term) {
			break
		}
		i++
		j++
	}
	// truncate and break
	rf.entries = rf.entries[:j]
	for ; i < len(entries); i++ {
		entries[i].Committed = false
		rf.entries = append(rf.entries, entries[i])
	}
}

// getEntryInfoAux
func getEntryInfoAux(entries []*Entry, idx int) (int, int) {
	if idx == ZeroIndex {
		return ZeroIndex, ZeroTerm
	}
	e := entries[idx-1]
	return e.Index, e.Term
}

// getSendAppendEntryInfo
func (rf *Raft) getSendAppendEntryInfo(nextIndex int, entries []*Entry) (int, int, []*Entry) {
	idx, term := getEntryInfoAux(entries, nextIndex-1)
	logs := make([]*Entry, 0)
	if nextIndex > ZeroIndex {
		logs = entries[nextIndex-1:]
	}
	return idx, term, logs
}

// HeartBeatTimeout
func (rf *Raft) HeartBeatTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.heartBeatTimeout()
}

// getMajorityIndex
func (rf *Raft) getMajorityIndex(nextIndex []int) int {
	sort.Ints(nextIndex)
	return nextIndex[len(nextIndex)-rf.getMajorityCount()] - 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	term, isLeader, _, _ := rf.getPeerInfo()
	Debugf(dInfo, "S%d GetState Term %d IsLeader %t",
		rf.me, term, isLeader)
	return term, isLeader
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

// Start a leader election if need
func (rf *Raft) startLeaderElection() {
	rf.mu.Lock()
	term, _, state, _ := rf.getPeerInfo()
	if state != Follower || !rf.heartBeatTimeout() {
		rf.mu.Unlock()
		return
	}
	rf.modifyPeerInfo(term+1, LeaderFalse, Candidater, rf.me)
	term, _, _, _ = rf.getPeerInfo()
	lastLogIndex, lastLogTerm := rf.getLastEntryInfo()
	Debugf(dInfo, "S%d start leader election term %d lastLogIndex %d lastLogTerm %d",
		rf.me, term, lastLogIndex, lastLogTerm)
	rf.mu.Unlock()

	// make a leader election
	cond := sync.NewCond(&rf.mu)
	rpcFailed := 0 // guard by cond.L
	voteCount := 1 // guard by cond.L
	finished := 1  // guard by cond.L
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			req := RequestVoteArgs{
				PeerInfo:     PeerInfo{Peer: rf.me, Term: term},
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			rsp := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &req, &rsp)
			cond.L.Lock()
			defer cond.L.Unlock()
			finished++
			if !ok {
				rpcFailed++
				cond.Broadcast()
				return
			}
			if rsp.Term == term && rsp.VoteFor == rf.me {
				voteCount++
			}
			cond.Broadcast()
		}(peer)
	}
	cond.L.Lock()
	defer cond.L.Unlock()
	for voteCount < rf.getMajorityCount() && finished < rf.getPeerCount() && rpcFailed < rf.getMajorityCount() {
		cond.Wait()
	}
	if term == rf.term && voteCount >= rf.getMajorityCount() {
		rf.modifyPeerInfo(rf.term, LeaderTrue, Leader, rf.me)
		Debugf(dLeader, "S%d become leader Term %d VoteCount %d",
			rf.me, rf.term, voteCount)
		idx, _ := rf.getLastEntryInfo()
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = idx + 1
		}
	} else {
		rf.modifyPeerInfo(rf.term, LeaderFalse, Follower, rf.voteFor)
		Debugf(dLeader, "S%d not become leader Term %d VoteCount %d",
			rf.me, rf.term, voteCount)
	}
	// update heart beat time anyway
	rf.updateHeartBeatTime()
}

func (rf *Raft) leaderSendAppendEntry() {
	rf.mu.Lock()
	term, isLeader, state, voteFor := rf.getPeerInfo()
	_, _ = state, voteFor
	if !isLeader {
		rf.mu.Unlock()
		return
	}
	entries := make([]*Entry, 0)
	entries = append(entries, rf.entries...)
	nextIndex := make([]int, 0)
	nextIndex = append(nextIndex, rf.nextIndex...)
	lastLogIndex, _ := rf.getLastEntryInfo()
	nextIndex[rf.me] = lastLogIndex + 1
	committedIndex := rf.commitedIndex
	rf.mu.Unlock()

	// start send append entry to peers
	cond := sync.NewCond(&rf.mu)
	rpcFailed := 0          // guard by cond.L
	heartBeatOKCount := 1   // guard by cond.L
	appendEntryOKCount := 1 // guard by cond.L
	finished := 1           // guard by cond.L
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			nIdx := nextIndex[peer]
			prevLogIndex, prevLogTerm, logs := rf.getSendAppendEntryInfo(nIdx, entries)
			req := AppendEntryRequest{
				PeerInfo:      PeerInfo{Peer: rf.me, Term: term},
				PrevLogIndex:  prevLogIndex,
				PrevLogTerm:   prevLogTerm,
				Entries:       logs,
				CommitedIndex: committedIndex,
			}
			rsp := AppendEntryResponse{}
			ok := rf.sendAppendEntry(peer, &req, &rsp)
			cond.L.Lock()
			defer cond.L.Unlock()
			finished++
			if !ok {
				rpcFailed++
				cond.Broadcast()
				return
			}
			if term == rsp.Term {
				heartBeatOKCount++
				if rsp.Succ {
					appendEntryOKCount++
					nextIndex[peer] = prevLogIndex + len(req.Entries) + 1
					rf.nextIndex[peer] = prevLogIndex + len(req.Entries) + 1
				} else {
					nextIndex[peer] = ZeroIndex + 1
					rf.nextIndex[peer] = ZeroIndex + 1
				}
			}
			cond.Broadcast()
		}(peer)
	}
	cond.L.Lock()
	defer cond.L.Unlock()
	for heartBeatOKCount < rf.getMajorityCount() && finished < rf.getPeerCount() && rpcFailed < rf.getMajorityCount() {
		cond.Wait()
	}
	if heartBeatOKCount >= rf.getMajorityCount() {
		Debugf(dTimer, "S%d send append entry succ", rf.me)
		rf.updateHeartBeatTime()
	}
	if term == rf.term && appendEntryOKCount >= rf.getMajorityCount() {
		tmpIndex := make([]int, len(nextIndex))
		copy(tmpIndex, nextIndex)
		newCommitedIndex := rf.getMajorityIndex(tmpIndex)
		_, logTerm := rf.getEntryInfo(newCommitedIndex)
		Debugf(dCommit, "S%d leader commitIndex %d newCommitedIndex %d",
			rf.me, rf.commitedIndex, newCommitedIndex)
		if rf.commitedIndex < newCommitedIndex && logTerm == rf.term {
			for i := rf.commitedIndex; i < newCommitedIndex; i++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.entries[i].Command,
					CommandIndex: rf.entries[i].Index,
				}
				rf.entries[i].Committed = true
				Debugf(dCommit, "S%d leader commit at %d term %d command %v",
					rf.me, i, rf.term, rf.entries[i].Command)
			}
			rf.commitedIndex = newCommitedIndex
		}
	}
	if rf.heartBeatTimeout() {
		// fail to send heart beat, convert leader to follower
		Debugf(dTimer, "S%d send append entry fail and timeout heartBeatOKCount %d %d %d",
			rf.me, heartBeatOKCount, finished, rpcFailed)
		rf.modifyPeerInfo(rf.term, LeaderFalse, Follower, rf.voteFor)
	}
}

// PeerInfo record raft term and peer
type PeerInfo struct {
	Peer int
	Term int
}

func (r *PeerInfo) GetInfo() (int, int) {
	return r.Term, r.Peer
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	PeerInfo
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	PeerInfo
	VoteFor int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	li, lt := rf.getLastEntryInfo()
	Debugf(dLog2, "S%d receive request vote from S%d req %v term %d li %d lt %d entries %s",
		rf.me, args.Peer, *args, rf.term, li, lt, entriesStr(rf.entries))
	if rf.couldVote(args) {
		Debugf(dVote, "S%d vote for S%d", rf.me, args.Peer)
		// convert peer to follower state and update heart beat time
		rf.modifyPeerInfo(args.Term, LeaderFalse, Follower, args.Peer)
		rf.updateHeartBeatTime()
	} else if args.Term > rf.term {
		// update peer state but not update heart beart time
		rf.modifyPeerInfo(args.Term, LeaderFalse, Follower, None)
	}
	reply.Peer, reply.Term, reply.VoteFor = rf.me, rf.term, rf.voteFor
}

type AppendEntryRequest struct {
	PeerInfo
	PrevLogIndex  int
	PrevLogTerm   int
	Entries       []*Entry
	CommitedIndex int
}

type AppendEntryResponse struct {
	PeerInfo
	Succ bool
}

func (rf *Raft) AppendEntry(req *AppendEntryRequest, rsp *AppendEntryResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debugf(dLog2, "S%d receive append entry from S%d req %v entries %s",
		rf.me, req.Peer, *req, entriesStr(req.Entries))
	if req.Term >= rf.term {
		rf.modifyPeerInfo(req.Term, LeaderFalse, Follower, req.Peer)
		rf.updateHeartBeatTime()
		Debugf(dLog2, "S%d follower entries %s", rf.me, entriesStr(rf.entries))
		// check prev log entry, if match then merge entries sent from leader
		if rf.matchLog(req.PrevLogIndex, req.PrevLogTerm) {
			// merge log
			rf.mergeLog(req.PrevLogIndex, req.Entries)
			li, _ := rf.getLastEntryInfo()
			newCommitedIndex := getMin(li, req.CommitedIndex)
			Debugf(dCommit, "S%d follower commitIndex %d newCommitedIndex %d",
				rf.me, rf.commitedIndex, newCommitedIndex)
			if rf.commitedIndex < newCommitedIndex {
				// apply msg
				for i := rf.commitedIndex; i < newCommitedIndex; i++ {
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.entries[i].Command,
						CommandIndex: rf.entries[i].Index,
					}
					rf.entries[i].Committed = true
					Debugf(dCommit, "S%d follower commit at %d term %d command %v",
						rf.me, i, rf.term, rf.entries[i].Command)
				}
				rf.commitedIndex = newCommitedIndex
			}
			rsp.Succ = true
		}
	}
	rsp.Peer, rsp.Term = rf.me, rf.term
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
	start := time.Now()
	logId := rand.Uint64()
	Debugf(dClient, "S%d send[%d] request vote to S%d req %v",
		rf.me, logId, server, *args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	Debugf(dClient, "S%d send[%d] request vote to S%d req %v rpc %t time %d",
		rf.me, logId, server, *args, ok, time.Since(start)/time.Millisecond)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryRequest, reply *AppendEntryResponse) bool {
	start := time.Now()
	logId := rand.Uint64()
	Debugf(dLog, "S%d send[%d] append entry to S%d req %v",
		rf.me, logId, server, *args)
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	Debugf(dLog, "S%d send[%d] append entry to S%d req %v rpc %t time %d",
		rf.me, logId, server, *args, ok, time.Since(start)/time.Millisecond)
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := false
	currentTerm, leader, _, _ := rf.getPeerInfo()
	lastLogInex, _ := rf.getLastEntryInfo()
	if leader {
		isLeader = leader
		term = currentTerm
		index = lastLogInex + 1
		rf.entries = append(rf.entries, &Entry{
			Command:   command,
			Index:     index,
			Term:      currentTerm,
			Committed: false,
		})
		rf.nextIndex[rf.me] = index + 1
	}
	Debugf(dStart, "S%d start command %v index %d term %d isLeader %t",
		rf.me, command, index, term, isLeader)
	return index, term, isLeader
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

		// Your code here (2A)
		// Check if a leader election should be started.
		term, isLeader, state, voteFor := rf.GetPeerInfo()
		Debugf(dTimer, "S%d timer check Term %d IsLeader %t, State %d VoteFor %d",
			rf.me, term, isLeader, state, voteFor)
		if state == Follower {
			// check leader election
			go rf.startLeaderElection()
		} else if state == Leader {
			// leader send append entry to followers
			go rf.leaderSendAppendEntry()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
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
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.modifyPeerInfo(ZeroTerm, false, Follower, None)
		rf.updateHeartBeatTime()
		rf.entries = make([]*Entry, 0)
		rf.commitedIndex = ZeroIndex
		rf.nextIndex = make([]int, 0)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex = append(rf.nextIndex, ZeroIndex+1)
		}
	}()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
