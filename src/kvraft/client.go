package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderServer *labrpc.ClientEnd
	leaderIndex  int
	seq          int
	cliNo        int64 // client number
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.mu = sync.Mutex{}
	ck.seq = 0
	ck.cliNo = nrand() // get a uniq client number
	return ck
}

// getLeader assert hold ck.mu
func (ck *Clerk) getLeader() (*labrpc.ClientEnd, int) {
	return ck.leaderServer, ck.leaderIndex
}

// setLeader assert hold ck.mu
func (ck *Clerk) setLeader(l *labrpc.ClientEnd, idx int) {
	ck.leaderServer = l
	ck.leaderIndex = idx
}

// getSeq get a uniq sequence number
func (ck *Clerk) getSeq() int {
	ck.seq++
	return ck.seq
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	seq := ck.getSeq()
	Debugf(dClient, "Client %d seq %d Get key %s\n",
		ck.cliNo, seq, key)
	req := GetArgs{
		Key:   key,
		Seq:   seq,
		CliNo: ck.cliNo,
	}
	// 1.try leader
	if leader, idx := ck.getLeader(); leader != nil {
		rsp := GetReply{}
		ok := leader.Call("KVServer.Get", &req, &rsp)
		if ok && rsp.OK() {
			Debugf(dClient, "Client %d Send Server%d seq %d Get key %s val %s succ1\n",
				ck.cliNo, idx, seq, key, rsp.Value)
			return rsp.Value
		}
	}
	// 2.try all servers
	for {
		for idx, s := range ck.servers {
			rsp := GetReply{}
			ok := s.Call("KVServer.Get", &req, &rsp)
			if ok && rsp.OK() {
				Debugf(dClient, "Client %d Send Server%d seq %d Get key %s val %s succ2\n",
					ck.cliNo, idx, seq, key, rsp.Value)
				ck.setLeader(s, idx) // Cache the leader
				return rsp.Value
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	seq := ck.getSeq()
	Debugf(dClient, "Client %d seq %d %s key %s val %s\n",
		ck.cliNo, seq, op, key, value)
	req := PutAppendArgs{
		Op:    op,
		Key:   key,
		Value: value,
		Seq:   seq,
		CliNo: ck.cliNo,
	}
	// 1.try leader
	if leader, idx := ck.getLeader(); leader != nil {
		rsp := PutAppendReply{}
		ok := leader.Call("KVServer.PutAppend", &req, &rsp)
		if ok && rsp.OK() {
			Debugf(dClient, "Client %d Send Server%d seq %d %s key %s val %s succ1\n",
				ck.cliNo, idx, seq, op, key, value)
			return
		}
	}
	// 2.try all servers
	for {
		for idx, s := range ck.servers {
			rsp := PutAppendReply{}
			ok := s.Call("KVServer.PutAppend", &req, &rsp)
			if ok && rsp.OK() {
				Debugf(dClient, "Client %d Send Server%d seq %d %s key %s val %s succ2\n",
					ck.cliNo, idx, seq, op, key, value)
				ck.setLeader(s, idx) // Cache the leader
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutCmd)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendCmd)
}
