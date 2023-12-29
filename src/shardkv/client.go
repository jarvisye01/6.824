package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	mu       sync.Mutex
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	seq   int
	cliNo int64 // client number
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.seq = 0
	ck.cliNo = nrand()
	return ck
}

// SetCliNo set cliNo for clerk
func (ck *Clerk) SetCliNo(n int64) {
	ck.cliNo = n
}

// setSeq set seq for clerk
func (ck *Clerk) SetSeq(n int) {
	ck.seq = n
}

// getSeq get q uniq sequence number
func (ck *Clerk) getSeq() int {
	ck.seq++
	return ck.seq
}

// getCurSeq get currrnet seq
func (ck *Clerk) GetCurSeq() int {
	return ck.seq
}

// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{
		ClientInfo: ClientInfo{
			Seq:      ck.getSeq(),
			ClientNo: ck.cliNo,
		},
		Key: key,
	}
	Debugf(dClient, "Client%d Seq %d Get Key %s\n",
		ck.cliNo, args.Seq, key)
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.OK() || reply.Err == ErrNoKey) {
					Debugf(dClient, "Client_%d_%d Seq %d Rg %s Get Key %s Value %s Succ",
						ck.cliNo, gid, args.Seq, servers[si], args.Key, reply.Value)
					return reply.Value
				}
				Debugf(dClient, "Client_%d_%d Seq %d Rg %s Get Key %s Value %s error %s ok %t Fail",
					ck.cliNo, gid, args.Seq, servers[si], args.Key, reply.Value, reply.Err, ok)
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{
		ClientInfo: ClientInfo{
			Seq:      ck.getSeq(),
			ClientNo: ck.cliNo,
		},
		Key:   key,
		Value: value,
		Op:    op,
	}
	Debugf(dClient, "Client%d Seq %d %s Key %s Value %s\n",
		ck.cliNo, args.Seq, args.Op, key, args.Value)
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.OK() {
					Debugf(dClient, "Client_%d_%d Seq %d Rg %s %s Key %s Value %s Succ\n",
						ck.cliNo, gid, args.Seq, servers[si], args.Op, key, args.Value)
					return
				}
				Debugf(dClient, "Client_%d_%d Seq %d Rg %s %s to gid %d error %s ok %t fail",
					ck.cliNo, gid, args.Seq, servers[si], args.Op, gid, reply.Err, ok)
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) Migrate(shard int, m map[string]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	req := MigrateArgs{
		ClientInfo: ClientInfo{
			Seq:      ck.getSeq(),
			ClientNo: ck.cliNo,
		},
		Shard: shard,
		M:     m,
	}
	for {
		ck.config = ck.sm.Query(-1)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				svr := ck.make_end(servers[si])
				var rsp MigrateReply
				Debugf(dClient, "Client%d Seq %d Migrate shard %d to gid %d server %s",
					ck.cliNo, req.Seq, shard, gid, servers[si])
				ok := svr.Call("ShardKV.MigrateShards", &req, &rsp)
				if ok && rsp.OK() {
					Debugf(dClient, "Client%d Seq %d Migrate shard %d to gid %d server %s succ",
						ck.cliNo, req.Seq, shard, gid, servers[si])
					return
				}
				Debugf(dClient, "Client%d Seq %d Migrate shard %d to gid %d server %s error %s ok %t fail",
					ck.cliNo, req.Seq, shard, gid, servers[si], rsp.Err, ok)
				// migrate fail, need retry
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) ReConfig(gid int, servers []*labrpc.ClientEnd, oldConfig, newConfig shardctrler.Config) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	req := ReConfigArgs{
		ClientInfo: ClientInfo{
			Seq:      ck.getSeq(),
			ClientNo: ck.cliNo,
		},
		OldConfig: oldConfig,
		NewConfig: newConfig,
	}
	for {
		for si := 0; si < len(servers); si++ {
			svr := servers[si]
			var rsp ReConfigReply
			Debugf(dClient, "Client%d Seq %d ReConfig to gid %d server[%s]",
				ck.cliNo, req.Seq, gid, servers[si])
			ok := svr.Call("ShardKV.ReConfig", &req, &rsp)
			if ok && rsp.OK() {
				Debugf(dClient, "Client%d Seq %d ReConfig to gid %d rg %s succ",
					ck.cliNo, req.Seq, gid, servers[si])
				return
			}
			// reconfig fail, need retry
			Debugf(dClient, "Client%d Seq %d ReConfig to gid %d error %s rg %s ok %t fail",
				ck.cliNo, req.Seq, gid, rsp.Err, servers[si], ok)
		}
	}
}
