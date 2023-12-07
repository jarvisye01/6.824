package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

func getMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Op command will be sent to raft peer
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Val   string
	Seq   int
	CliNo int64
}

type ClientCommand struct {
	CliCmd string
	CliNo  int64
	Seq    int
	Req    interface{}
	Rsp    interface{}
	Index  int
	Term   int
	Done   chan struct{}
}

func (c *ClientCommand) matchMsg(m *raft.ApplyMsg) bool {
	return c.Index == m.CommandIndex && c.Term == m.CommandTerm
}

type ClientLatestResponse struct {
	Seq int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientLastestRequestMap map[int64]ClientLatestResponse // latest client request result
	kvStore                 map[string]string              // store key and value
	clientCommandChannel    chan *ClientCommand            // client request channel
	waitApplyMsgCommandMap  map[string]*ClientCommand      // client wait apply message from raft
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	Debugf(dServer, "Server%d receive Get command client %d seq %d key %s",
		kv.me, args.CliNo, args.Seq, args.Key)

	var cliCmd *ClientCommand
	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if kv.killed() {
			Debugf(dError, "Server%d Killed", kv.me)
			reply.Err = "Server killed"
			return
		}
		// check has been processed
		if val, _ := kv.clientLastestRequestMap[args.CliNo]; args.Seq < val.Seq {
			Debugf(dError, "Server%d recieve old request seq %s lasted seq %s",
				kv.me, args.Seq, val.Seq)
			reply.Err = "Old request"
			return
		}

		cliCmd = &ClientCommand{
			CliCmd: GetCmd,
			CliNo:  args.CliNo,
			Seq:    args.Seq,
			Req:    args,
			Rsp:    reply,
			Done:   make(chan struct{}),
		}
		Debugf(dInfo, "Server%d client command channel size %d",
			kv.me, len(kv.clientCommandChannel))
		kv.clientCommandChannel <- cliCmd
	}()
	if cliCmd != nil {
		<-cliCmd.Done
	}
	Debugf(dServer, "Server%d reply Get command %v response %v",
		kv.me, *args, *reply)
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	Debugf(dServer, "Server%d receive PutAppend command client %d seq %d %s key %s val %s",
		kv.me, args.CliNo, args.Seq, args.Op, args.Key, args.Value)

	var cliCmd *ClientCommand
	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if kv.killed() {
			Debugf(dError, "Server%d Killed", kv.me)
			reply.Err = "Server killed"
			return
		}
		// check has been processed
		if val, _ := kv.clientLastestRequestMap[args.CliNo]; args.Seq < val.Seq {
			Debugf(dError, "Server%d recieve old request seq %s lasted seq %s",
				kv.me, args.Seq, val.Seq)
			reply.Err = "Old request"
			return
		}

		cliCmd = &ClientCommand{
			CliCmd: args.Op,
			CliNo:  args.CliNo,
			Seq:    args.Seq,
			Req:    args,
			Rsp:    reply,
			Done:   make(chan struct{}),
		}
		Debugf(dInfo, "Server%d client command channel size %d",
			kv.me, len(kv.clientCommandChannel))
		kv.clientCommandChannel <- cliCmd
	}()
	if cliCmd != nil {
		<-cliCmd.Done
	}
	Debugf(dServer, "Server%d reply PutAppend command %v response %v",
		kv.me, *args, *reply)
	return
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.clientCommandChannel) // close clientCommandChannel
	for cliCmd := range kv.clientCommandChannel {
		if cliCmd != nil {
			kv.fastFail(cliCmd, fmt.Errorf("Server%d Killed", kv.me))
		}
	}
	for k, cliCmd := range kv.waitApplyMsgCommandMap {
		delete(kv.waitApplyMsgCommandMap, k)
		if cliCmd != nil {
			kv.fastFail(cliCmd, fmt.Errorf("Server%d Killed", kv.me))
		}
	}
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// encodeKey encode seq and cliNo to a key
func (kv *KVServer) encodeKey(seq int, cliNo int64) string {
	return fmt.Sprintf("%d_%d", seq, cliNo)
}

// fillOp
func (kv *KVServer) fillOp(o *Op, cliCmd *ClientCommand) {
	o.Op = cliCmd.CliCmd
	o.CliNo, o.Seq = cliCmd.CliNo, cliCmd.Seq
	switch cliCmd.CliCmd {
	case GetCmd:
		req := cliCmd.Req.(*GetArgs)
		o.Key = req.Key
	case PutCmd:
		fallthrough
	case AppendCmd:
		req := cliCmd.Req.(*PutAppendArgs)
		o.Key = req.Key
		o.Val = req.Value
	}
}

// addToWaitApplyMsgCommandMap add client request command to kv.waitApplyMsgCommandMap
// After invoke Raft.Start successfully
func (kv *KVServer) addToWaitApplyMsgCommandMap(key string, cliCmd *ClientCommand) error {
	Debugf(dInfo, "Server%d add key %s cliCmd %v waitApplyMsgCommandMap %v",
		kv.me, key, cliCmd, kv.waitApplyMsgCommandMap)
	_, ok := kv.waitApplyMsgCommandMap[key]
	if ok {
		Debugf(dError, "Server%d element exist key %s", key)
		return fmt.Errorf("Element Exist %s", key)
	}
	kv.waitApplyMsgCommandMap[key] = cliCmd
	return nil
}

// fastFail reply request an error
func (kv *KVServer) fastFail(cliCmd *ClientCommand, err error) {
	switch cliCmd.CliCmd {
	case GetCmd:
		rsp := cliCmd.Rsp.(*GetReply)
		rsp.Err = Err(err.Error())
	case PutCmd:
		fallthrough
	case AppendCmd:
		rsp := cliCmd.Rsp.(*PutAppendReply)
		rsp.Err = Err(err.Error())
	}
	cliCmd.Done <- struct{}{}
}

// doApply  process msg from Raft
func (kv *KVServer) doApply(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if msg.CommandValid {
		op := msg.Command.(Op)
		cliKey := kv.encodeKey(op.Seq, op.CliNo)
		cliCmd := kv.waitApplyMsgCommandMap[cliKey]
		if op.Op != EmptyCmd {
			Debugf(dServer, "Server%d apply key %s cliNo %d seq %d msg %v",
				kv.me, op.Key, op.CliNo, op.Seq, *msg)
		}
		if op.Op != EmptyCmd {
			// not empty command
			updateLatest := false
			val, _ := kv.clientLastestRequestMap[op.CliNo]
			if op.Seq == val.Seq {
				// has processed
				if cliCmd != nil && cliCmd.matchMsg(msg) {
					if op.Op == GetCmd {
						cliCmd.Rsp.(*GetReply).Value = kv.kvStore[op.Key]
					}
					cliCmd.Done <- struct{}{}
					delete(kv.waitApplyMsgCommandMap, cliKey)
				}
			} else if op.Seq == val.Seq+1 {
				updateLatest = true
				switch op.Op {
				case GetCmd:
					if cliCmd != nil && cliCmd.matchMsg(msg) {
						cliCmd.Rsp.(*GetReply).Value = kv.kvStore[op.Key]
					}
					Debugf(dCommand, "Server%d doApply get key %s val %s",
						kv.me, op.Key, kv.kvStore[op.Key])
				case PutCmd:
					kv.kvStore[op.Key] = op.Val
					Debugf(dCommand, "Server%d doApply put key %s val %s store %s",
						kv.me, op.Key, op.Val, kv.kvStore[op.Key])
				case AppendCmd:
					kv.kvStore[op.Key] += op.Val
					Debugf(dCommand, "Server%d doApply append key %s val %s store %s",
						kv.me, op.Key, op.Val, kv.kvStore[op.Key])
				case EmptyCmd:
					// do nothing
				}
				if cliCmd != nil && cliCmd.matchMsg(msg) {
					cliCmd.Done <- struct{}{}
					delete(kv.waitApplyMsgCommandMap, cliKey)
				}
				Debugf(dCommand, "Server%d apply op %s key %s val %s",
					kv.me, op.Op, op.Key, op.Val)
			} else if op.Seq < val.Seq {
				if cliCmd != nil && cliCmd.matchMsg(msg) {
					kv.fastFail(cliCmd, fmt.Errorf("Newer apply"))
					delete(kv.waitApplyMsgCommandMap, cliKey)
				}
			} else {
				Debugf(dCommand, "Server%d discard op %s key %s val %s",
					kv.me, op.Op, op.Key, op.Val)
			}
			if updateLatest {
				// update client's latest request number
				kv.clientLastestRequestMap[op.CliNo] = ClientLatestResponse{
					Seq: op.Seq,
				}
			}
		}

		// clear some request if not EmptyCmd
		needDelKey := make([]string, 0)
		for k, cliCmd := range kv.waitApplyMsgCommandMap {
			if cliCmd == nil {
				needDelKey = append(needDelKey, k)
			} else if cliCmd.Index <= msg.CommandIndex || cliCmd.Term < msg.CommandTerm {
				Debugf(dInfo, "Server%d clear older key %s", kv.me, k)
				needDelKey = append(needDelKey, k)
			}
		}
		for _, k := range needDelKey {
			Debugf(dClear, "Server%d clear key %s", kv.me, k)
			cliCmd := kv.waitApplyMsgCommandMap[k]
			delete(kv.waitApplyMsgCommandMap, k)
			if cliCmd != nil {
				kv.fastFail(cliCmd, fmt.Errorf("Apply newer index/term"))
			}
		}
	}
}

// processClientCommand
func (kv *KVServer) processClientCommand() {
	for cliCmd := range kv.clientCommandChannel {
		// process commands from client
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			if kv.killed() {
				kv.fastFail(cliCmd, fmt.Errorf("Server%d Killed", kv.me))
				return
			}
			op := Op{}
			kv.fillOp(&op, cliCmd)
			idx, term, isLeader := kv.rf.Start(op)
			if isLeader {
				// add to waitApplyMsgCommandMap
				cliCmd.Index, cliCmd.Term = idx, term
				err := kv.addToWaitApplyMsgCommandMap(kv.encodeKey(cliCmd.Seq, cliCmd.CliNo), cliCmd)
				if err != nil {
					// fast fail
					kv.fastFail(cliCmd, err)
				}
			} else {
				// not leader fast fail
				Debugf(dError, "Server%d not leader", kv.me)
				kv.fastFail(cliCmd, fmt.Errorf("Server%d is not leader", kv.me))
			}
		}()
	}
}

// readApplyMsg read apply msg from applyCh
func (kv *KVServer) readApplyMsg() {
	tiker := time.NewTicker(time.Duration(500) * time.Millisecond)
	defer tiker.Stop()
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			kv.doApply(&msg)
		case <-tiker.C:
		}
	}
}

// startEmptyOp send an empty message to leader, so leader could commit command message
func (kv *KVServer) startEmptyOp() {
	tiker := time.NewTicker(time.Duration(500) * time.Millisecond)
	defer tiker.Stop()
	for !kv.killed() {
		select {
		case <-tiker.C:
			kv.mu.Lock()
			// if server is leader, send an empty op
			if _, isLeader := kv.rf.GetState(); isLeader && !kv.killed() {
				kv.rf.Start(Op{
					Op: EmptyCmd,
				})
			}
			kv.mu.Unlock()
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	Debugf(dInfo, "Server%d StartKVServer start", me)
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.clientLastestRequestMap = make(map[int64]ClientLatestResponse)
	kv.kvStore = make(map[string]string)
	kv.clientCommandChannel = make(chan *ClientCommand, 1000)
	kv.waitApplyMsgCommandMap = make(map[string]*ClientCommand)
	go kv.processClientCommand()
	go kv.readApplyMsg()
	go kv.startEmptyOp()
	return kv
}
