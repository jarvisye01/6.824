package shardctrler

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// Your data here.
	Op     string
	Params interface{} // user command params
	Seq    int
	CliNo  int64
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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.
	lastApplyIndex          int // Raft last apply log index
	clientLatestResponseMap map[int64]ClientLatestResponse
	configs                 []Config // indexed by config num
	clientCommandChannel    chan *ClientCommand
	waitApplyMsgCommandMap  map[string]*ClientCommand
}

func (sc *ShardCtrler) processReq(
	cmd string, cliNo int64, seq int,
	req interface{}, rsp interface{},
) *ClientCommand {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	cliCmd := &ClientCommand{
		CliCmd: cmd,
		CliNo:  cliNo,
		Seq:    seq,
		Req:    req,
		Rsp:    rsp,
		Done:   make(chan struct{}),
	}
	sc.clientCommandChannel <- cliCmd
	return cliCmd
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	Debugf(dServer, "Controller%d receive Join command client %d req %+v",
		sc.me, args.ClientNo, *args)
	cliCmd := sc.processReq(JoinCmd, args.ClientNo, args.Seq, args, reply)
	if cliCmd != nil {
		<-cliCmd.Done
	}
	Debugf(dServer, "Controller%d reply Join command %+v repsonse %+v",
		sc.me, *args, *reply)
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	Debugf(dServer, "Controller%d receive Leave command client %d req %+v",
		sc.me, args.ClientNo, *args)
	cliCmd := sc.processReq(LeaveCmd, args.ClientNo, args.Seq, args, reply)
	if cliCmd != nil {
		<-cliCmd.Done
	}
	Debugf(dServer, "Controller%d reply Leave command %+v repsonse %+v",
		sc.me, *args, *reply)
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	Debugf(dServer, "Controller%d receive Move command client %d req %+v",
		sc.me, args.ClientNo, *args)
	cliCmd := sc.processReq(MoveCmd, args.ClientNo, args.Seq, args, reply)
	if cliCmd != nil {
		<-cliCmd.Done
	}
	Debugf(dServer, "Controller%d reply Move command %+v repsonse %+v",
		sc.me, *args, *reply)
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	Debugf(dServer, "Controller%d receive Query command client %d req %+v",
		sc.me, args.ClientNo, *args)
	cliCmd := sc.processReq(QueryCmd, args.ClientNo, args.Seq, args, reply)
	if cliCmd != nil {
		<-cliCmd.Done
	}
	Debugf(dServer, "Controller%d reply Query command %+v repsonse %+v",
		sc.me, *args, *reply)
	return
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// encodeKey encode seq and cliNo to a key
func (sc *ShardCtrler) encodeKey(seq int, cliNo int64) string {
	return fmt.Sprintf("%d_%d", seq, cliNo)
}

// addToWaitApplyMsgCommandMap add client request command to sc.waitApplyMsgCommandMap
// After invoke Raft.Start successfully
func (sc *ShardCtrler) addToWaitApplyMsgCommandMap(key string, cliCmd *ClientCommand) error {
	if _, ok := sc.waitApplyMsgCommandMap[key]; ok {
		return errors.New(RequestExistError)
	}
	sc.waitApplyMsgCommandMap[key] = cliCmd
	return nil
}

// fastFail
func (sc *ShardCtrler) fastFail(cliCmd *ClientCommand, err error) {
	switch cliCmd.CliCmd {
	case JoinCmd:
		rsp := cliCmd.Rsp.(*JoinReply)
		rsp.Err = Err(err.Error())
	case LeaveCmd:
		rsp := cliCmd.Rsp.(*LeaveReply)
		rsp.Err = Err(err.Error())
	case MoveCmd:
		rsp := cliCmd.Rsp.(*MoveReply)
		rsp.Err = Err(err.Error())
	case QueryCmd:
		rsp := cliCmd.Rsp.(*QueryReply)
		rsp.Err = Err(err.Error())
	}
	cliCmd.Done <- struct{}{}
}

// fillOp fill option
func (sc *ShardCtrler) fillOp(o *Op, cliCmd *ClientCommand) {
	o.Op = cliCmd.CliCmd
	o.CliNo, o.Seq = cliCmd.CliNo, cliCmd.Seq
	switch cliCmd.CliCmd {
	case JoinCmd:
		o.Params = *cliCmd.Req.(*JoinArgs)
	case LeaveCmd:
		o.Params = *cliCmd.Req.(*LeaveArgs)
	case MoveCmd:
		o.Params = *cliCmd.Req.(*MoveArgs)
	case QueryCmd:
		o.Params = *cliCmd.Req.(*QueryArgs)
	}
}

// processClientCommand
func (sc *ShardCtrler) processClientCommand() {
	for cliCmd := range sc.clientCommandChannel {
		// process commands from client
		func() {
			sc.mu.Lock()
			defer sc.mu.Unlock()
			op := Op{}
			sc.fillOp(&op, cliCmd)
			idx, term, isLeader := sc.rf.Start(op)
			if isLeader {
				// add to waitApplyMsgCommandMap
				cliCmd.Index, cliCmd.Term = idx, term
				err := sc.addToWaitApplyMsgCommandMap(sc.encodeKey(cliCmd.Seq, cliCmd.CliNo), cliCmd)
				if err != nil {
					sc.fastFail(cliCmd, err)
				}
			} else {
				// not leader fast fail
				Debugf(dError, "Controller%d not leader", sc.me)
				sc.fastFail(cliCmd, errors.New(NotLeaderError))
			}
		}()
	}
}

// lastConfigIndex
func (sc *ShardCtrler) lastConfigIndex() int {
	return len(sc.configs) - 1
}

// getBestGid
func (sc *ShardCtrler) getBestGid(cfg *Config) int {
	gid := 0
	gidShardMap := make(map[int][]int)
	for gid := range cfg.Groups {
		gidShardMap[gid] = make([]int, 0)
	}
	for shard, gid := range cfg.Shards {
		if gid == 0 {
			continue
		}
		gidShardMap[gid] = append(gidShardMap[gid], shard)
	}
	min := math.MaxInt
	for g, shards := range gidShardMap {
		if len(shards) < min {
			gid = g
		}
	}
	return gid
}

// rebalance cfg => old config, cfg => new config
func (sc *ShardCtrler) rebalance(cfg1 *Config, cfg2 *Config) {
	if len(cfg2.Groups) == 0 {
		cfg2.Shards = [NShards]int{}
		return
	}
	m, n := NShards/len(cfg2.Groups), NShards%len(cfg2.Groups)
	noAllocateShards := make([]int, 0)
	allGids := make([]int, 0)
	gidShardMap := make(map[int][]int)
	maxShardGidCount := 0
	for gid := range cfg2.Groups {
		gidShardMap[gid] = make([]int, 0)
		allGids = append(allGids, gid)
	}
	sort.Sort(sort.IntSlice(allGids))
	for shard, gid := range cfg1.Shards {
		if gid == 0 {
			noAllocateShards = append(noAllocateShards, shard)
			continue
		}
		if _, ok := cfg2.Groups[gid]; !ok {
			noAllocateShards = append(noAllocateShards, shard)
			continue
		}
		if (maxShardGidCount < n && len(gidShardMap[gid]) < m+1) ||
			(maxShardGidCount == n && len(gidShardMap[gid]) < m) {
			gidShardMap[gid] = append(gidShardMap[gid], shard)
			if len(gidShardMap[gid]) == m+1 {
				maxShardGidCount++
			}
		} else {
			noAllocateShards = append(noAllocateShards, shard)
		}
	}
	for _, shard := range noAllocateShards {
		for _, sortGid := range allGids {
			gid := sortGid
			shards := gidShardMap[sortGid]
			if (maxShardGidCount < n && len(shards) < m+1) ||
				(maxShardGidCount == n && len(shards) < m) {
				gidShardMap[gid] = append(gidShardMap[gid], shard)
				if len(gidShardMap[gid]) == m+1 {
					maxShardGidCount++
				}
				break
			}
		}
	}
	for gid, shards := range gidShardMap {
		for _, s := range shards {
			cfg2.Shards[s] = gid
		}
	}
}

// lastConfig
func (sc *ShardCtrler) lastConfig() *Config {
	return &sc.configs[sc.lastConfigIndex()]
}

// generateConfig
func (sc *ShardCtrler) generateConfig(idx int, cfg *Config) *Config {
	newConfig := &Config{
		Num:    idx,
		Shards: cfg.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range cfg.Groups {
		newConfig.Groups[gid] = make([]string, 0)
		for _, s := range servers {
			newConfig.Groups[gid] = append(newConfig.Groups[gid], s)
		}
	}
	return newConfig
}

// processApplyMsg
func (sc *ShardCtrler) processApplyMsg(op *Op, cliCmd *ClientCommand, msg *raft.ApplyMsg) {
	Debugf(dInfo, "Controller%d process apply msg op %+v cliCmd %v msg %+v",
		sc.me, *op, cliCmd, *msg)
	switch op.Op {
	case JoinCmd:
		req := op.Params.(JoinArgs)
		lastConfig := sc.lastConfig()
		newConfig := sc.generateConfig(lastConfig.Num+1, lastConfig)
		for gid, servers := range req.Servers {
			if _, ok := newConfig.Groups[gid]; !ok {
				newConfig.Groups[gid] = servers
			}
		}
		sc.rebalance(&sc.configs[sc.lastConfigIndex()], newConfig)
		sc.configs = append(sc.configs, *newConfig)
	case LeaveCmd:
		req := op.Params.(LeaveArgs)
		lastConfig := sc.lastConfig()
		newConfig := sc.generateConfig(lastConfig.Num+1, lastConfig)
		for _, gid := range req.GIDs {
			delete(newConfig.Groups, gid)
		}
		sc.rebalance(lastConfig, newConfig)
		sc.configs = append(sc.configs, *newConfig)
	case MoveCmd:
		req := op.Params.(MoveArgs)
		lastConfig := sc.lastConfig()
		newConfig := sc.generateConfig(lastConfig.Num+1, lastConfig)
		newConfig.Shards[req.Shard] = req.GID
		sc.rebalance(lastConfig, newConfig)
		sc.configs = append(sc.configs, *newConfig)
	case QueryCmd:
		req := op.Params.(QueryArgs)
		if cliCmd != nil {
			rsp := cliCmd.Rsp.(*QueryReply)
			if req.Num < 0 {
				rsp.Config = *sc.lastConfig()
			} else if req.Num < len(sc.configs) {
				rsp.Config = sc.configs[req.Num]
			} else {
				rsp.Err = ConfigNumError
			}
		}
	case EmptyCmd:
	}
	if cliCmd != nil && cliCmd.matchMsg(msg) {
		cliCmd.Done <- struct{}{}
		delete(sc.waitApplyMsgCommandMap, sc.encodeKey(op.Seq, op.CliNo))
	}
}

// doApply process msg from Raft
func (sc *ShardCtrler) doApply(msg *raft.ApplyMsg) {
	Debugf(dInfo, "Controller%d doApply msg %+v", sc.me, *msg)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if msg.CommandValid {
		op := msg.Command.(Op)
		cliKey := sc.encodeKey(op.Seq, op.CliNo)
		cliCmd := sc.waitApplyMsgCommandMap[cliKey]
		if op.Op != EmptyCmd {
			// process command
			updateLatest := false
			val, _ := sc.clientLatestResponseMap[op.CliNo]
			Debugf(dInfo, "Controller%d client latest repsonse %+v req map %+v",
				sc.me, val, sc.waitApplyMsgCommandMap)
			if op.Seq == val.Seq {
				if cliCmd != nil && cliCmd.matchMsg(msg) {
					// has processed
					if op.Op == QueryCmd {
						req := cliCmd.Req.(*QueryArgs)
						rsp := cliCmd.Rsp.(*QueryReply)
						if req.Num == -1 {
							rsp.Config = *sc.lastConfig()
						} else if req.Num < len(sc.configs) {
							rsp.Config = sc.configs[req.Num]
						} else {
							rsp.Err = ConfigNumError
						}
					}
					cliCmd.Done <- struct{}{}
					delete(sc.waitApplyMsgCommandMap, cliKey)
				}
			} else if op.Seq == val.Seq+1 {
				updateLatest = true
				sc.processApplyMsg(&op, cliCmd, msg)
			} else {
				Debugf(dServer, "Controller%d discard op %s", sc.me, op.Op)
			}
			if updateLatest {
				// update client's latest request number
				sc.clientLatestResponseMap[op.CliNo] = ClientLatestResponse{
					Seq: op.Seq,
				}
			}
		}
		// clear some request
		needDelKey := make([]string, 0)
		for k, cliCmd := range sc.waitApplyMsgCommandMap {
			if cliCmd == nil {
				needDelKey = append(needDelKey, k)
			} else if cliCmd.Index <= msg.CommandIndex || cliCmd.Term < msg.CommandTerm {
				Debugf(dInfo, "Controller%d clear older key %s", sc.me, k)
				needDelKey = append(needDelKey, k)
			}
		}
		for _, k := range needDelKey {
			Debugf(dInfo, "Controller%d clear key %s", sc.me, k)
			cliCmd := sc.waitApplyMsgCommandMap[k]
			delete(sc.waitApplyMsgCommandMap, k)
			if cliCmd != nil {
				sc.fastFail(cliCmd, errors.New(OldMsgError))
			}
		}
		sc.lastApplyIndex = msg.CommandIndex
	}
}

// readApplyMsg read apply msg from applyCh
func (sc *ShardCtrler) readApplyMsg() {
	ticker := time.NewTicker(time.Duration(500) * time.Millisecond)
	defer ticker.Stop()
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			sc.doApply(&msg)
		case <-ticker.C:
		}
	}
}

// startEmptyOp send an empty message to leader, so leader could commit command message
func (sc *ShardCtrler) startEmptyOp() {
	ticker := time.NewTicker(time.Duration(500) * time.Millisecond)
	defer ticker.Stop()
	for !sc.killed() {
		select {
		case <-ticker.C:
			sc.mu.Lock()
			// if server is leader, send an empty op
			if _, isLeader := sc.rf.GetState(); isLeader && !sc.killed() {
				sc.rf.Start(Op{
					Op: EmptyCmd,
				})
			}
			sc.mu.Unlock()
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	Debugf(dInfo, "Controller%d start server", me)
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplyIndex = 0
	sc.clientLatestResponseMap = make(map[int64]ClientLatestResponse)
	sc.clientCommandChannel = make(chan *ClientCommand, 1000)
	sc.waitApplyMsgCommandMap = make(map[string]*ClientCommand)
	go sc.processClientCommand()
	go sc.readApplyMsg()
	go sc.startEmptyOp()
	return sc
}
