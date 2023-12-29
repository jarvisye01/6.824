package shardkv

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

func getMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func sliceContainsInt(s []int, n int) bool {
	for _, si := range s {
		if si == n {
			return true
		}
	}
	return false
}

func deepCopyMap(m1 map[string]string, m2 map[string]string) {
	for k, v := range m1 {
		m2[k] = v
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op     string
	Params interface{} // option request
	Seq    int
	CliNo  int64
}

type ClientCommand struct {
	CliCmd string
	CliNo  int64
	Seq    int
	Req    interface{}   // client request
	Rsp    interface{}   // client response
	Index  int           // raft log index
	Term   int           // raft log term
	Done   chan struct{} // done channel, wait to response
}

// matchMsg
func (c *ClientCommand) matchMsg(m *raft.ApplyMsg) bool {
	return c.Index == m.CommandIndex && c.Term == m.CommandTerm
}

type ClientLatestResponse struct {
	Seq int
}

type RgClientManager struct {
	ClientSeqMap map[int]int // client => client seq
	RgClerkMap   map[int]*Clerk
}

func (r *RgClientManager) saveClientSeq() {
	for gid, clerk := range r.RgClerkMap {
		r.ClientSeqMap[gid] = clerk.GetCurSeq()
	}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	gid          int
	applyCh      chan raft.ApplyMsg
	rf           *raft.Raft
	make_end     func(string) *labrpc.ClientEnd
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int                 // snapshot if log grows this big
	dead         int32               // set by Kill()
	servers      []*labrpc.ClientEnd // replica group servers

	// Your definitions here.
	curConfig               shardctrler.Config // need persist in snaphsot
	readyShards             map[int]struct{}   // ready shard, need persist in snapshot
	controllerClerk         *shardctrler.Clerk
	rgClientManager         RgClientManager                // clientSeqMap need persist in snaphsot
	lastApplyIndex          int                            // last apply log index
	clientLatestResponseMap map[int64]ClientLatestResponse // latest client request info
	kvStore                 map[string]string              // store key and value
	clientCommandChannel    chan *ClientCommand            // client request channel
	waitApplyMsgCommandMap  map[string]*ClientCommand
}

// processReq
func (kv *ShardKV) processReq(commonReq interface{}, commonRsp interface{}) *ClientCommand {
	var cliCmd *ClientCommand
	killed := kv.killed()
	switch commonReq.(type) {
	case *GetArgs:
		req := commonReq.(*GetArgs)
		rsp := commonRsp.(*GetReply)
		if killed {
			rsp.Err = ErrServerKilled
			return nil
		}
		cliCmd = &ClientCommand{
			CliCmd: GetCmd,
			CliNo:  req.ClientNo,
			Seq:    req.Seq,
			Req:    req,
			Rsp:    rsp,
			Done:   make(chan struct{}),
		}
		kv.clientCommandChannel <- cliCmd
	case *PutAppendArgs:
		req := commonReq.(*PutAppendArgs)
		rsp := commonRsp.(*PutAppendReply)
		if killed {
			rsp.Err = ErrServerKilled
			return nil
		}
		cliCmd = &ClientCommand{
			CliCmd: req.Op,
			CliNo:  req.ClientNo,
			Seq:    req.Seq,
			Req:    req,
			Rsp:    rsp,
			Done:   make(chan struct{}),
		}
		kv.clientCommandChannel <- cliCmd
	case *MigrateArgs:
		req := commonReq.(*MigrateArgs)
		rsp := commonRsp.(*MigrateReply)
		if killed {
			rsp.Err = ErrServerKilled
			return nil
		}
		cliCmd = &ClientCommand{
			CliCmd: MigrateCmd,
			CliNo:  req.ClientNo,
			Seq:    req.Seq,
			Req:    req,
			Rsp:    rsp,
			Done:   make(chan struct{}),
		}
		kv.clientCommandChannel <- cliCmd
	case *ReConfigArgs:
		req := commonReq.(*ReConfigArgs)
		rsp := commonRsp.(*ReConfigReply)
		if killed {
			rsp.Err = ErrServerKilled
			return nil
		}
		cliCmd = &ClientCommand{
			CliCmd: ReConfigCmd,
			CliNo:  req.ClientNo,
			Seq:    req.Seq,
			Req:    req,
			Rsp:    rsp,
			Done:   make(chan struct{}),
		}
		kv.clientCommandChannel <- cliCmd
	}
	return cliCmd
}

// Get process get rpc
func (kv *ShardKV) Get(req *GetArgs, rsp *GetReply) {
	// Your code here.
	var cliCmd *ClientCommand
	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		Debugf(dServer, "Server_%d_%d receive Get command client %d seq %d key %s",
			kv.gid, kv.me, req.ClientNo, req.Seq, req.Key)
		cliCmd = kv.processReq(req, rsp)
	}()
	if cliCmd != nil {
		<-cliCmd.Done
	}
	Debugf(dServer, "Server_%d_%d reply Get command %v response %v",
		kv.gid, kv.me, *req, *rsp)
	return
}

// PutAppend process put/append rpc
func (kv *ShardKV) PutAppend(req *PutAppendArgs, rsp *PutAppendReply) {
	// Your code here.
	var cliCmd *ClientCommand
	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		Debugf(dServer, "Server_%d_%d receive %s command client %d seq %d key %s val %s",
			kv.gid, kv.me, req.Op, req.ClientNo, req.Seq, req.Key, req.Value)
		cliCmd = kv.processReq(req, rsp)
	}()
	if cliCmd != nil {
		<-cliCmd.Done
	}
	Debugf(dServer, "Server_%d_%d reply %s command response %v",
		kv.gid, kv.me, req.Op, *rsp)
	return
}

// ReConfig process migrate rpc
func (kv *ShardKV) ReConfig(req *ReConfigArgs, rsp *ReConfigReply) {
	var cliCmd *ClientCommand
	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		Debugf(dServer, "Server_%d_%d receive ReConfig command client %d seq %d",
			kv.gid, kv.me, req.ClientNo, req.Seq)
		cliCmd = kv.processReq(req, rsp)
	}()
	if cliCmd != nil {
		<-cliCmd.Done
	}
	return
}

// MigrateShards process migrate rpc
func (kv *ShardKV) MigrateShards(req *MigrateArgs, rsp *MigrateReply) {
	var cliCmd *ClientCommand
	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		Debugf(dServer, "Server_%d_%d receive Migrate command client %d seq %d req %+v",
			kv.gid, kv.me, req.ClientNo, req.Seq, req)
		cliCmd = kv.processReq(req, rsp)
	}()
	if cliCmd != nil {
		<-cliCmd.Done
	}
	return
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	Debugf(dServer, "Server_%d_%d Kill", kv.gid, kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.clientCommandChannel) // close clientCommandChannel
	for cliCmd := range kv.clientCommandChannel {
		if cliCmd != nil {
			kv.fastFail(cliCmd, errors.New(ErrServerKilled))
		}
	}
	for k, cliCmd := range kv.waitApplyMsgCommandMap {
		delete(kv.waitApplyMsgCommandMap, k)
		if cliCmd != nil {
			kv.fastFail(cliCmd, errors.New(ErrServerKilled))
		}
	}
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// getRgClerk get clerk to request to other replica groups
func (kv *ShardKV) getRgClerk(gid int) *Clerk {
	if _, ok := kv.rgClientManager.RgClerkMap[gid]; !ok {
		kv.rgClientManager.RgClerkMap[gid] = MakeClerk(kv.ctrlers, kv.make_end)
		kv.rgClientManager.RgClerkMap[gid].SetCliNo(int64(kv.gid))
		if _, ok := kv.rgClientManager.ClientSeqMap[gid]; !ok {
			kv.rgClientManager.ClientSeqMap[gid] = 0
		}
		kv.rgClientManager.RgClerkMap[gid].SetSeq(kv.rgClientManager.ClientSeqMap[gid])
	}
	return kv.rgClientManager.RgClerkMap[gid]
}

// endcodeKey encode seq and cliNo to a key
func (kv *ShardKV) encodeKey(seq int, cliNo int64) string {
	return fmt.Sprintf("%d_%d", seq, cliNo)
}

// fastFail fast fail and return to client
func (kv *ShardKV) fastFail(cliCmd *ClientCommand, err error) {
	switch cliCmd.CliCmd {
	case GetCmd:
		rsp := cliCmd.Rsp.(*GetReply)
		rsp.Err = Err(err.Error())
	case PutCmd:
		fallthrough
	case AppendCmd:
		rsp := cliCmd.Rsp.(*PutAppendReply)
		rsp.Err = Err(err.Error())
	case ReConfigCmd:
		rsp := cliCmd.Rsp.(*ReConfigReply)
		rsp.Err = Err(err.Error())
	case MigrateCmd:
		rsp := cliCmd.Rsp.(*MigrateReply)
		rsp.Err = Err(err.Error())
	}
	cliCmd.Done <- struct{}{}
}

// fillOp fill client option
func (kv *ShardKV) fillOp(o *Op, cliCmd *ClientCommand) {
	o.Op = cliCmd.CliCmd
	o.CliNo = cliCmd.CliNo
	o.Seq = cliCmd.Seq
	switch cliCmd.CliCmd {
	case GetCmd:
		req := cliCmd.Req.(*GetArgs)
		o.Params = *req
	case PutCmd:
		fallthrough
	case AppendCmd:
		req := cliCmd.Req.(*PutAppendArgs)
		o.Params = *req
	case ReConfigCmd:
		req := cliCmd.Req.(*ReConfigArgs)
		o.Params = *req
	case MigrateCmd:
		req := cliCmd.Req.(*MigrateArgs)
		o.Params = *req
	}
}

func (kv *ShardKV) addToWaitApplyMsgCommandMap(key string, cliCmd *ClientCommand) error {
	Debugf(dInfo, "Server%d add key %s cliCmd %v waitApplyMsgCommandMap %+v",
		kv.me, key, cliCmd, kv.waitApplyMsgCommandMap)
	_, ok := kv.waitApplyMsgCommandMap[key]
	if ok {
		Debugf(dError, "Server%d element exists key %s", kv.me, key)
		return fmt.Errorf("Element exists %s", key)
	}
	kv.waitApplyMsgCommandMap[key] = cliCmd
	return nil
}

// processClientCommand
func (kv *ShardKV) processClientCommand() {
	// process commands from client
	for cliCmd := range kv.clientCommandChannel {
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			if kv.killed() {
				kv.fastFail(cliCmd, errors.New(ErrServerKilled))
				return
			}
			op := Op{}
			kv.fillOp(&op, cliCmd)
			idx, term, isLeader := kv.rf.Start(op)
			if isLeader {
				// add to waitApplyMsgCommandMap
				cliCmd.Index, cliCmd.Term = idx, term
				err := kv.addToWaitApplyMsgCommandMap(
					kv.encodeKey(cliCmd.Seq, cliCmd.CliNo), cliCmd,
				)
				if err != nil {
					// fast fail
					kv.fastFail(cliCmd, err)
				}
			} else {
				// not leader, fast fail
				Debugf(dError, "Server_%d_%d not leader", kv.gid, kv.me)
				kv.fastFail(cliCmd, errors.New(ErrWrongLeader))
			}
		}()
	}
}

// keyMatchShard
func (kv *ShardKV) keyMatchShard(cfg *shardctrler.Config, key string) bool {
	return kv.gid == cfg.Shards[key2shard(key)]
}

// gidMatchShard
func (kv *ShardKV) gidMatchShard(cfg *shardctrler.Config, shard, gid int) bool {
	return gid == cfg.Shards[shard]
}

func (kv *ShardKV) matchShard(cfg *shardctrler.Config, req interface{}) bool {
	switch req.(type) {
	case GetArgs:
		r := req.(GetArgs)
		return kv.keyMatchShard(cfg, r.Key)
	case PutAppendArgs:
		r := req.(PutAppendArgs)
		return kv.keyMatchShard(cfg, r.Key)
	case ReConfigArgs:
		return true
	case MigrateArgs:
		return true
	}
	return false
}

// processGetCommand
func (kv *ShardKV) processGetCommand(op *Op, cliCmd *ClientCommand, msg *raft.ApplyMsg,
	curConfig *shardctrler.Config, updateLatest *bool) {
	if !kv.matchShard(curConfig, op.Params) {
		if cliCmd != nil {
			kv.fastFail(cliCmd, errors.New(ErrWrongGroup))
		}
		return
	}
	req := op.Params.(GetArgs)
	if _, ok := kv.readyShards[key2shard(req.Key)]; !ok {
		if cliCmd != nil {
			kv.fastFail(cliCmd, errors.New(ErrNotReady))
		}
		return
	}
	val, _ := kv.clientLatestResponseMap[op.CliNo]
	v, _ := kv.kvStore[op.Params.(GetArgs).Key]
	Debugf(dInfo, "Server_%d_%d processGetCommand key %s client %d op.Seq %d val.Seq %d",
		kv.gid, kv.me, req.Key, op.CliNo, op.Seq, val.Seq)
	if op.Seq < val.Seq {
		if cliCmd != nil {
			kv.fastFail(cliCmd, errors.New(ErrNewerLog))
		}
		return
	}
	if cliCmd != nil {
		cliCmd.Rsp.(*GetReply).Err = OK
		cliCmd.Rsp.(*GetReply).Value = v
		cliCmd.Done <- struct{}{}
	}
	*updateLatest = true
	return
}

// processPutAppendCommand
func (kv *ShardKV) processPutAppendCommand(op *Op, cliCmd *ClientCommand,
	msg *raft.ApplyMsg, curConfig *shardctrler.Config, updateLatest *bool) {
	Debugf(dInfo, "Server_%d_%d process put/append command", kv.gid, kv.me)
	if !kv.matchShard(curConfig, op.Params) {
		if cliCmd != nil {
			kv.fastFail(cliCmd, errors.New(ErrWrongGroup))
		}
		return
	}
	req := op.Params.(PutAppendArgs)
	if _, ok := kv.readyShards[key2shard(req.Key)]; !ok {
		if cliCmd != nil {
			kv.fastFail(cliCmd, errors.New(ErrNotReady))
		}
		return
	}
	val, _ := kv.clientLatestResponseMap[op.CliNo]
	Debugf(dInfo, "Server_%d_%d process put/append command client %d op.Seq %d val.Seq %d",
		kv.gid, kv.me, op.CliNo, op.Seq, val.Seq)
	if op.Seq <= val.Seq {
		if cliCmd != nil {
			cliCmd.Rsp.(*PutAppendReply).Err = OK
			cliCmd.Done <- struct{}{}
		}
		return
	}
	switch op.Op {
	case PutCmd:
		kv.kvStore[req.Key] = req.Value
	case AppendCmd:
		kv.kvStore[req.Key] += req.Value
	}
	if cliCmd != nil {
		cliCmd.Rsp.(*PutAppendReply).Err = OK
		cliCmd.Done <- struct{}{}
	}
	*updateLatest = true
	return
}

// migrateShards
func (kv *ShardKV) migrateShards(cfg *shardctrler.Config, m map[int]map[string]string) {
	Debugf(dInfo, "Server_%d_%d migrate shards m %v",
		kv.gid, kv.me, m)
	wg := sync.WaitGroup{}
	shards := make([]int, 0)
	for shard := range m {
		shards = append(shards, shard)
	}
	sort.Sort(sort.IntSlice(shards))
	for _, shard := range shards {
		m := m[shard]
		wg.Add(1)
		go func(shard int, m map[string]string) {
			defer wg.Done()
			var ck *Clerk
			for {
				kv.mu.Lock()
				curConfig := kv.curConfig
				gid := curConfig.Shards[shard]
				kv.mu.Unlock()
				if _, ok := curConfig.Groups[gid]; ok {
					if ck == nil {
						kv.mu.Lock()
						ck = kv.getRgClerk(gid)
						kv.mu.Unlock()
					}
					ck.Migrate(shard, m)
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(shard, m)
	}
	wg.Wait()
}

// processReConfigCommand
func (kv *ShardKV) processReConfigCommand(op *Op, cliCmd *ClientCommand,
	msg *raft.ApplyMsg, curConfig *shardctrler.Config, updateLatest *bool) {
	val, _ := kv.clientLatestResponseMap[op.CliNo]
	Debugf(dInfo, "Server_%d_%d process reconfig command client %d op.Seq %d val.Seq %d",
		kv.gid, kv.me, op.CliNo, op.Seq, val.Seq)
	if op.Seq <= val.Seq {
		if cliCmd != nil {
			cliCmd.Done <- struct{}{}
			cliCmd.Rsp.(*ReConfigReply).Err = OK
		}
		return
	}
	r := op.Params.(ReConfigArgs)
	getShards := func(cfg *shardctrler.Config, selfGid int) []int {
		shards := make([]int, 0)
		for shard, gid := range cfg.Shards {
			if gid == selfGid {
				shards = append(shards, shard)
			}
		}
		return shards
	}
	oldShards := getShards(&r.OldConfig, kv.gid)
	newShards := getShards(&r.NewConfig, kv.gid)
	for _, shard := range newShards {
		// new shard
		if r.OldConfig.Shards[shard] == 0 {
			kv.readyShards[shard] = struct{}{}
		}
	}
	Debugf(dInfo, "Server_%d_%d process reconfig command oldShards %v newShards %v",
		kv.gid, kv.me, oldShards, newShards)
	migrateShardKeyValueMap := make(map[int]map[string]string)
	needClearKeys := make([]string, 0)
	for _, shard := range oldShards {
		_, ok := kv.readyShards[shard]
		// migrate to other replica group
		if !sliceContainsInt(newShards, shard) && ok {
			if _, ok := migrateShardKeyValueMap[shard]; !ok {
				migrateShardKeyValueMap[shard] = make(map[string]string, 0)
			}
			m := migrateShardKeyValueMap[shard]
			for k, v := range kv.kvStore {
				if key2shard(k) == shard {
					needClearKeys = append(needClearKeys, k)
					m[k] = v
				}
			}
			// migrate shard to another replica group, then delete it in readyShards
			delete(kv.readyShards, shard)
		}
	}
	// migrate
	go kv.migrateShards(&r.NewConfig, migrateShardKeyValueMap)
	// modify curConfig
	kv.curConfig = r.NewConfig
	Debugf(dInfo, "Server_%d_%d update config %+v", kv.gid, kv.me, kv.curConfig)
	if cliCmd != nil {
		cliCmd.Rsp.(*ReConfigReply).Err = OK
		cliCmd.Done <- struct{}{}
	}
	*updateLatest = true
	return
}

// processMigrateCommand
func (kv *ShardKV) processMigrateCommand(op *Op, cliCmd *ClientCommand,
	msg *raft.ApplyMsg, curConfig *shardctrler.Config, updateLatest *bool) {
	val, _ := kv.clientLatestResponseMap[op.CliNo]
	Debugf(dInfo, "Server_%d_%d process migrate command client %d op.Seq %d val.Seq %d",
		kv.gid, kv.me, op.CliNo, op.Seq, val.Seq)
	r := op.Params.(MigrateArgs)
	shard := r.Shard
	if curConfig.Shards[shard] != kv.gid {
		if cliCmd != nil {
			kv.fastFail(cliCmd, errors.New(ErrWrongGroup))
		}
		return
	}
	if op.Seq <= val.Seq {
		if cliCmd != nil {
			cliCmd.Done <- struct{}{}
			cliCmd.Rsp.(*MigrateReply).Err = OK
		}
		return
	}
	if _, ok := kv.readyShards[shard]; !ok {
		// accept shard, then add it to readyShards
		for k, v := range r.M {
			kv.kvStore[k] = v
		}
		kv.readyShards[shard] = struct{}{}
	}
	if cliCmd != nil {
		cliCmd.Rsp.(*MigrateReply).Err = OK
		cliCmd.Done <- struct{}{}
	}
	*updateLatest = true
	return
}

// processApplyMsg
func (kv *ShardKV) processApplyMsg(op *Op, cliCmd *ClientCommand, msg *raft.ApplyMsg) {
	Debugf(dInfo, "Server_%d_%d process apply msg op %+v cliCmd %v msg %+v",
		kv.gid, kv.me, *op, cliCmd, *msg)
	config := kv.curConfig
	cliKey := kv.encodeKey(op.Seq, op.CliNo)
	updateLatest := false
	switch op.Op {
	case GetCmd:
		kv.processGetCommand(op, cliCmd, msg, &config, &updateLatest)
	case PutCmd:
		kv.processPutAppendCommand(op, cliCmd, msg, &config, &updateLatest)
	case AppendCmd:
		kv.processPutAppendCommand(op, cliCmd, msg, &config, &updateLatest)
	case ReConfigCmd:
		kv.processReConfigCommand(op, cliCmd, msg, &config, &updateLatest)
	case MigrateCmd:
		kv.processMigrateCommand(op, cliCmd, msg, &config, &updateLatest)
	}
	if cliCmd != nil && cliCmd.matchMsg(msg) {
		delete(kv.waitApplyMsgCommandMap, cliKey)
	}
	if updateLatest {
		// update client's latest request number
		Debugf(dInfo, "Server_%d_%d update client %d latest response info seq %d",
			kv.gid, kv.me, op.CliNo, op.Seq)
		kv.clientLatestResponseMap[op.CliNo] = ClientLatestResponse{
			Seq: op.Seq,
		}
	}
}

// scheduleMigrate
func (kv *ShardKV) scheduleMigrate() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	cfg := &kv.curConfig
	migrateShardKeyValueMap := make(map[int]map[string]string)
	needClearKeys := make([]string, 0)
	for k, v := range kv.kvStore {
		shard := key2shard(k)
		if _, ok := kv.readyShards[shard]; !ok {
			continue
		}
		if cfg.Shards[shard] != kv.gid {
			if _, ok := migrateShardKeyValueMap[shard]; !ok {
				migrateShardKeyValueMap[cfg.Shards[shard]] = make(map[string]string)
			}
			m1 := migrateShardKeyValueMap[shard]
			m1[k] = v
			needClearKeys = append(needClearKeys, k)
		}
	}
	for _, k := range needClearKeys {
		delete(kv.readyShards, key2shard(k))
	}
	go kv.migrateShards(cfg, migrateShardKeyValueMap)
	return
}

// doapply
func (kv *ShardKV) doApply(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	Debugf(dInfo, "Server_%d_%d doApply msg CommandValid %t SnapshotValid %t op %v",
		kv.gid, kv.me, msg.CommandValid, msg.SnapshotValid, msg.Command)
	if msg.CommandValid {
		op := msg.Command.(Op)
		cliKey := kv.encodeKey(op.Seq, op.CliNo)
		cliCmd := kv.waitApplyMsgCommandMap[cliKey]
		if op.Op != EmptyCmd {
			kv.processApplyMsg(&op, cliCmd, msg)
		}
		// clear some request if not EmptyCmd
		needDelKey := make([]string, 0)
		for k, cliCmd := range kv.waitApplyMsgCommandMap {
			if cliCmd == nil {
				needDelKey = append(needDelKey, k)
			} else if cliCmd.Index <= msg.CommandIndex || cliCmd.Term < msg.CommandTerm {
				needDelKey = append(needDelKey, k)
			}
		}
		for _, k := range needDelKey {
			Debugf(dInfo, "Server_%d_%d clear key %s",
				kv.gid, kv.me, k)
			cliCmd := kv.waitApplyMsgCommandMap[k]
			delete(kv.waitApplyMsgCommandMap, k)
			if cliCmd != nil {
				kv.fastFail(cliCmd, errors.New(ErrNewerLog))
			}
		}
		kv.lastApplyIndex = msg.CommandIndex
		kv.makeSnapshot(false)
	} else {
		Debugf(dServer, "Server_%d_%d recieve snapshot msg index %d term %d",
			kv.gid, kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
		if msg.Snapshot != nil {
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			kv.curConfig = shardctrler.Config{}
			kv.readyShards = make(map[int]struct{})
			kv.rgClientManager = RgClientManager{
				ClientSeqMap: make(map[int]int),
				RgClerkMap:   make(map[int]*Clerk),
			}
			kv.lastApplyIndex = 0
			kv.kvStore = make(map[string]string, 0)
			kv.clientLatestResponseMap = make(map[int64]ClientLatestResponse, 0)
			d.Decode(&kv.curConfig)
			d.Decode(&kv.readyShards)
			d.Decode(&kv.rgClientManager.ClientSeqMap)
			d.Decode(&kv.lastApplyIndex)
			d.Decode(&kv.kvStore)
			d.Decode(&kv.clientLatestResponseMap)
			// mabye need migrate
			go kv.scheduleMigrate()
		}
	}
}

// readApplyMsg
func (kv *ShardKV) readApplyMsg() {
	ticker := time.NewTicker(time.Duration(500) * time.Millisecond)
	defer ticker.Stop()
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			kv.doApply(&msg)
		case <-ticker.C:
		}
	}
}

// startEmptyOp
func (kv *ShardKV) startEmptyOp() {
	ticker := time.NewTicker(time.Duration(500) * time.Millisecond)
	defer ticker.Stop()
	for !kv.killed() {
		select {
		case <-ticker.C:
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

// makeSnapshot
func (kv *ShardKV) makeSnapshot(force bool) {
	if kv.maxraftstate == -1 {
		return
	}
	if force || kv.rf.GetRaftStateSize() > kv.maxraftstate {
		Debugf(dInfo, "Server_%d_%d makeSnapshot lastApplyIndex %d",
			kv.gid, kv.me, kv.lastApplyIndex)
		kv.rgClientManager.saveClientSeq()
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.curConfig)
		e.Encode(kv.readyShards)
		e.Encode(kv.rgClientManager.ClientSeqMap)
		e.Encode(kv.lastApplyIndex)
		e.Encode(kv.kvStore)
		e.Encode(kv.clientLatestResponseMap)
		snapshot := w.Bytes()
		kv.rf.Snapshot(kv.lastApplyIndex, snapshot)
	}
}

// snapshoter
func (kv *ShardKV) snapshoter() {
	ticker := time.NewTicker(time.Duration(1) * time.Second)
	defer ticker.Stop()
	for !kv.killed() {
		select {
		case <-ticker.C:
			kv.mu.Lock()
			kv.makeSnapshot(true)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) configPoller() {
	ticker := time.NewTicker(time.Duration(100) * time.Millisecond)
	defer ticker.Stop()
	for !kv.killed() {
		select {
		case <-ticker.C:
			func() {
				newConfig := kv.controllerClerk.Query(-1)
				Debugf(dInfo, "Server_%d_%d new config %+v", kv.gid, kv.me, newConfig)
				kv.mu.Lock()
				curConfig := kv.curConfig
				Debugf(dInfo, "Server_%d_%d cur config %+v", kv.gid, kv.me, curConfig)
				ck := kv.getRgClerk(kv.gid)
				kv.mu.Unlock()
				_, isLeader := kv.rf.GetState()
				if !isLeader {
					Debugf(dInfo, "Server_%d_%d not leader", kv.gid, kv.me)
				}
				if newConfig.Num > curConfig.Num && isLeader {
					// re configuration
					Debugf(dInfo, "Server_%d_%d new config %+v", kv.gid, kv.me, newConfig)
					ck.ReConfig(kv.gid, kv.servers, curConfig, newConfig)
				}
			}()
		}
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {

	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(ReConfigArgs{})
	labgob.Register(MigrateArgs{})

	kv := new(ShardKV)
	kv.mu = sync.Mutex{}
	kv.me = me
	kv.gid = gid
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.make_end = make_end
	kv.ctrlers = ctrlers
	kv.maxraftstate = maxraftstate
	kv.dead = 0
	kv.servers = servers

	kv.curConfig = shardctrler.Config{}
	kv.readyShards = make(map[int]struct{})
	kv.controllerClerk = shardctrler.MakeClerk(kv.ctrlers) // make shard controller to get config
	kv.rgClientManager = RgClientManager{
		ClientSeqMap: make(map[int]int),
		RgClerkMap:   make(map[int]*Clerk),
	}
	kv.lastApplyIndex = 0
	kv.clientLatestResponseMap = make(map[int64]ClientLatestResponse)
	kv.kvStore = make(map[string]string)
	kv.clientCommandChannel = make(chan *ClientCommand, 1000)
	kv.waitApplyMsgCommandMap = make(map[string]*ClientCommand)
	go kv.processClientCommand()
	go kv.readApplyMsg()
	go kv.startEmptyOp()
	go kv.snapshoter()
	go kv.configPoller()
	return kv
}
