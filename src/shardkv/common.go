package shardkv

import "6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	EmptyCmd    = "Empty"
	GetCmd      = "Get"
	PutCmd      = "Put"
	AppendCmd   = "Append"
	ReConfigCmd = "ReConfig"
	MigrateCmd  = "Migrate"
)

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrNotReady     = "ErrNotReady"
	ErrServerKilled = "ErrServerKilled"
	ErrNewerLog     = "ErrNewerLog"
	ErrNewerRequest = "ErrNewerRequest"
)

type Err string

type ClientInfo struct {
	Seq      int
	ClientNo int64
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	ClientInfo
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

func (p *PutAppendReply) OK() bool {
	return p.Err == "" || p.Err == OK
}

type GetArgs struct {
	ClientInfo
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

func (g *GetReply) OK() bool {
	return g.Err == "" || g.Err == OK
}

// ReConfigArgs request for re-configuration
type ReConfigArgs struct {
	ClientInfo
	OldConfig shardctrler.Config
	NewConfig shardctrler.Config
}

// ReConfigReply
type ReConfigReply struct {
	Err Err
}

func (r *ReConfigReply) OK() bool {
	return r.Err == "" || r.Err == OK
}

// MigrateArgs request for migrate shard
type MigrateArgs struct {
	ClientInfo
	Gid    int                       //dst gid
	Shards map[int]map[string]string // migrate shards
}

// MigrateReply
type MigrateReply struct {
	Err Err
}

func (m *MigrateReply) OK() bool {
	return m.Err == "" || m.Err == OK
}
