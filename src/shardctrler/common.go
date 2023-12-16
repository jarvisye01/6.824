package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// shard controller options
const (
	JoinCmd  = "Join"
	QueryCmd = "Query"
	LeaveCmd = "Leave"
	MoveCmd  = "Move"
	EmptyCmd = "Empty"
)

// The number of shards.
const NShards = 10

const (
	OldMsgError       = "OldMsgError"
	OldRequestError   = "Old Request Error"
	NotLeaderError    = "Not Leader Error"
	RequestExistError = "Request Exists Error"
	ConfigNumError    = "Config Num Error"
)

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

// ClientInfo client info
type ClientInfo struct {
	Seq      int
	ClientNo int64
}

type JoinArgs struct {
	ClientInfo
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	ClientInfo
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	ClientInfo
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	ClientInfo
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
