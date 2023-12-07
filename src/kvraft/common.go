package kvraft

const (
	EmptyCmd  = "Empty"
	GetCmd    = "Get"
	PutCmd    = "Put"
	AppendCmd = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Op    string // "Put" or "Append"
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq   int
	CliNo int64
}

type PutAppendReply struct {
	Err Err
}

func (r *PutAppendReply) OK() bool {
	return r.Err == ""
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq   int
	CliNo int64
}

type GetReply struct {
	Err   Err
	Value string
}

func (r *GetReply) OK() bool {
	return r.Err == ""
}
