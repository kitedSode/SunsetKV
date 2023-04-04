package common

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrExpired     = "ErrExpired"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	SeqId   int
	ClerkId int64
	Key     string
	Value   string
	Op      string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	SeqId   int
	Key     string
	ClerkId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type PingArgs struct {
	ClerkId int64
	Msg     string
}

type PingReply struct {
	Msg      string
	LeaderId int
}
