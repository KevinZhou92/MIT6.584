package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store map[string]string
	// The clientSeqs and history data structure only works if the client is calling server serially(1 by 1) so we can guarantee that
	// history will always store the value for the previous reply, (each request will have an unique sequence number).
	// If there are multiple calls concurrently then they will share the same sequence number, then we can't simply store a client id -> seq mapping
	history map[int64]*Reply
}

type Reply struct {
	Value string
	Seq   int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	if value, present := kv.store[key]; present {
		// fmt.Printf("=[Get] Found value %s for key %s\n", value, key)
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key, value, newSeq, clientId := args.Key, args.Value, args.Seq, args.ClientId
	lastReply, present := kv.history[clientId]
	// log.Printf("=[Put]: client id %d, curSeq %d, newSeq %d, present %t", clientId, curSeq, newSeq, present)
	if present && lastReply.Seq >= newSeq {
		return
	}

	kv.store[key] = value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key, value, newSeq, clientId := args.Key, args.Value, args.Seq, args.ClientId
	lastReply, present := kv.history[clientId]

	existingValue := kv.store[key]
	// fmt.Printf("=[Append] Found existing value %s for key %s\n", existingValue, key)
	if present && lastReply.Seq >= newSeq {
		reply.Value = kv.history[clientId].Value
		return
	}

	newValue := existingValue + value
	kv.store[key] = newValue

	reply.Value = existingValue
	kv.history[clientId] = &Reply{
		Value: existingValue,
		Seq:   newSeq,
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.history = make(map[int64]*Reply)

	return kv
}
