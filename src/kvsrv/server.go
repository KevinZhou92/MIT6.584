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
	store      map[string]string
	clientSeqs map[int64]int64
	history    map[int64]string
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
	curSeq, present := kv.clientSeqs[clientId]
	// log.Printf("=[Put]: client id %d, curSeq %d, newSeq %d, present %t", clientId, curSeq, newSeq, present)
	if present && curSeq >= newSeq {
		reply.Ack = curSeq
		return
	}

	kv.store[key] = value
	reply.Ack = newSeq
	kv.clientSeqs[clientId] = newSeq
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key, value, newSeq, clientId := args.Key, args.Value, args.Seq, args.ClientId
	curSeq, present := kv.clientSeqs[clientId]

	existingValue := kv.store[key]
	// fmt.Printf("=[Append] Found existing value %s for key %s\n", existingValue, key)
	if present && curSeq >= newSeq {
		reply.Value = kv.history[clientId]
		reply.Ack = curSeq
		return
	}

	newValue := existingValue + value
	kv.store[key] = newValue

	reply.Value = existingValue
	reply.Ack = newSeq
	kv.clientSeqs[clientId] = newSeq
	kv.history[clientId] = existingValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.clientSeqs = make(map[int64]int64)
	kv.history = make(map[int64]string)

	return kv
}
