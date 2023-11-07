package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Name     string
	ClientId int64
	SeqId    int
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	lastApplied int
	chans       map[int]chan Op
	client2seq  map[int64]int
	keyvalue    map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	oldOp := Op{"GET", args.ClientId, args.SeqId, args.Key, ""}
	index, _, isLeader := kv.rf.Start(oldOp)

	if !isLeader || kv.killed() {
		return
	}

	// fmt.Println("SERVER GET: ", args.Key)
	ch := kv.getCh(index)

	select {
	case newOp := <-ch:
		if newOp.ClientId != oldOp.ClientId || newOp.SeqId != oldOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.keyvalue[args.Key]
			kv.mu.Unlock()
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.chans, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	// time.Sleep(1 * time.Second)
	oldOp := Op{args.Op, args.ClientId, args.SeqId, args.Key, args.Value}
	index, _, isLeader := kv.rf.Start(oldOp)

	// fmt.Println("SERVER: ", args.Op, args.Key, args.Value, isLeader)
	if !isLeader || kv.killed() {
		return
	}

	// fmt.Println("SERVER: ", args.Op, args.Key, args.Value)
	ch := kv.getCh(index)

	select {
	case newOp := <-ch:
		if newOp.ClientId != oldOp.ClientId || newOp.SeqId != oldOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.chans, index)
	kv.mu.Unlock()
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
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.chans = make(map[int]chan Op)
	kv.client2seq = make(map[int64]int)
	kv.keyvalue = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	snapshot := persister.ReadSnapshot()
	kv.decodeSnapshot(snapshot)

	go kv.getLog()

	return kv
}

func (kv *KVServer) decodeSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var keyvalue map[string]string
	//var Index2Cmd map[int] chan Op
	var client2seq map[int64]int

	if d.Decode(&keyvalue) != nil ||
		d.Decode(&client2seq) != nil {
	} else {
		kv.keyvalue = keyvalue
		kv.client2seq = client2seq
	}

}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.keyvalue)
	e.Encode(kv.client2seq)
	snapshot := w.Bytes()
	return snapshot
}

func (kv *KVServer) getLog() {
	for !kv.killed() {
		entry := <-kv.applyCh
		// fmt.Println("APPLYCH: ", entry.CommandIndex)

		if entry.CommandValid {

			op := entry.Command.(Op)
			if !kv.exist(op.ClientId, op.SeqId) {
				kv.mu.Lock()
				if op.Name == "Put" {
					kv.keyvalue[op.Key] = op.Value
				} else if op.Name == "Append" {
					kv.keyvalue[op.Key] += op.Value
				}
				kv.client2seq[op.ClientId] = op.SeqId
				kv.mu.Unlock()
			}

			if kv.maxraftstate != -1 && kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
				kv.rf.Snapshot(entry.CommandIndex, kv.makeSnapshot())
			}

			kv.getCh(entry.CommandIndex) <- op
		} else if entry.SnapshotValid {
			kv.mu.Lock()
			if entry.SnapshotIndex > kv.lastApplied {
				kv.decodeSnapshot(entry.Snapshot)
				kv.lastApplied = entry.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) getCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, ok := kv.chans[index]

	if !ok {
		kv.chans[index] = make(chan Op, 1)
		ch = kv.chans[index]
	}

	return ch
}

func (kv *KVServer) exist(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	id, ok := kv.client2seq[clientId]

	if !ok {
		return false
	}

	return seqId <= id
}
