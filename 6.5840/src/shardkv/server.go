package shardkv

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

// python dstest.py TestStaticShards TestJoinLeave TestSnapshot TestMissChange TestConcurrent1 TestConcurrent2 TestConcurrent3 TestUnreliable1 TestUnreliable2 TestUnreliable3 TestChallenge1Delete TestChallenge2Unaffected TestChallenge2Partial -p 30 -n 500

type Op struct {
	ShardValid bool
	Cmd        interface{}
}

type KVOp struct {
	Key      string
	Value    string
	Command  int
	Seq      int
	ClientId int64
}

type ShardOp struct {
	Command    int
	DB         map[string]string
	Client2Seq map[int64]int
	Servers    []string
	ShardId    int
	Num        int
	Config     shardctrler.Config
}

const (
	Not           = "Not"
	Pull          = "Pull"
	MigrateShard  = 1
	RemoveShard   = 2
	Finish        = 3
	Configuration = 4
	Clearshard    = 5
	PUT           = 6
	APPEND        = 7
	GET           = 8
	PULL          = 9
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()
	// Your definitions here.
	LastApplied int
	Client2Seq  map[int64]int
	chans       map[int]chan KVOp

	mck *shardctrler.Clerk
	// 新老配置, 用于比较是否迁移完了
	oldConfig    shardctrler.Config
	newConfig    shardctrler.Config
	ShardState   map[int]string                    // 记录 shard 的状态, 未被迁移还是正在被迁移
	ShardNum     map[int]int                       // 记录 shard 的版本号
	OutedData    map[int]map[int]map[string]string // num->shardId->data
	Shard2Client map[int][]int64                   // 记录 shard

	Pullchan map[int]chan PullReply
	K2V      [shardctrler.NShards]map[string]string
}

func (kv *ShardKV) Migrate(shardId int, shard map[string]string) Err {
	kv.K2V[shardId] = make(map[string]string)
	for k, v := range shard {
		kv.K2V[shardId][k] = v
	}
	return OK
}

func (kv *ShardKV) Copy(shardId int) map[string]string {
	res := make(map[string]string)
	for k, v := range kv.K2V[shardId] {
		res[k] = v
	}
	return res
}

func (kv *ShardKV) Remove(shardId int) Err {
	kv.K2V[shardId] = make(map[string]string)
	return OK
}

func (kv *ShardKV) Get(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	kv.Command(args, reply)
}

func (kv *ShardKV) PutAppend(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	kv.Command(args, reply)
}

func (kv *ShardKV) CheckGroup(shardId int) bool {
	return kv.newConfig.Shards[shardId] == kv.gid &&
		kv.ShardState[shardId] == OK
}

func (kv *ShardKV) Command(args *CommandArgs, reply *CommandReply) {

	kv.mu.Lock()

	if !kv.CheckGroup(key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	intcmd := 0
	switch args.Op {
	case Put:
		intcmd = PUT
	case Append:
		intcmd = APPEND
	case Get:
		intcmd = GET
	}
	op := KVOp{
		Key:      args.Key,
		Value:    args.Value,
		Command:  intcmd,
		Seq:      args.Seq,
		ClientId: args.ClientId,
	}
	index, _, isLeader := kv.rf.Start(Op{ShardValid: false, Cmd: op})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.GetChan(index)
	select {
	case app := <-ch:
		if app.ClientId != op.ClientId || app.Seq != op.Seq {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			if args.Op == "Get" {
				reply.Value = app.Value
			}
		}
	case <-time.After(time.Millisecond * 200):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.chans, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		ch := <-kv.applyCh
		if ch.CommandValid {
			kv.mu.Lock()
			if ch.Command == nil {
				continue
			}
			tmp := ch.Command.(Op)
			if tmp.ShardValid {
				op := tmp.Cmd.(ShardOp)
				switch op.Command {
				case MigrateShard:
					{

						// 如果对应 shard 的版本号小于当前的版本号 并且这个 shard 的状态是待迁移的
						if kv.ShardNum[op.ShardId] < op.Num && kv.ShardState[op.ShardId] != OK {
							kv.Migrate(op.ShardId, op.DB)
							delete(kv.Shard2Client, op.ShardId)
							for k, v := range op.Client2Seq {
								if kv.Client2Seq[k] < v {
									kv.Client2Seq[k] = v
								}

								if len(kv.Shard2Client[op.ShardId]) == 0 {
									kv.Shard2Client[op.ShardId] = append(kv.Shard2Client[op.ShardId], k)
								} else {
									flag := true
									for _, exitClient := range kv.Shard2Client[op.ShardId] {
										if exitClient == k {
											flag = false
											break
										}
									}
									if flag {
										kv.Shard2Client[op.ShardId] = append(kv.Shard2Client[op.ShardId], k)
									}
								}

							}
							kv.ShardState[op.ShardId] = OK
							kv.ShardNum[op.ShardId] = op.Num
							args := PullArgs{Num: op.Num - 1, ShardId: op.ShardId}
							kv.SendClearShard(op.Servers, &args)

						}

					}

				case Configuration:
					{
						// 只有版本号正好差一的时候才能迁移
						if kv.newConfig.Num+1 == op.Config.Num {
							kv.oldConfig = kv.newConfig
							kv.newConfig = op.Config
							for shardid, gid := range kv.newConfig.Shards {
								// 如果这个分片要被迁移走
								if gid != kv.gid && kv.oldConfig.Shards[shardid] == kv.gid {
									// 当这个 shard 没有被迁移
									if kv.ShardState[shardid] == OK {
										CloneMap := kv.Copy(shardid)
										// 这段是迁移 outeddata
										if len(CloneMap) == 0 {
											delete(kv.OutedData[op.Config.Num-1], shardid)
										} else {

											if len(kv.OutedData[op.Config.Num-1]) == 0 {
												tmp := make(map[int]map[string]string)
												tmp[shardid] = CloneMap
												kv.OutedData[op.Config.Num-1] = tmp
											} else {
												kv.OutedData[op.Config.Num-1][shardid] = CloneMap
											}

										}

										kv.Remove(shardid)
										// 变为迁移态, 更新版本号
										kv.ShardState[shardid] = Not
										kv.ShardNum[shardid] = op.Config.Num
									}
									// 不用被迁移走的情况就只需要更新一下版本号即可
								} else if gid == kv.gid && kv.oldConfig.Shards[shardid] == kv.gid {
									kv.ShardNum[shardid] = op.Config.Num
								}
							}
						}
					}
				case Clearshard:
					{
						delete(kv.OutedData[op.Num], op.ShardId)
						delete(kv.Shard2Client, op.ShardId)
						if len(kv.OutedData[op.Num]) == 0 {
							delete(kv.OutedData, op.Num)
						}

					}
				case PULL:
					{
						var reply PullReply
						tmp, ok := kv.ShardState[op.ShardId]
						if !ok || kv.ShardNum[op.ShardId] < op.Num {
							reply.Err = Not
						}
						if tmp == OK && kv.ShardNum[op.ShardId] == op.Num {

							reply.DB = kv.Copy(op.ShardId)
							CloneMap := kv.Copy(op.ShardId)
							if len(CloneMap) == 0 {
								delete(kv.OutedData[op.Num], op.ShardId)
							} else {

								if len(kv.OutedData[op.Num]) == 0 {
									tmp := make(map[int]map[string]string)
									tmp[op.ShardId] = CloneMap
									kv.OutedData[op.Num] = tmp
								} else {
									kv.OutedData[op.Num][op.ShardId] = CloneMap
								}

							}
							kv.Remove(op.ShardId)
							kv.ShardState[op.ShardId] = Not
							kv.ShardNum[op.ShardId] = op.Num

						} else {
							reply.DB = make(map[string]string)
							for k, v := range kv.OutedData[op.Num][op.ShardId] {
								reply.DB[k] = v
							}

						}
						reply.Err = OK
						reply.Client2Seq = make(map[int64]int)
						for k, v := range kv.Client2Seq {
							for _, value := range kv.Shard2Client[op.ShardId] {
								if k == value {
									reply.Client2Seq[k] = v
									break
								}
							}
						}
						ch2, exist := kv.Pullchan[op.Num*100+op.ShardId]
						if !exist {
							ch2 = make(chan PullReply, 1)
							kv.Pullchan[op.Num*100+op.ShardId] = ch2
						}
						go func() {
							select {
							case ch2 <- reply:
								return
							case <-time.After(time.Millisecond * 1000):
								return
							}

						}()
					}
				}
				if kv.LastApplied < ch.CommandIndex {
					kv.LastApplied = ch.CommandIndex
				}
				if kv.maxraftstate != -1 && kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
					kv.rf.Snapshot(kv.LastApplied, kv.PersistSnapShot())

				}
				kv.mu.Unlock()
				continue
			}
			op := tmp.Cmd.(KVOp)
			if !kv.CheckGroup(key2shard(op.Key)) {
				kv.mu.Unlock()
				continue
			}
			if ch.CommandIndex <= kv.LastApplied {
				kv.mu.Unlock()
				continue
			}

			kv.LastApplied = ch.CommandIndex
			kv.mu.Unlock()
			opchan := kv.GetChan(ch.CommandIndex)
			kv.mu.Lock()

			if kv.Client2Seq[op.ClientId] < op.Seq {

				shardId := key2shard(op.Key)
				switch op.Command {
				case PUT:
					kv.K2V[shardId][op.Key] = op.Value
				case APPEND:
					kv.K2V[shardId][op.Key] += op.Value
				}
				kv.Client2Seq[op.ClientId] = op.Seq

				if len(kv.Shard2Client[shardId]) == 0 {
					kv.Shard2Client[shardId] = make([]int64, 0)
				}
				flag := 0
				for _, v := range kv.Shard2Client[shardId] {
					if v == op.ClientId {
						flag = 1
						break
					}
				}
				if flag == 0 {
					kv.Shard2Client[key2shard(op.Key)] = append(kv.Shard2Client[key2shard(op.Key)], op.ClientId)
				}

			}

			if kv.maxraftstate != -1 && kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
				kv.rf.Snapshot(ch.CommandIndex, kv.PersistSnapShot())

			}
			if op.Command == GET {
				op.Value = kv.K2V[key2shard(op.Key)][op.Key]
			}

			kv.mu.Unlock()
			opchan <- op
		}

		if ch.SnapshotValid {
			kv.mu.Lock()
			if ch.SnapshotIndex > kv.LastApplied {

				kv.DecodeSnapShot(ch.Snapshot)
				kv.LastApplied = ch.SnapshotIndex
			}
			kv.mu.Unlock()
		}

	}

}

func (kv *ShardKV) ClearShard(args *PullArgs, reply *PullReply) {
	kv.mu.Lock()
	delete(kv.OutedData[args.Num], args.ShardId)
	delete(kv.Shard2Client, args.ShardId)
	if len(kv.OutedData[args.Num]) == 0 {
		delete(kv.OutedData, args.Num)
	}
	_, _, isLeader := kv.rf.Start(Op{Cmd: ShardOp{Command: Clearshard, ShardId: args.ShardId, Num: args.Num}, ShardValid: true})

	if isLeader {
		reply.Err = OK
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PullShard(args *PullArgs, reply *PullReply) {
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	if args.Num > kv.newConfig.Num {
		reply.Err = Not
		kv.mu.Unlock()
		return
	}

	tmp, ok := kv.ShardState[args.ShardId]
	if !ok || kv.ShardNum[args.ShardId] < args.Num {
		reply.Err = Not
		kv.mu.Unlock()
		return
	}
	if tmp == OK && kv.ShardNum[args.ShardId] == args.Num {
		_, _, isleader := kv.rf.Start(Op{ShardValid: true, Cmd: ShardOp{Command: PULL, ShardId: args.ShardId, Num: args.Num}})
		if !isleader {
			reply.Err = Not
			kv.mu.Unlock()
			return
		}
		ch, exist := kv.Pullchan[args.Num*100+args.ShardId]
		if !exist {
			ch = make(chan PullReply, 1)
			kv.Pullchan[args.Num*100+args.ShardId] = ch
		}
		kv.mu.Unlock()
		select {
		case app := <-ch:
			reply.DB = app.DB
			reply.Err = OK
			reply.Client2Seq = app.Client2Seq

		case <-time.After(time.Millisecond * 1000):
			reply.Err = Not

		}

		go func() {
			kv.mu.Lock()
			delete(kv.Pullchan, args.Num*100+args.ShardId)
			kv.mu.Unlock()
		}()
		return

	} else {
		reply.DB = make(map[string]string)
		for k, v := range kv.OutedData[args.Num][args.ShardId] {
			reply.DB[k] = v
		}
	}
	reply.Err = OK
	reply.Client2Seq = make(map[int64]int)
	for k, v := range kv.Client2Seq {
		for _, value := range kv.Shard2Client[args.ShardId] {
			if k == value {
				reply.Client2Seq[k] = v
				break
			}
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) SendClearShard(servers []string, args *PullArgs) {
	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		go func() {

			for !kv.killed() {
				var res PullReply
				ok := srv.Call("ShardKV.ClearShard", args, &res)
				if ok {
					return
				}

			}

		}()
	}
}

func (kv *ShardKV) SendPullShard(Group map[int][]string, args *PullArgs, oldgid int) {
	for !kv.killed() {
		if servers, ok := Group[oldgid]; ok {
			// try each server for the shard.
			var res PullReply
			flag := false
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				for !kv.killed() {
					var reply PullReply
					ok := srv.Call("ShardKV.PullShard", args, &reply)
					if ok {
						if reply.Err == ErrWrongLeader {
							break
						}
						if reply.Err == OK {
							if len(reply.DB) >= len(res.DB) {
								res.DB = reply.DB
								res.Client2Seq = reply.Client2Seq
								flag = true
							}
							break
						}
						if reply.Err == Not {
							break
						}
					} else {
						break
					}
					time.Sleep(time.Millisecond * 50)
				}
			}
			if flag {

				kv.mu.Lock()

				op := ShardOp{
					Command:    MigrateShard,
					DB:         res.DB,
					ShardId:    args.ShardId,
					Num:        args.Num + 1,
					Client2Seq: res.Client2Seq,
					Servers:    servers,
				}
				kv.rf.Start(Op{ShardValid: true, Cmd: op})
				kv.mu.Unlock()

				return
			}

		} else {
			return
		}
	}

}

func (kv *ShardKV) UpdateConfig() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		kv.mu.Lock()
		if kv.newConfig.Num != kv.oldConfig.Num {
			canConfig := true
			for shardid, gid := range kv.newConfig.Shards {
				if gid == kv.gid && kv.oldConfig.Shards[shardid] != kv.gid && kv.ShardNum[shardid] != kv.newConfig.Num {
					args := PullArgs{Num: kv.newConfig.Num - 1, ShardId: shardid}
					oldgid := kv.oldConfig.Shards[shardid]
					canConfig = false
					if oldgid == 0 {
						op := ShardOp{
							Command: MigrateShard,
							Num:     kv.newConfig.Num,
							DB:      make(map[string]string),
							ShardId: shardid,
						}
						kv.rf.Start(Op{ShardValid: true, Cmd: op})

						continue
					}
					Group := make(map[int][]string)
					for k, v := range kv.oldConfig.Groups {
						Group[k] = v
					}
					go kv.SendPullShard(Group, &args, oldgid)
				}
			}
			if canConfig {
				kv.oldConfig = kv.newConfig
			}
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 33)
			continue
		}
		nextnum := kv.newConfig.Num + 1
		kv.mu.Unlock()
		newConfig := kv.mck.Query(nextnum)
		kv.mu.Lock()
		if newConfig.Num == nextnum {
			//kv.rf.Start(Op{Command: Configuration, Config: newConfig, ShardValid: true})
			kv.rf.Start(Op{Cmd: ShardOp{Command: Configuration, Config: newConfig}, ShardValid: true})
			for shardid, gid := range newConfig.Shards {
				//lack
				if gid == kv.gid && kv.newConfig.Shards[shardid] != kv.gid {
					oldgid := kv.newConfig.Shards[shardid]
					if oldgid == 0 {
						op := ShardOp{
							Command: MigrateShard,
							Num:     kv.newConfig.Num + 1,
							DB:      make(map[string]string),
							ShardId: shardid,
						}
						kv.rf.Start(Op{ShardValid: true, Cmd: op})

						continue
					}
					args := PullArgs{Num: nextnum - 1, ShardId: shardid}
					Group := make(map[int][]string)
					for k, v := range kv.newConfig.Groups {
						Group[k] = v
					}
					go kv.SendPullShard(Group, &args, oldgid)

				}

			}

		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 33)
	}

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()

}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var db [shardctrler.NShards]map[string]string

	var Client2Seq map[int64]int
	var preconfig shardctrler.Config
	var lastconfig shardctrler.Config
	var ShardState map[int]string
	var ShardNum map[int]int
	var OutedData map[int]map[int]map[string]string
	var Shard2Client map[int][]int64

	if d.Decode(&db) != nil ||
		d.Decode(&Client2Seq) != nil || d.Decode(&preconfig) != nil ||
		d.Decode(&lastconfig) != nil ||
		d.Decode(&ShardState) != nil ||
		d.Decode(&ShardNum) != nil ||
		d.Decode(&OutedData) != nil ||
		d.Decode(&Shard2Client) != nil {
	} else {
		kv.K2V = db

		kv.Client2Seq = Client2Seq
		kv.newConfig = preconfig
		kv.oldConfig = lastconfig
		kv.ShardState = ShardState
		kv.ShardNum = ShardNum
		kv.OutedData = OutedData
		kv.Shard2Client = Shard2Client

	}

}

func (kv *ShardKV) PersistSnapShot() []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.K2V)
	e.Encode(kv.Client2Seq)
	e.Encode(kv.newConfig)
	e.Encode(kv.oldConfig)
	e.Encode(kv.ShardState)
	e.Encode(kv.ShardNum)
	e.Encode(kv.OutedData)
	e.Encode(kv.Shard2Client)
	snapshot := w.Bytes()
	return snapshot
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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.mck = shardctrler.MakeClerk(ctrlers)
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.Client2Seq = make(map[int64]int)
	kv.chans = make(map[int]chan KVOp)

	kv.K2V = [shardctrler.NShards]map[string]string{}
	kv.OutedData = make(map[int]map[int]map[string]string)
	kv.oldConfig = shardctrler.Config{Num: 0, Groups: map[int][]string{}}
	kv.newConfig = shardctrler.Config{Num: 0, Groups: map[int][]string{}}
	kv.ShardState = make(map[int]string)
	kv.ShardNum = make(map[int]int)
	kv.Shard2Client = make(map[int][]int64)
	kv.Pullchan = make(map[int]chan PullReply)
	snapshot := persister.ReadSnapshot()

	kv.DecodeSnapShot(snapshot)

	go kv.apply()
	go kv.UpdateConfig()
	gob.Register(ShardOp{})
	gob.Register(KVOp{})
	return kv
}

func (kv *ShardKV) GetChan(index int) chan KVOp {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, exist := kv.chans[index]
	if !exist {
		ch = make(chan KVOp, 1)
		kv.chans[index] = ch
	}
	return ch
}
