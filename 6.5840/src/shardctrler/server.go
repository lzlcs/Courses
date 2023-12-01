package shardctrler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	client2Seq map[int64]int
	chans      map[int]chan Op
}

type Op struct {
	Name     string
	ClientId int64
	SeqId    int

	QNum     int
	MShard   int
	MGID     int
	LGIDs    []int
	JServers map[int][]string
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {

	oldOp := Op{
		Name:     "Join",
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		JServers: args.Servers,
	}
	// fmt.Println("Join: ", oldOp)
	index, _, isLeader := sc.rf.Start(oldOp)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := sc.getCh(index)

	select {
	case newOp := <-ch:
		if newOp.ClientId != oldOp.ClientId || newOp.SeqId != oldOp.SeqId {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
		}
	case <-time.After(100 * time.Millisecond):
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.chans, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	oldOp := Op{
		Name:     "Leave",
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		LGIDs:    args.GIDs,
	}
	index, _, isLeader := sc.rf.Start(oldOp)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := sc.getCh(index)

	select {
	case newOp := <-ch:
		reply.WrongLeader = newOp.ClientId != oldOp.ClientId || newOp.SeqId != oldOp.SeqId
	case <-time.After(100 * time.Millisecond):
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.chans, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	oldOp := Op{
		Name:     "Move",
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		MShard:   args.Shard,
		MGID:     args.GID,
	}
	index, _, isLeader := sc.rf.Start(oldOp)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := sc.getCh(index)

	select {
	case newOp := <-ch:
		reply.WrongLeader = newOp.ClientId != oldOp.ClientId || newOp.SeqId != oldOp.SeqId
	case <-time.After(100 * time.Millisecond):
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.chans, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	oldOp := Op{
		Name:     "Query",
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		QNum:     args.Num,
	}
	index, _, isLeader := sc.rf.Start(oldOp)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := sc.getCh(index)

	select {
	case newOp := <-ch:
		if newOp.ClientId != oldOp.ClientId || newOp.SeqId != oldOp.SeqId {
			reply.WrongLeader = true
		} else {
			sc.mu.Lock()
			index := newOp.QNum
			if index == -1 || index >= len(sc.configs) {
				index = len(sc.configs) - 1
			}
			reply.Config = sc.configs[index]
			// fmt.Println("Query: ", len(sc.configs), reply.Config)
			sc.mu.Unlock()
		}
	case <-time.After(100 * time.Millisecond):
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.chans, index)
	sc.mu.Unlock()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.client2Seq = make(map[int64]int)
	sc.chans = make(map[int]chan Op)

	fmt.Print("")
	go sc.getLog()
	return sc
}

func (sc *ShardCtrler) getLog() {
	for {
		entry := <-sc.applyCh

		if entry.CommandValid {
			op := entry.Command.(Op)
			if sc.exist(op.ClientId, op.SeqId) {
				sc.mu.Lock()
				sc.client2Seq[op.ClientId] = op.SeqId
				switch op.Name {
				case "Join":
					sc.JoinConfig(op.JServers)
				case "Leave":
					sc.LeaveConfig(op.LGIDs)
				case "Move":
					sc.MoveConfig(op.MShard, op.MGID)
				}
				sc.mu.Unlock()
			}
			sc.getCh(entry.CommandIndex) <- op
		}
	}
}

func (sc *ShardCtrler) JoinConfig(servers map[int][]string) {
	newConfig := copyConfig(sc.configs[len(sc.configs)-1])

	// 添加组
	for gid, server := range servers {
		newConfig.Groups[gid] = server
	}

	sc.configs = append(sc.configs, newConfig)

	sc.rearrange()
}

func (sc *ShardCtrler) LeaveConfig(gids []int) {
	newConfig := copyConfig(sc.configs[len(sc.configs)-1])

	leaveGid := make(map[int]bool)
	// 删除对应的组并记录哪个组被删除
	for _, gid := range gids {
		delete(newConfig.Groups, gid)
		leaveGid[gid] = true
	}

	// 如果该组被删除了, 那就 Shards[i] 置零
	for i := 0; i < len(newConfig.Shards); i++ {
		gid := newConfig.Shards[i]
		if leaveGid[gid] {
			newConfig.Shards[i] = 0
		}
	}

	sc.configs = append(sc.configs, newConfig)
	sc.rearrange()
}
func (sc *ShardCtrler) MoveConfig(shard, gid int) {
	newConfig := copyConfig(sc.configs[len(sc.configs)-1])
	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
}

// 用于排序的结构体
type Temp struct {
	gid    int
	shards []int
}

func (sc *ShardCtrler) rearrange() {

	// 取出待负载均衡的配置
	newConfig := &sc.configs[len(sc.configs)-1]
	// 如果没有组, 那么直接返回
	if len(newConfig.Groups) == 0 {
		for i := 0; i < 10; i++ {
			newConfig.Shards[i] = 0
		}
		return
	}

	gid2Shards := make(map[int][]int)
	for shard, gid := range newConfig.Shards {
		gid2Shards[gid] = append(gid2Shards[gid], shard)
	}

	// shard[i] == 0 表明正在等待被分配
	waitToRearrange := gid2Shards[0]
	waitIndex := 0
	groupCount := len(newConfig.Groups)

	// 排序部分
	origin := make([]Temp, 0)
	for gid := range newConfig.Groups {
		if gid != 0 {
			x := Temp{gid, gid2Shards[gid]}
			origin = append(origin, x)
		}
	}

	sort.Slice(origin, func(i, j int) bool {
		// 为了稳定排序, 如果 a == b 就根据 gid 排序
		// 否则在多线程环境中会出错
		a, b := len(origin[i].shards), len(origin[j].shards)
		if a == b {
			return origin[i].gid > origin[j].gid
		}
		return a > b
	})

	// 计算 avg 和 bonus
	avg := 10 / groupCount
	bonus := 10 % groupCount

	for i := 0; i < len(origin); i++ {
		// goal 表示当前组需要几个 shard
		goal := avg
		if i < bonus {
			goal++
		}

		// 这个循环把缺少的都加进来
		for j := len(origin[i].shards); j < goal; j++ {
			origin[i].shards = append(origin[i].shards, waitToRearrange[waitIndex])
			waitIndex++
		}
		// 这个循环把多余的都加入等待队列
		for j := goal; j < len(origin[i].shards); j++ {
			waitToRearrange = append(waitToRearrange, origin[i].shards[j])
		}
	}

	// 重新设置 Shards
	for _, v := range origin {
		for _, s := range v.shards {
			// 这句话总共执行 10 次
			newConfig.Shards[s] = v.gid
		}
	}
}

func copyConfig(oldConfig Config) Config {
	newConfig := Config{}
	newConfig.Num = oldConfig.Num + 1
	newConfig.Shards = [10]int{}
	newConfig.Groups = make(map[int][]string)

	for k, v := range oldConfig.Groups {
		t := make([]string, len(v))
		copy(t, v)
		newConfig.Groups[k] = t
	}

	copy(newConfig.Shards[:], oldConfig.Shards[:])

	return newConfig
}

func (sc *ShardCtrler) getCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	ch, ok := sc.chans[index]

	if !ok {
		sc.chans[index] = make(chan Op, 1)
		ch = sc.chans[index]
	}

	return ch
}

func (sc *ShardCtrler) exist(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	id, ok := sc.client2Seq[clientId]

	if !ok {
		return true
	}

	return seqId > id
}
