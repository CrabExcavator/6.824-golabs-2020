package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func isClosedWrite(ch <-chan PutAppendReply) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}

func isClosedRead(ch <-chan GetReply) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	WaitWriteCh chan PutAppendReply
	WaitReadCh  chan GetReply

	ID      int64
	Client  int64
	NodeNum int
	OpType  string
	Key     string
	Value   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	appliedID map[int64]int64
	kvTable   map[string]string
	sink      int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	cink := kv.appliedID[args.Client]
	kv.mu.Unlock()

	if cink < args.ID {
		op := Op{
			ID:         args.ID,
			Client:     args.Client,
			NodeNum:    kv.me,
			WaitReadCh: make(chan GetReply),
			OpType:     "Get",
			Key:        args.Key,
		}
		kv.rf.Start(op)

		t := time.NewTimer(time.Second * 1)
		defer t.Stop()
		select {
		case wait := <-op.WaitReadCh:
			{
				close(op.WaitReadCh)
				op.WaitReadCh = nil
				reply.Sink = wait.Sink
				reply.Err = wait.Err
				reply.Value = wait.Value
			}
		case <-t.C:
			{
				close(op.WaitReadCh)
				op.WaitReadCh = nil
				reply.Err = "Time Out"
			}
		}
	} else {
		reply.Err = "Applied"
		kv.mu.Lock()
		reply.Value = kv.kvTable[args.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	cink := kv.appliedID[args.Client]
	kv.mu.Unlock()

	if cink < args.ID {
		op := Op{
			ID:          args.ID,
			Client:      args.Client,
			NodeNum:     kv.me,
			WaitWriteCh: make(chan PutAppendReply),
			OpType:      args.Op,
			Key:         args.Key,
			Value:       args.Value,
		}
		kv.rf.Start(op)

		t := time.NewTimer(time.Second * 1)
		defer t.Stop()
		select {
		case wait := <-op.WaitWriteCh:
			{
				close(op.WaitWriteCh)
				op.WaitWriteCh = nil
				reply.Sink = wait.Sink
				reply.Err = wait.Err
			}
		case <-t.C:
			{
				close(op.WaitWriteCh)
				op.WaitWriteCh = nil
				reply.Err = "Time Out"
			}
		}
	} else {
		reply.Err = "Applied"
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.sink = 0
	kv.kvTable = make(map[string]string)
	kv.appliedID = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg, 10000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	checkSnapShot := func(index int) {
		if kv.maxraftstate != -1 && persister.RaftStateSize() >= kv.maxraftstate {
			log.Println("startSnapShot")
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.appliedID)
			e.Encode(kv.kvTable)
			data := w.Bytes()
			kv.rf.StartSnapShot(index, data)
		}
	}

	installSnapShot := func(data []byte) {
		if data == nil || len(data) < 1 {
			return
		}
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		if d.Decode(&kv.appliedID) != nil || d.Decode(&kv.kvTable) != nil {
			log.Println("decode error")
		}
	}

	installSnapShot(persister.ReadSnapshot())

	// You may need initialization code here.
	go func() {

		max := func(a int, b int) int {
			if a > b {
				return a
			}
			return b
		}

		for {
			select {
			case applyMsg := <-kv.applyCh:
				{
					kv.mu.Lock()

					kv.sink = max(kv.sink, applyMsg.CommandIndex)
					if applyMsg.IsSnapShot {
						installSnapShot(persister.ReadSnapshot())
						checkSnapShot(kv.sink)
						kv.mu.Unlock()
						continue
					}
					op := applyMsg.Command.(Op)
					if kv.appliedID[op.Client] >= op.ID {
						kv.mu.Unlock()
						continue
					}
					if op.OpType == "Put" || op.OpType == "Append" {
						if applyMsg.CommandValid == true {
							kv.appliedID[op.Client] = op.ID
							switch op.OpType {
							case "Put":
								{
									kv.kvTable[op.Key] = op.Value
								}
							case "Append":
								{
									kv.kvTable[op.Key] += op.Value
								}
							}
							//log.Println(kv.me, op.OpType, "<", op.Key, op.Value, ">", op.ID)
							if op.NodeNum == kv.me && op.WaitWriteCh != nil && !isClosedWrite(op.WaitWriteCh) {
								op.WaitWriteCh <- PutAppendReply{
									Err:  "",
									Sink: applyMsg.CommandIndex,
								}
							}
						} else if op.WaitWriteCh != nil && !isClosedWrite(op.WaitWriteCh) {
							op.WaitWriteCh <- PutAppendReply{
								Err:  "Not Leader",
								Sink: applyMsg.CommandIndex,
							}
						}
					} else if op.OpType == "Get" {
						if applyMsg.CommandValid == true {
							kv.appliedID[op.Client] = op.ID
							if op.NodeNum == kv.me && op.WaitReadCh != nil && !isClosedRead(op.WaitReadCh) {
								op.WaitReadCh <- GetReply{
									Err:   "",
									Sink:  applyMsg.CommandIndex,
									Value: kv.kvTable[op.Key],
								}
							}
						} else if op.WaitReadCh != nil && !isClosedRead(op.WaitReadCh) {
							op.WaitReadCh <- GetReply{
								Err: "Not Leader",
							}
						}
					}

					if applyMsg.CommandValid {
						checkSnapShot(kv.sink)
					}
					kv.mu.Unlock()
				}
			}
		}
	}()

	return kv
}
