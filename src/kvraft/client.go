package kvraft

import (
	"../labrpc"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	master int
	msgID  int
	me     int64
	//sink int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.master = 0
	ck.msgID = 0
	ck.me = nrand()
	//ck.sink = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	//log.Println("get")
	// You will have to modify this function.
	ret := ""
	currentServer := ck.master
	rID := ck.msgID + 1
	ck.msgID++
	flag := false
	for {
		args := GetArgs{
			ID:     rID,
			Client: ck.me,
			Key:    key,
		}
		reply := GetReply{}
		ok := ck.servers[currentServer].Call("KVServer.Get", &args, &reply)
		if ok {
			if len(reply.Err) == 0 {
				ret = reply.Value
				break
			} else {
				//log.Println(reply.Err)
				switch reply.Err {
				case "Not Leader":
					{
						currentServer = (currentServer + 1) % len(ck.servers)
					}
				case "Time Out":
					{
						currentServer = (currentServer + 1) % len(ck.servers)
					}
				case "Applied":
					{
						ret = reply.Value
						flag = true
						break
					}
				}
				if flag {
					break
				}
			}
		} else {
			currentServer = (currentServer + 1) % len(ck.servers)
		}
		time.Sleep(time.Millisecond * 100)
	}
	ck.master = currentServer
	return ret
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//log.Println("put")
	// You will have to modify this function.
	currentServer := ck.master
	rID := ck.msgID + 1
	ck.msgID++
	flag := false
	for {
		args := PutAppendArgs{
			ID:     rID,
			Client: ck.me,
			Key:    key,
			Value:  value,
			Op:     op,
		}
		reply := GetReply{}
		ok := ck.servers[currentServer].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if len(reply.Err) == 0 {
				break
			} else {
				//log.Println(reply.Err)
				switch reply.Err {
				case "Not Leader":
					{
						currentServer = (currentServer + 1) % len(ck.servers)
					}
				case "Time Out":
					{
						currentServer = (currentServer + 1) % len(ck.servers)
					}
				case "Applied":
					{
						flag = true
						break
					}
				}
				if flag {
					break
				}
			}
		} else {
			currentServer = (currentServer + 1) % len(ck.servers)
		}
		time.Sleep(time.Millisecond * 100)
	}
	ck.master = currentServer
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
