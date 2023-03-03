package kvraft

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu       sync.Mutex
	clientId int64
	seqId    int64
	leaderId int
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
	ck.clientId = nrand()
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
//获取键的当前值。
//如果键不存在，则返回 “”。
//面对所有其他错误，不断尝试。
//您可以使用如下代码发送 RPC：
//ok ：= ck.servers[i].call（“KVServer.Get”， &args， &reply）

//参数和回复的类型（包括它们是否是指针）
//必须与 RPC 处理程序函数的声明类型匹配
//参数。并且回复必须作为指针传递。
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}

	DPrintf("Client[%d] Get starts, Key=%s ", ck.clientId, key)
	leaderId := ck.currentLeaderId()
	for {
		reply := GetReply{}
		if ck.servers[leaderId].Call("KVServer.Get", &args, &reply) {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				DPrintf("KVServer.Get ErrNoKey")
				return ""
			}
		}

		leaderId = ck.changeLeaderId()
		time.Sleep(1 * time.Millisecond)
	}

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
////
//由放置和追加共享。
////
//您可以使用如下代码发送 RPC：
//ok ：= ck.servers[i].Call（“KVServer.PutAppend”， &args， &reply）
////
//参数和回复的类型（包括它们是否是指针）
//必须与 RPC 处理程序函数的声明类型匹配
//参数。并且回复必须作为指针传递。
///

//PutAppend 先找到leader 在将key-value信息发送给leader
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}

	DPrintf("Client[%d] PutAppend, Key=%s Value=%s", ck.clientId, key, value)

	leaderId := ck.currentLeaderId()

	for {
		reply := PutAppendReply{}
		if ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply) {
			if reply.Err == OK {
				break
			}
		}

		leaderId = ck.changeLeaderId()
		time.Sleep(1 * time.Millisecond)
	}
}

func (ck *Clerk) currentLeaderId() (leaderId int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	leaderId = ck.leaderId
	return
}

func (ck *Clerk) changeLeaderId() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	return ck.leaderId
}
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OP_TYPE_PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OP_TYPE_APPEND)
}
