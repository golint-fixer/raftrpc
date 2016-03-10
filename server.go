// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raftrpc

import (
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"

	"github.com/coreos/etcd/raft/raftpb"

	"golang.org/x/net/context"
)

const Debug = 1
const ServTag string = "serv"

func socketPort(tag string, host int) string {
	s := "/var/tmp/rs-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	// s += "rd-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//Op Basic Raft OP here
type Op struct {
}

type KVRaft struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing

	// Your definitions here.
	store *kvstore
}

//Msg Deliver msg for raft status exchange
func (kv *KVRaft) Msg(ctx context.Context, message raftpb.Message) error {
	return nil
}

func (kv *KVRaft) Get(args *GetArgs, reply *GetReply) error {
	log.Println("[GET]", args)
	if args.Key == "" {
		log.Println("[GET]", InvalidParam)
		return errors.New(InvalidParam)
	}

	if v, ok := kv.store.Lookup(args.Key); ok {
		reply.Value = v
	}

	reply.Err = ErrNoKey
	return errors.New(ErrNoKey)
}

func (kv *KVRaft) Put(args *PutArgs, reply *PutReply) error {
	log.Println("[PUT]", args)

	if args.Key == "" || args.Value == "" {
		log.Println("[PUT]", InvalidParam)
		err := errors.New(InvalidParam)
		reply.Err = InvalidParam
		return err
	}

	if v, ok := kv.store.Lookup(args.Key); ok {
		reply.PreviousValue = v
	}

	reply.Err = "NIL"

	data := fmt.Sprintf("%s:%s", args.Key, args.Value)
	byteData := []byte(data)
	log.Println("[PUT] ", byteData)
	kv.store.Propose(args.Key, args.Value)
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVRaft) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	//kv.raftNode.stop()
	//remove socket file
	os.Remove(socketPort(ServTag, kv.me))
}

func StartServer(serversPort string, me int) *KVRaft {
	return startServer(serversPort, me, []string{serversPort})
}

func StartClusterServers(serversPort string, me int, cluster []string) *KVRaft {
	return startServer(serversPort, me, cluster)
}

func StarServerJoinCluster() {

}

func startServer(serversPort string, me int, cluster []string) *KVRaft {
	gob.Register(Op{})

	kv := new(KVRaft)
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	proposeC := make(chan string)
	//defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	//defer close(confChangeC)
	join := false

	//node
	commitC, errorC := newRaftNode(me, cluster, join, proposeC, confChangeC)

	//kvstore
	kv.store = newKVStore(proposeC, commitC, errorC)

	log.Println("[server] ", me, " ==> ", serversPort)
	l, e := net.Listen("tcp", serversPort)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVRaft(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
