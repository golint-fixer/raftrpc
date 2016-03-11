RaftRPC: Simple RPC Key Value Server using etcd/Raft in Golang
==============

[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/kkdai/pd/master/LICENSE)  [![GoDoc](https://godoc.org/github.com/kkdai/raftrpc?status.svg)](https://godoc.org/github.com/kkdai/raftrpc)  [![Build Status](https://travis-ci.org/kkdai/raftrpc.svg?branch=master)](https://travis-ci.org/kkdai/raftrpc)



What is Raft RPC Server/Client
=============

This is a simple KV Server (Key/Value) server using raft consensus technology(implement by.

What is Raft
=============




Installation and Usage
=============


Install
---------------
```
go get github.com/kkdai/raftrpc
```

Usage
---------------

### Server Example(1) Single Server:

```go
package main
    
import (
	"fmt"
    
	. "github.com/kkdai/raftrpc"
)
    
func main() {
	forever := make(chan int)

	//RPC addr
	rpcAddr := "127.0.0.1:1234"
	srv := StartServer(rpcAddr, 1)

	<-forever
}
```

### Server Example(2) Cluster Server:

```go
package main
    
import (
	"fmt"
    
	. "github.com/kkdai/raftrpc"
)
    
func main() {
	forever := make(chan int)
	
	//Note there are two address and port.
	//
	// "127.0.0.1:1234" is RPC access point
	// "http://127.0.0.1:12379" is raft message access point which use http
	
	var raftMsgSrvList []string
	raftMsgSrvList = append(raftMsgSrvList, "http://127.0.0.1:12379")
	raftMsgSrvList = append(raftMsgSrvList, "http://127.0.0.1:22379")

	srv1 := StartClusterServers("127.0.0.1:1234", 1, raftMsgSrvList)
	srv2 := StartClusterServers("127.0.0.1:1235", 2, raftMsgSrvList)
	
	<-forever
}
```

### Client Example

Assume a server exist on `127.0.0.1:1234`.


```go
package main
    
import (
	"fmt"
    
	. "github.com/kkdai/raftrpc"
)
    
func main() {
	client := MakeClerk("127.0.0.1:1234")
	client.Put("t1", "v1")
	log.Println("got:", ret)
	if ret != "v1" {
		log.Println("Client get error:", ret)
	}
}	
```

Inspired By
---------------
- [CoreOS ETCD source code](https://github.com/coreos/etcd)
- [ETCD Example](https://github.com/coreos/etcd/tree/master/contrib/raftexample)
- [Raft: A First Implementation](http://otm.github.io/2015/05/raft-a-first-implementation/)

Project52
---------------

It is one of my [project 52](https://github.com/kkdai/project52).


License
---------------

etcd is under the Apache 2.0 [license](LICENSE). See the LICENSE file for details.