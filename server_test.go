package raftrpc

import (
	"log"
	"os"
	"testing"
	"time"
)

func TestSingleServer(t *testing.T) {
	log.Println("TEST >>>>>TestSingleServer<<<<")

	srv := StartClusterServers("127.0.0.1:1234", 1, []string{"127.0.0.1:1234"})

	argP := PutArgs{Key: "test1", Value: "v1"}
	var reP PutReply
	err := srv.Put(&argP, &reP)
	if err != nil {
		t.Error("Error happen on ", err)
	}
	log.Println(">>", reP, err)

	argP = PutArgs{Key: "test1", Value: "v2"}
	err = srv.Put(&argP, &reP)
	if err != nil {
		t.Error("Error happen on ", err)
	}
	log.Println(">>", reP, err)

	if reP.PreviousValue != "v1" {
		t.Error("Error on last value, expect v1, got :", reP.PreviousValue)
	}

	argP = PutArgs{Key: "test1", Value: "v3"}
	err = srv.Put(&argP, &reP)
	if err != nil {
		t.Error("Error happen on ", err)
	}
	log.Println(">>", reP, err)

	if reP.PreviousValue != "v2" {
		t.Error("Error on last value, expect v1, got :", reP.PreviousValue)
	}

	argG := GetArgs{Key: "test1"}
	var reG GetReply
	err = srv.Get(&argG, &reG)
	if err != nil || reG.Value != "v3" {
		t.Error("Error happen on ", err, reG)
	}

	log.Println(">>", reG, err)
	log.Println("Stop server..")
	srv.kill()
	os.RemoveAll("raftexample-1")
}

func TestTwoServers(t *testing.T) {
	log.Println("TEST >>>>>TestTwoServers<<<<")

	var serverList []string
	serverList = append(serverList, "127.0.0.1:10001")
	serverList = append(serverList, "127.0.0.1:10002")

	srv1 := StartClusterServers(serverList[0], 1, serverList)
	srv2 := StartClusterServers(serverList[1], 2, serverList)

	argP := PutArgs{Key: "test1", Value: "v1"}
	var reP PutReply
	err := srv1.Put(&argP, &reP)
	if err != nil {
		t.Error("Error happen on ", err)
	}
	log.Println(">>", reP, err)

	argP = PutArgs{Key: "test1", Value: "v2"}
	err = srv2.Put(&argP, &reP)
	if err != nil {
		t.Error("Error happen on ", err)
	}
	log.Println(">>", reP, err)

	argP = PutArgs{Key: "test1", Value: "v3"}
	err = srv1.Put(&argP, &reP)
	if err != nil {
		t.Error("Error happen on ", err)
	}
	log.Println(">>", reP, err)

	//log.Printf("** Sleeping to visualize heartbeat between nodes **\n")
	//time.Sleep(3000 * time.Millisecond)

	argG := GetArgs{Key: "test1"}
	var reG GetReply
	err = srv1.Get(&argG, &reG)
	if err != nil || reG.Value != "v3" {
		t.Error("Error happen on ", err, reG)
	}

	//currently srv1 is leader, ask value from srv2 will introduce error
	err = srv2.Get(&argG, &reG)
	if err == nil {
		t.Error("Error to request value from non-leader", err, reG)
	}
	log.Println(">>", reG, err)
	log.Println("Stop server..")

	//Kill leader test
	srv1.kill()
	os.RemoveAll("raftexample-1")
	log.Printf("** Sleeping to visualize heartbeat between nodes **\n")
	time.Sleep(5000 * time.Millisecond)
	err = srv2.Get(&argG, &reG)
	if err != nil || reG.Value != "v3" {
		t.Error("Error on kill leader happen on ", err, reG)
	}
	srv2.kill()
	os.RemoveAll("raftexample-2")
}
