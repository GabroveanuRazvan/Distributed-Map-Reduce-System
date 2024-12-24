package main

import (
	"Distributed-Map-Reduce-System/Utils"
	"time"
)

func main() {

	pool, err := Utils.NewConnectionPool("0.0.0.0:7878")
	Utils.Panic(err)
	pool.StartListenThread()
	pool.StartReceiveResultsThread()

	node := Utils.NewNetworkNode("127.0.0.1:7878")

	go func() {
		time.Sleep(5 * time.Second)
		node.Start(2)
	}()

	time.Sleep(1 * time.Second)

	task1 := Utils.NewMapTask(Utils.TypeMap1, "sdfsdf", 7)
	pool.SendTask(&task1)

	task2 := Utils.NewReduceTask(Utils.TypeReduce1, "sdfdsfds", []int{1, 1, 1}, 9)

	pool.SendTask(&task2)

	time.Sleep(5 * time.Second)

}
