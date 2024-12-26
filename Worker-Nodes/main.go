package main

import (
	"Distributed-Map-Reduce-System/Utils"
	"fmt"
	"os"
	"strconv"
)

func main() {

	if len(os.Args) < 3 {
		fmt.Println("Usage: ./worker-node <peer address: <ipv4>:<port> > <number of threads>")
		os.Exit(1)
	}

	peerAddress := os.Args[1]
	numThreads, err := strconv.Atoi(os.Args[2])

	if err != nil || numThreads <= 0 {
		fmt.Println("Usage: ./worker-node <number of threads>")
		os.Exit(1)
	}

	workerNode := Utils.NewNetworkNode(peerAddress)
	workerNode.Start(uint32(numThreads))

	os.Exit(0)
}
