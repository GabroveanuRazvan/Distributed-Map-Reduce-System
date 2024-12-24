package main

import (
	"Distributed-Map-Reduce-System/Utils"
)

func main() {

	nodeCluster := []Utils.NetworkNode{
		Utils.NewNetworkNode("127.0.0.1:7878"),
		Utils.NewNetworkNode("127.0.0.2:7878"),
		Utils.NewNetworkNode("127.0.0.3:7878"),
	}

	for _, node := range nodeCluster {
		go func() { node.Start(2) }()
	}

	select {}
}
