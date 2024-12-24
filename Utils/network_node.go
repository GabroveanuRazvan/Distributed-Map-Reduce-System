package Utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

type NetworkNode struct {
	connection  net.Conn
	peerAddress string
}

// NewNetworkNode initializes a new unconnected network node
func NewNetworkNode(peerAddress string) NetworkNode {
	return NetworkNode{
		connection:  nil,
		peerAddress: peerAddress,
	}
}

func (node *NetworkNode) Start(numWorkers uint32) {
	connection, err := net.Dial("tcp", node.peerAddress)
	Panic(err)

	node.connection = connection

	threadPool := NewThreadPool(numWorkers)

	for {
		task := node.ReceiveTask()
		threadPool.Go(func() {
			result := task.Complete()
			fmt.Println(result)
		})
	}

}

// ReceiveTask reads bytes from the connection and deserializes the bytes to form a given task.
func (node *NetworkNode) ReceiveTask() Task {

	// Read the encoding length
	encodingLengthBytes := make([]byte, 4)
	_, err := node.connection.Read(encodingLengthBytes)
	Panic(err)
	encodingLength := binary.BigEndian.Uint32(encodingLengthBytes)

	// Read the encoding bytes using the encoding length
	encodingBytes := make([]byte, encodingLength)
	_, err = node.connection.Read(encodingBytes)
	Panic(err)

	// Deserialize the task
	encodingBuffer := bytes.NewBuffer(encodingBytes)
	task, err := DeserializeTask(encodingBuffer)
	Panic(err)

	return task
}
