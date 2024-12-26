package Utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
)

type NetworkNode struct {
	connection  net.Conn
	peerAddress string
}

// NewNetworkNode initializes a new unconnected network node.
func NewNetworkNode(peerAddress string) NetworkNode {
	return NetworkNode{
		connection:  nil,
		peerAddress: peerAddress,
	}
}

// Start attempts to connect to the peer and creates a new ThreadPool.
// In a loop receive tasks and send them in a thread pool to be completed and sent back to the peer.
func (node *NetworkNode) Start(numWorkers uint32) {
	connection, err := net.Dial("tcp", node.peerAddress)
	Panic(err)

	node.connection = connection

	threadPool := NewThreadPool(numWorkers)

	for {
		task := node.ReceiveTask()
		threadPool.Go(func() {
			// Complete the task and get its result
			result := task.Complete()

			// Serialize the task and build the message buffer by concatenating the length and the serialized result into a single buffer
			resultBuffer := result.Serialize()
			resultLen := resultBuffer.Len()

			lenBuffer := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuffer, uint32(resultLen))

			messageBuffer := bytes.NewBuffer(nil)
			messageBuffer.Write(lenBuffer)
			messageBuffer.Write(resultBuffer.Bytes())

			_, err := node.connection.Write(messageBuffer.Bytes())
			Panic(err)
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

// CreateNodeCluster creates a cluster of nodes by running the executable of each node in a new process.
func CreateNodeCluster(numNodes uint32, peerAddress string, numThreadsPerNode uint32, executablePath string) {

	// Check if the executable exists
	_, err := os.Stat(executablePath)

	if err != nil {
		fmt.Println("Executable file does not exist")
		os.Exit(1)
	}

	var wg sync.WaitGroup

	// Create a new process for each node
	for i := 0; i < int(numNodes); i++ {

		wg.Add(1)

		go func() {
			defer wg.Done()
			numThreads := strconv.Itoa(int(numThreadsPerNode))

			cmd := exec.Command(executablePath, peerAddress, numThreads)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			err := cmd.Run()
			if err != nil {
				fmt.Println(err)
			}

		}()

	}

}
