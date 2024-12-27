package Utils

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

type NetworkNode struct {
	connection      net.Conn
	peerAddress     string
	threadPool      *ThreadPool
	shutdownContext context.Context
	contextCancel   context.CancelFunc
	contextWG       sync.WaitGroup
}

// NewNetworkNode initializes a new unconnected network node.
func NewNetworkNode(peerAddress string) NetworkNode {

	shutdownContext, contextCancel := context.WithCancel(context.Background())
	return NetworkNode{
		connection:      nil,
		peerAddress:     peerAddress,
		threadPool:      nil,
		shutdownContext: shutdownContext,
		contextCancel:   contextCancel,
		contextWG:       sync.WaitGroup{},
	}
}

// Start attempts to connect to the peer and creates a new ThreadPool.
// In a loop receive tasks and send them in a thread pool to be completed and sent back to the peer.
func (node *NetworkNode) Start(numWorkers uint32, verbose bool) {
	connection, err := net.Dial("tcp", node.peerAddress)
	Panic(err)

	node.connection = connection
	node.threadPool = NewThreadPool(numWorkers, verbose)
	node.receiverThread()
}

// receiverThread listens with a timeout for tasks and sends them to the thread pool to be processed.
// At each iteration checks the cancellation of the context in order to gracefully shutdown when needed.
func (node *NetworkNode) receiverThread() {

	node.contextWG.Add(1)

	go func() {

		defer node.contextWG.Done()
		isActive := true

		for isActive {

			select {

			// Check the shutdown signal, stop the loop and decrement the waiting group
			case <-node.shutdownContext.Done():
				isActive = false
				break

			// attempt to receive a task and send it to be processed if the reception was successful
			default:

				task := node.ReceiveTask()

				if task == nil {
					continue
				}

				node.threadPool.Go(func() {

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

	}()

}

// ReceiveTask reads bytes from the connection and deserializes the bytes to form a given task.
// Will use a lock on the connection
func (node *NetworkNode) ReceiveTask() Task {

	// Set a read deadline for the connection
	readDeadline := time.Now().Add(200 * time.Millisecond)
	err := node.connection.SetReadDeadline(readDeadline)
	Panic(err)

	// Read the encoding length
	encodingLengthBytes := make([]byte, 4)
	_, err = node.connection.Read(encodingLengthBytes)

	// Return nil if a timeout occurred
	if TimeoutError(err) {
		return nil
	} else {
		node.CloseNode()
		Panic(err)
	}

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

// CloseNode closes the connection and waits for each thread to finish.
func (node *NetworkNode) CloseNode() {

	// First send the signal and wait for it to be received
	node.contextCancel()
	node.contextWG.Wait()

	// Then wait for the thread pool to finish processing and close the connection
	node.threadPool.Wait()

	err := node.connection.Close()
	fmt.Println("Closing node connection")
	Panic(err)

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
