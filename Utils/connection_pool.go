package Utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"
)

// ConnectionPool is a structure that manages many node connections.
type ConnectionPool struct {
	server           net.Listener
	connections      []net.Conn
	connectionsMutex sync.RWMutex
	roundRobinIndex  int
}

// NewConnectionPool initializes the pool by creating a listener on the given address.
func NewConnectionPool(serverAddress string) (*ConnectionPool, error) {

	listener, err := net.Listen("tcp", serverAddress)

	if err != nil {
		return nil, err
	}

	pool := &ConnectionPool{
		server:           listener,
		connections:      make([]net.Conn, 0),
		connectionsMutex: sync.RWMutex{},
		roundRobinIndex:  0,
	}

	return pool, nil
}

// addConnection locks the connection vector in order to append to it a new connection.
func (connectionPool *ConnectionPool) addConnection(newConnection net.Conn) {

	connectionPool.connectionsMutex.Lock()
	defer connectionPool.connectionsMutex.Unlock()

	connectionPool.connections = append(connectionPool.connections, newConnection)

}

// StartListenThread starts a thread that listens for new connections and adds them to the connection vector.
func (connectionPool *ConnectionPool) StartListenThread() {

	go func() {

		for {

			client, err := connectionPool.server.Accept()

			if err != nil {
				println(err.Error())
				continue
			}

			connectionPool.addConnection(client)

		}

	}()

}

// NumConnections returns the number of remaining connections.
func (connectionPool *ConnectionPool) NumConnections() int {

	connectionPool.connectionsMutex.RLock()
	defer connectionPool.connectionsMutex.RUnlock()

	return len(connectionPool.connections)
}

// incrementRoundRobinIndex increments the index in a round-robin manner.
func (connectionPool *ConnectionPool) incrementRoundRobinIndex() {

	numConnections := connectionPool.NumConnections()

	connectionPool.roundRobinIndex = (connectionPool.roundRobinIndex + 1) % numConnections

}

// SendTask sends a task through a connection in a round-robin manner.
func (connectionPool *ConnectionPool) SendTask(task Task) {

	// Build the message buffer by creating a buffer formed from the bytes of the encoding length and the encoding itself
	messageBuffer := bytes.NewBuffer(nil)
	encodingBuffer := task.Serialize()

	encodingLength := encodingBuffer.Len()
	lengthBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuffer, uint32(encodingLength))

	messageBuffer.Write(lengthBuffer)
	messageBuffer.Write(encodingBuffer.Bytes())

	// Send the message through a connection
	connectionPool.connectionsMutex.RLock()
	defer connectionPool.connectionsMutex.RUnlock()

	currentConnection := connectionPool.connections[connectionPool.roundRobinIndex]
	_, err := currentConnection.Write(messageBuffer.Bytes())

	connectionPool.incrementRoundRobinIndex()
	Panic(err)

}

func (connectionPool *ConnectionPool) StartReceiveResultsThread() {

	receiveIndex := 0

	go func() {
		for {

			connectionPool.connectionsMutex.RLock()

			currentConn := connectionPool.connections[receiveIndex]
			receiveIndex = (receiveIndex + 1) % connectionPool.NumConnections()

			readDeadline := time.Now().Add(200 * time.Millisecond)
			err := currentConn.SetReadDeadline(readDeadline)
			Panic(err)

			// Read the encoding length
			encodingLengthBytes := make([]byte, 4)
			_, err = currentConn.Read(encodingLengthBytes)

			// If the read timed out just continue to the next connection
			if ReadTimeoutError(err) {
				continue
			} else {
				Panic(err)
			}

			encodingLength := binary.BigEndian.Uint32(encodingLengthBytes)

			// Read the encoding bytes using the encoding length
			encodingBytes := make([]byte, encodingLength)
			_, err = currentConn.Read(encodingBytes)
			Panic(err)

			// Deserialize the task result
			var taskRes TaskResult
			encodingBuffer := bytes.NewBuffer(encodingBytes)
			taskRes.Deserialize(encodingBuffer)

			fmt.Println("Task res", taskRes)

			connectionPool.connectionsMutex.RUnlock()
		}
	}()

}
