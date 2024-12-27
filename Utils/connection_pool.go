package Utils

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionPool is a structure that manages many node connections.
type ConnectionPool struct {
	server           net.Listener
	connections      []net.Conn
	connectionsMutex sync.RWMutex
	haltingMutex     sync.Mutex
	haltingCond      *sync.Cond
	roundRobinIndex  int64

	// Maps each id of each problem to the number of tasks that need to be processed
	problemTasks sync.Map
	ppidCounter  int64

	// All problems are mapped to a channel that will return the result
	reduceResultPipes sync.Map
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
		haltingMutex:     sync.Mutex{},
		haltingCond:      nil,
		roundRobinIndex:  0,

		problemTasks: sync.Map{},
		ppidCounter:  1,

		reduceResultPipes: sync.Map{},
	}

	pool.haltingCond = sync.NewCond(&pool.haltingMutex)

	return pool, nil
}

// Start starts the connection pool and creates the threads that compose the architecture of the system.
func (connectionPool *ConnectionPool) Start() {

	resultsChannel := make(chan TaskResult)
	connectionPool.listenThread()
	connectionPool.receiveResultsThread(resultsChannel)
	connectionPool.resultProcessorThread(resultsChannel)
}

// addConnection locks the connection vector in order to append to it a new connection.
// Uses a Write lock on the connections.
func (connectionPool *ConnectionPool) addConnection(newConnection net.Conn) {

	connectionPool.connectionsMutex.Lock()
	defer connectionPool.connectionsMutex.Unlock()
	connectionPool.connections = append(connectionPool.connections, newConnection)

	// Signal the receiver and sender threads that there are open connections
	connectionPool.haltingCond.Broadcast()
}

// listenThread starts a thread that listens for new connections and adds them to the connection vector.
func (connectionPool *ConnectionPool) listenThread() {

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

// numConnections returns the number of remaining connections.
// Uses a Read lock on the connections.
func (connectionPool *ConnectionPool) numConnections() int {

	connectionPool.connectionsMutex.RLock()
	defer connectionPool.connectionsMutex.RUnlock()

	return len(connectionPool.connections)
}

// incrementRoundRobinIndex increments the index in a round-robin manner.
// Uses a Read lock on the connections.
func (connectionPool *ConnectionPool) incrementRoundRobinIndex() {

	// Compare and swap in a loop needed in case there is another thread that has already updated the indes, while
	// the current thread was in the middle of updating it
	for {
		numConnections := int64(connectionPool.numConnections())
		currentIndex := atomic.LoadInt64(&connectionPool.roundRobinIndex) % numConnections

		newIndex := (currentIndex + 1) % numConnections

		if atomic.CompareAndSwapInt64(&connectionPool.roundRobinIndex, currentIndex, newIndex) {
			break
		}
	}

}

// removeConnection removes a connection from connections and updates roundRobinIndex.
// Uses a Write lock on the connections.
func (connectionPool *ConnectionPool) removeConnection(connectionIndex int) {

	// Get the new number of connections first so that there will be no 2 locks on connections
	numConnections := int64(connectionPool.numConnections()) - 1

	// Lock the connections mutex for writing
	connectionPool.connectionsMutex.Lock()
	defer connectionPool.connectionsMutex.Unlock()
	// Remove the indexed connection from the array
	connectionPool.connections = append(connectionPool.connections[:connectionIndex], connectionPool.connections[connectionIndex+1:]...)

	// Check the number of connections to avoid division by 0
	if numConnections == 0 {
		return
	}

	// Make sure that the round-robin index remains valid after this removal
	for {
		currentIndex := atomic.LoadInt64(&connectionPool.roundRobinIndex)
		newIndex := currentIndex % numConnections

		if atomic.CompareAndSwapInt64(&connectionPool.roundRobinIndex, currentIndex, newIndex) {
			break
		}

	}
}

// RegisterProblem takes a matrix of strings and a type of mapping and sends tasks to the system in order to be solved.
// This function will block the current thread until the result of the problem is computed.
func (connectionPool *ConnectionPool) RegisterProblem(words [][]string, mapId MapFunctionId) float64 {

	if !isValidMapType(mapId) {
		Panic(errors.New("map id invalid"))
	}

	if !IsValidMatrix(words) {
		Panic(errors.New("invalid problem to solve"))
	}

	// Compute the number of tasks to scatter across the node cluster
	numTasks := 0
	numArrays := len(words)
	for _, array := range words {
		numTasks += len(array)
	}

	// Allocate a new ppid and map it to the number of tasks
	currentPpid := atomic.AddInt64(&connectionPool.ppidCounter, 1) - 1
	connectionPool.problemTasks.Store(currentPpid, numTasks)

	// Create the channel that will be used to retrieve the result
	resultChan := make(chan int, 1)
	connectionPool.reduceResultPipes.Store(currentPpid, resultChan)

	// For each string, wrap it into a task and send it to be processed
	for _, subVector := range words {
		for _, word := range subVector {
			mapTask := NewMapTask(mapId, word, currentPpid)
			connectionPool.sendTask(&mapTask)
		}
	}

	// Wait to get the result
	numValidWords := <-resultChan

	connectionPool.reduceResultPipes.Delete(currentPpid)
	return float64(numValidWords) / float64(numArrays)

}

// sendTask sends a task through a connection in a round-robin manner.
// Halts if there are no available connections to send the task.
// Uses Read locks on the connections.
func (connectionPool *ConnectionPool) sendTask(task Task) {

	// Build the message buffer by creating a buffer formed from the bytes of the encoding length and the encoding itself
	messageBuffer := bytes.NewBuffer(nil)
	encodingBuffer := task.Serialize()

	encodingLength := encodingBuffer.Len()
	lengthBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuffer, uint32(encodingLength))

	messageBuffer.Write(lengthBuffer)
	messageBuffer.Write(encodingBuffer.Bytes())

	// Halt if there are no connections
	connectionPool.haltThread()

	// Send the message through a connection
	connectionPool.connectionsMutex.RLock()
	currentConnection := connectionPool.connections[connectionPool.roundRobinIndex]
	_, err := currentConnection.Write(messageBuffer.Bytes())

	// Unlock the mutex so that there will be no 2 Read Locks on the same thread
	connectionPool.connectionsMutex.RUnlock()

	connectionPool.incrementRoundRobinIndex()
	Panic(err)
}

// receiveResultsThread receives data from each connection and decodes them into TaskResults.
// Through a channel forwards them to the resultProcessorThread.
func (connectionPool *ConnectionPool) receiveResultsThread(resultTx chan<- TaskResult) {

	go func() {

		receiveIndex := 0

		for {

			// Halt if there are no connections
			connectionPool.haltThread()

			connectionPool.connectionsMutex.RLock()
			currentConn := connectionPool.connections[receiveIndex]

			// Set a read deadline for the current connection
			readDeadline := time.Now().Add(200 * time.Millisecond)
			err := currentConn.SetReadDeadline(readDeadline)
			Panic(err)

			// Read the encoding length
			encodingLengthBytes := make([]byte, 4)
			_, err = currentConn.Read(encodingLengthBytes)

			// If the read timed out just continue to the next connection
			if TimeoutError(err) {

				// Unlock the current lock so that there will be no 2 Read locks
				connectionPool.connectionsMutex.RUnlock()
				// Update the receive index
				receiveIndex = (receiveIndex + 1) % connectionPool.numConnections()

				continue

				// If there is an error assume to end the connection
			} else if err != nil {

				// First unlock the mutex so that the connection can be removed
				connectionPool.connectionsMutex.RUnlock()
				connectionPool.removeConnection(receiveIndex)

				// Check the number of connections to avoid division by 0
				numConnections := int64(connectionPool.numConnections())
				if numConnections == 0 {
					receiveIndex = 0
				} else {
					receiveIndex = (receiveIndex + 1) % connectionPool.numConnections()
				}

				continue
			}

			encodingLength := binary.BigEndian.Uint32(encodingLengthBytes)

			// Read the encoding bytes using the encoding length
			encodingBytes := make([]byte, encodingLength)
			_, err = currentConn.Read(encodingBytes)
			Panic(err)

			connectionPool.connectionsMutex.RUnlock()

			// Deserialize the task result
			var taskRes TaskResult
			encodingBuffer := bytes.NewBuffer(encodingBytes)
			taskRes.Deserialize(encodingBuffer)

			resultTx <- taskRes

		}
	}()

}

// resultProcessorThread gathers the completed tasks from the nodes and computes the results.
// If the results come from map tasks, computes the reduce tasks and sends them to the node cluster.
func (connectionPool *ConnectionPool) resultProcessorThread(resultRx <-chan TaskResult) {

	go func() {

		// Make a map of ppids that stores maps of strings of arrays of results
		mapResults := make(map[int64]map[string][]int)
		// A map of ppids to reduced results for each problem
		reduceResults := make(map[int64]int)
		// Store for each ppid the number of received tasks
		taskCounter := make(map[int64]int)

		// Process each incoming task
		for taskResult := range resultRx {

			// Get the useful data and increment the task to its corresponding ppid
			ppid := taskResult.Ppid
			numInitialTasks, _ := connectionPool.problemTasks.Load(ppid)
			processedString := taskResult.ProcessedString
			result := taskResult.Result
			taskCounter[ppid]++
			numCurrentTasks := taskCounter[ppid]

			// Verify the type of task
			if taskResult.TaskID == MapTaskId {

				if _, exists := mapResults[ppid]; !exists {
					mapResults[ppid] = make(map[string][]int)
				}

				// Append the result to the corresponding results vector
				mapResults[ppid][processedString] = append(mapResults[ppid][processedString], result)

				// If the current set of tasks has been processed, send the reduce tasks
				if numInitialTasks == numCurrentTasks {

					// Reset the task counter as there will be at most the same number of reduce tasks as the map tasks
					taskCounter[ppid] = 0
					reduceNumTasks := connectionPool.sendReduceTasks(ppid, mapResults[ppid], TypeReduce1)

					// Delete the map results as they are no longer needed
					delete(mapResults, ppid)

					// Store the new expected number of tasks for this ppid
					connectionPool.problemTasks.Store(ppid, reduceNumTasks)
				}

			} else if taskResult.TaskID == ReduceTaskId {
				reduceResults[ppid] += result

				// If the current set of tasks has been processed,
				if numInitialTasks == numCurrentTasks {

					// Get the corresponding channel of this task and send the result
					val, _ := connectionPool.reduceResultPipes.Load(ppid)
					resultChan, _ := val.(chan int)

					resultChan <- reduceResults[ppid]

					// Delete the entries in each map as they are no longer needed
					delete(reduceResults, ppid)
					delete(taskCounter, ppid)

				}

			}

		}

	}()

}

// sendReduceTasks takes a bunch of strings and their map values and wraps them into ReduceTask to be sent back to the cluster of worker nodes.
// Return the number of reduce tasks sent.
// Uses Read locks on the connections.
func (connectionPool *ConnectionPool) sendReduceTasks(ppid int64, mapResults map[string][]int, reduceId ReduceFunctionId) int {

	numTasks := 0
	// For each entry in the map build a ReduceTask
	for processedString, results := range mapResults {
		numTasks += 1
		reduceTask := NewReduceTask(reduceId, processedString, results, ppid)
		connectionPool.sendTask(&reduceTask)
	}
	return numTasks
}

// / haltThread Halts the current thread until there are available connections
func (connectionPool *ConnectionPool) haltThread() {
	// Halt the thread if there are no connections
	connectionPool.haltingMutex.Lock()
	if connectionPool.numConnections() == 0 {
		connectionPool.haltingCond.Wait()
	}
	connectionPool.haltingMutex.Unlock()
}
