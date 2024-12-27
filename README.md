# Distributed-Map-Reduce-System

## Problem description
A large input of an array of arrays of strings is provided. We need to get
the mean number of strings that satisfy a certain condition.

Example:
```go
arr := [][]string{
		{"level", "radar", "hello", "world", "madam"},
		{"test", "rotor", "wow", "golang", "refer"},
		{"example", "deed", "not", "a", "civic"},
	}
	
Result: 1.66 
```

## Motivation

As the data provided can be large, the problem shall be distributed using the [**Map-Reduce paradigm**](https://en.wikipedia.org/wiki/MapReduce).

## Architecture of the system

A **Master Node** is used that has the following attributes:
- Shall be connected to each worker node.
- Accepts new worker nodes dynamically.
- Removes closed worker nodes dynamically.
- Distributes the tasks following a policy.
- Gathers the **mapping results** sorts them and distributes the **reduce tasks** back to the nodes.
- Gathers the **reduce results** and computes the overall result of the initial data.

A cluster of **Worker Nodes**, each node having the following attributes:

- Shall receive either **mapping tasks** or **reduce tasks**, distinguish between them and solve them.
- Shall send back the result to the master node.
- Is able to gracefully shut down.

## Deeper architecture

### Master Node

1. Runs a **listener thread** that accepts the new worker nodes and adds them to the **connection pool**.
2. Runs a **receiver thread** that is responsible to remove closed connections, to fetch tasks from the
**worker nodes**, deserialize them and send them to be processed by the **processor thread**
3. Runs a **processor thread** that receives either **mapping tasks** or **reduce tasks**. It is responsible to
check if each task from a problem has been gathered and to decide weather to return the problem result or to send
**reduce tasks** back to the worker nodes.

### Worker Node
1. Runs a **task receiver thread** that deserializes the tasks and sends them to a **thread pool** to be processed and completed.
2. Runs a **thread pool** that shall get deserialized tasks and complete it.

## How to run

Build the **Worker Node** executable.
```bash
    Distributed-Map-Reduce-System/Prototype-System$ go build -o Worker-Nodes/worker-node Worker-Nodes/main.go
```

Run the **Master Node** script.
```bash
    Distributed-Map-Reduce-System/Prototype-System$ go run main.go
```