package Utils

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// Task is an interface for distributed tasks.
type Task interface {
	Complete() TaskResult
	Serialize() *bytes.Buffer
	Deserialize(buffer *bytes.Buffer) error
}

type TaskId = int

const (
	MapTaskId TaskId = iota
	ReduceTaskId
)

// TaskDeserializationError implements Error and is used when deserializing tasks.
type TaskDeserializationError struct {
	Message string
}

func (e TaskDeserializationError) Error() string {
	return fmt.Sprintf(e.Message)
}

func NewTaskDeserializationError(message string) TaskDeserializationError {
	return TaskDeserializationError{message}
}

// MapTask is a type of Task that applies a function on a string.
type MapTask struct {
	MapId           MapFunctionId
	StringToProcess string
	Ppid            int
}

// NewMapTask initializes a new map task.
func NewMapTask(mapId MapFunctionId, stringToProcess string, ppid int) MapTask {
	return MapTask{
		MapId:           mapId,
		StringToProcess: stringToProcess,
		Ppid:            ppid,
	}
}

// Complete on a MapTask applies the corresponding function on the given string.
func (task *MapTask) Complete() TaskResult {
	result := BoolToInt(MapFunctionRegistry[task.MapId](task.StringToProcess))

	return NewTaskResult(task.StringToProcess, result, MapTaskId, task.Ppid)
}

// Serialize serializes the current object and returns it's buffer.
func (task *MapTask) Serialize() *bytes.Buffer {

	buffer := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(task)
	Panic(err)

	return buffer
}

// Deserialize deserializes the given buffer and stores the Task into the current object.
func (task *MapTask) Deserialize(buffer *bytes.Buffer) error {

	var newTask MapTask
	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(&newTask)

	if err != nil {
		return err
	}

	if newTask.MapId >= TypeMapRightBound || newTask.MapId <= TypeMapLeftBound {
		return NewTaskDeserializationError("Deserialization error. Inconsistent bytes.")
	}

	task.StringToProcess = newTask.StringToProcess
	task.MapId = newTask.MapId

	return nil
}

// ReduceTask is a type of Task that reduces the map results of the given string.
type ReduceTask struct {
	ReduceId        ReduceFunctionId
	StringToProcess string
	Predicates      []int
	Ppid            int
}

// NewReduceTask initializes a new reduce task.
func NewReduceTask(reduceId ReduceFunctionId, stringToProcess string, predicates []int, ppid int) ReduceTask {
	return ReduceTask{
		ReduceId:        reduceId,
		StringToProcess: stringToProcess,
		Predicates:      predicates,
		Ppid:            ppid,
	}
}

// Complete on a MapTask applies the corresponding function on the given results.
func (task *ReduceTask) Complete() TaskResult {
	result := ReduceFunctionRegistry[task.ReduceId](task.Predicates)

	return NewTaskResult(
		task.StringToProcess,
		result,
		ReduceTaskId,
		task.Ppid)
}

// Serialize serializes the current object and returns it's buffer.
func (task *ReduceTask) Serialize() *bytes.Buffer {

	buffer := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(task)
	Panic(err)

	return buffer
}

// Deserialize deserializes the given buffer and stores the Task into the current object.
func (task *ReduceTask) Deserialize(buffer *bytes.Buffer) error {

	var newTask ReduceTask
	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(&newTask)

	if err != nil {
		return err
	}

	if newTask.ReduceId >= TypeReduceRightBound || newTask.ReduceId <= TypeReduceLeftBound {
		return NewTaskDeserializationError("Deserialization error. Inconsistent bytes.")
	}

	task.StringToProcess = newTask.StringToProcess
	task.ReduceId = newTask.ReduceId
	task.Predicates = newTask.Predicates

	return nil
}

func DeserializeTask(buffer *bytes.Buffer) (Task, error) {
	bufferCopy := bytes.NewBuffer(nil)
	bufferCopy.Write(buffer.Bytes())

	var newTask MapTask
	err := newTask.Deserialize(buffer)

	if err != nil {
		var task ReduceTask
		err = task.Deserialize(bufferCopy)

		if err != nil {
			return nil, err
		}

		return &task, nil

	}

	return &newTask, nil
}

// TaskResult represents the data returned by a Task.
type TaskResult struct {
	ProcessedString string
	Result          int
	TaskID          TaskId
	Ppid            int
}

// NewTaskResult initializes a new TaskResult.
func NewTaskResult(processedString string, result int, taskId TaskId, ppi int) TaskResult {
	return TaskResult{processedString, result, taskId, ppi}
}

// Serialize serializes the current TaskResult and returns its bytes buffer.
func (taskResult *TaskResult) Serialize() *bytes.Buffer {
	buffer := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(taskResult)
	Panic(err)

	return buffer
}

// Deserialize deserializes the given bytes buffer into the current object.
func (taskResult *TaskResult) Deserialize(buffer *bytes.Buffer) {

	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(taskResult)
	Panic(err)

}
