package main

import (
	"Distributed-Map-Reduce-System/Utils"
	"fmt"
)

func main() {

	task := Utils.NewMapTask(Utils.TypeMap1, "sd")

	buffer := task.Serialize()

	fmt.Println(buffer)

	var task1 Utils.ReduceTask

	err := task1.Deserialize(buffer)
	Utils.Panic(err)
	fmt.Println(task1)

	fmt.Println(task1.Complete())

}
