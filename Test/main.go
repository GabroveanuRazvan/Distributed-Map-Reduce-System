package main

import (
	"Distributed-Map-Reduce-System/Utils"
	"fmt"
)

func main() {

	mapTask := Utils.NewMapTask(Utils.TypeMap1, "ceva", 6)
	buffer := mapTask.Serialize()

	newTask, err := Utils.DeserializeTask(buffer)
	Utils.Panic(err)
	fmt.Println(newTask)

	var mapTask2 Utils.MapTask
	buffer = mapTask.Serialize()
	err = mapTask2.Deserialize(buffer)
	Utils.Panic(err)
	fmt.Println(mapTask2)

}
