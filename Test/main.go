package main

import (
	"Distributed-Map-Reduce-System/Utils"
	"fmt"
)

func main() {

	res := Utils.NewTaskResult("mwre", 3, Utils.MapTaskId, 3)

	fmt.Println(res)
	buffer := res.Serialize()

	fmt.Println(buffer)

	var newRes Utils.TaskResult

	newRes.Deserialize(buffer)

	fmt.Println(newRes)

}
