package main

import (
	"Distributed-Map-Reduce-System/Utils"
	"fmt"
)

func main() {

	task := Utils.NewReduceTask(Utils.TypeReduce1, "mere", []int{1, 1, 1, 1, 1, 1, 0, 0})

	fmt.Println(task.Complete())

}
