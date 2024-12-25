package main

import (
	"Distributed-Map-Reduce-System/Utils"
	"fmt"
)

func main() {

	masterNode, err := Utils.NewConnectionPool("0.0.0.0:7878")
	masterNode.Start()
	Utils.Panic(err)

	input := [][]string{
		{"aabbb", "ebep", "blablablaa", "hijk", "wsww"},
		{"abba", "eeeppp", "cocor", "ppppppaa", "qwerty"},
		{"lalala", "lalal", "papapa", "aabbb", "acasq"}}

	fmt.Println(masterNode.RegisterProblem(input, Utils.TypeMap1))

	select {}
}
