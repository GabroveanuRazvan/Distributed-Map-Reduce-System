package main

import "Distributed-Map-Reduce-System/Utils"

func main() {

	masterNode, err := Utils.NewConnectionPool("0.0.0.0:7878")
	masterNode.Start()
	Utils.Panic(err)

	input := [][]string{
		{"aabbb", "ebep", "blablablaa", "hijk", "wsww"},
		{"abba", "eeeppp", "cocor", "ppppppaa", "qwerty", "acasq"},
		{"lalala", "lalal", "papapa", "papap"}}

	masterNode.RegisterProblem(input, Utils.TypeMap1)

	select {}
}
