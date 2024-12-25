package main

import (
	"Distributed-Map-Reduce-System/Utils"
	"fmt"
)

func main() {

	masterNode, err := Utils.NewConnectionPool("0.0.0.0:7878")
	masterNode.Start()
	Utils.Panic(err)

	//input1 := [][]string{
	//	{"aabbb", "ebep", "blablablaa", "hijk", "wsww"},
	//	{"abba", "eeeppp", "cocor", "ppppppaa", "qwerty"},
	//	{"lalala", "lalal", "papapa", "aabbb", "acasq"}}
	//
	//go func() {
	//	fmt.Println(masterNode.RegisterProblem(input1, Utils.TypeMap1))
	//}()

	input2 := [][]string{
		{"a1551a", "parc", "ana", "minim2", "1pcl3"},
		{"calabalac", "tivit", "leu", "zece10", "ploaie", "9ana9"},
		{"lalalal", "tema", "papa", "ger"}}

	go func() {
		fmt.Println(masterNode.RegisterProblem(input2, Utils.TypeMap2))
	}()

	input3 := [][]string{
		{"apap", "paprc", "apnap", "mipnipm", "copil"},
		{"cepr", "program", "lepu", "zepcep", "golang", "tema"},
		{"par", "impar", "papap", "gepr"}}

	go func() {
		fmt.Println(masterNode.RegisterProblem(input3, Utils.TypeMap3))
	}()

	select {}
}
