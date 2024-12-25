package main

import (
	"fmt"
)

func main() {

	m := make(map[int]map[string][]int)

	c, exists := m[1]
	fmt.Println(c, exists)

}
