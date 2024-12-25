package Utils

import "strings"

// MapFunctionId marks the id of a map function.
type MapFunctionId int

// This enum marks all the ids of a map function.
const (
	TypeMapLeftBound MapFunctionId = iota
	TypeMap1
	TypeMap2
	TypeMap3
	TypeMapRightBound
)

// isValidMapType checks if the given map id is valid.
func isValidMapType(t MapFunctionId) bool {

	if TypeMapLeftBound >= t || t >= TypeMapRightBound {
		return false
	}
	return true
}

// MapFunctionRegistry maps each map function id to the specific function.
var MapFunctionRegistry = map[MapFunctionId]func(string) bool{

	TypeMap1: map1,
	TypeMap2: map2,
	TypeMap3: map3,
}

// map1 returns true if the current string has an even number of vowels and the number of consonants can be divided by 3.
func map1(s string) bool {

	numVowels := 0
	numConsonants := 0
	vowels := []byte{'a', 'e', 'i', 'o', 'u'}

	s = strings.ToLower(s)

	for i := 0; i < len(s); i++ {
		char := s[i]

		if Contains(vowels, char) {
			numVowels++
		} else {
			numConsonants++
		}

	}

	return numVowels%2 == 0 && numConsonants%3 == 0

}

// map2 returns true if a string is a palindrome.
func map2(s string) bool {

	left := 0
	right := len(s) - 1

	for left < right {

		if s[left] != s[right] {
			return false
		}

		left++
		right--
	}
	return true
}

func map3(s string) bool {

	for index, char := range s {

		var nextChar uint8

		if index == len(s)-1 {
			nextChar = 'a'
		} else {
			nextChar = s[index+1]
		}

		if IsVowel(char) && nextChar != 'p' {
			return false
		}

	}
	return true
}
