package Utils

import (
	"golang.org/x/text/unicode/norm"
	"math/bits"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// MapFunctionId marks the id of a map function.
type MapFunctionId int

// This enum marks all the ids of a map function.
const (
	TypeMapLeftBound MapFunctionId = iota
	TypeMap1
	TypeMap2
	TypeMap3
	TypeMap4
	TypeMap5
	TypeMap6
	TypeMap7
	TypeMap8
	TypeMap9
	TypeMap10
	TypeMap11
	TypeMap12
	TypeMap13
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

	TypeMap1:  map1,
	TypeMap2:  map2,
	TypeMap3:  map3,
	TypeMap4:  map4,
	TypeMap5:  map5,
	TypeMap6:  map6,
	TypeMap7:  map7,
	TypeMap8:  map8,
	TypeMap9:  map9,
	TypeMap10: map10,
	TypeMap11: map11,
	TypeMap12: map12,
	TypeMap13: map13,
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

// map3 checks if the string has each vowel followed by 'p'.
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

// map4 checks if the string starts and ends with a vowel.
func map4(s string) bool {

	return IsVowel(rune(s[0])) && IsVowel(rune(s[len(s)-1]))

}

// map5 checks if the string is an anagram of the word "facultate".
func map5(s string) bool {

	word := "facultate"

	if len(s) != len(word) {
		return false
	}

	inputRunes := []rune(strings.ToLower(s))
	wordRunes := []rune(strings.ToLower(word))

	sort.Slice(inputRunes, func(i, j int) bool { return inputRunes[i] < inputRunes[j] })
	sort.Slice(wordRunes, func(i, j int) bool { return wordRunes[i] < wordRunes[j] })

	return string(inputRunes) == string(wordRunes)
}

// map6 returns true if the first and last letters are uppercase and the number of lower case letters if even.
func map6(s string) bool {

	upperCond := unicode.IsUpper(rune(s[0])) && unicode.IsUpper(rune(s[len(s)-1]))

	if !upperCond {
		return false
	}

	lowerCount := 0

	for _, char := range s {

		if unicode.IsLower(char) {
			lowerCount++
		}

	}

	return lowerCount%2 == 0
}

// map7 checks if the string has at least 2 diacritics.
func map7(s string) bool {

	diacriticsCount := 0
	normalizedString := norm.NFD.String(s)
	for _, char := range normalizedString {

		if unicode.Is(unicode.Mn, char) {

			diacriticsCount++
		}

	}

	return diacriticsCount >= 2

}

// map8 checks if a string is made of a sequence of one consonant and one vowel.
func map8(s string) bool {

	size := len(s)

	for i := 0; i < size; i += 2 {
		if IsVowel(rune(s[i])) {
			return false
		}

		if i != size-1 && !IsVowel(rune(s[i+1])) {
			return false
		}
	}
	return true
}

// map9 checks if the string is a valid password.
func map9(s string) bool {

	hasUpperCase := regexp.MustCompile("[A-Z]").MatchString(s)
	hasLowerCase := regexp.MustCompile("[a-z]").MatchString(s)
	hasDigit := regexp.MustCompile("[0-9]").MatchString(s)
	hasSpecialChar := regexp.MustCompile("[!@#~$%^&*=?]").MatchString(s)

	return hasUpperCase && hasLowerCase && hasDigit && hasSpecialChar
}

// map10 checks if a string is an absolute file path.
func map10(s string) bool {
	return filepath.IsAbs(s)
}

// map11 checks if the string ends in "escu".
func map11(s string) bool {
	return strings.HasSuffix(s, "escu")
}

// map12 returns true if the string can be converted to an int and is a Fibonacci number.
func map12(s string) bool {

	num, err := strconv.Atoi(s)
	if err != nil {
		return false
	}

	return IsFibo(num)
}

// map13 returns true if the string can be converted to an int and if it has 3 one bits in its binary representation.
func map13(s string) bool {

	num, err := strconv.Atoi(s)
	if err != nil {
		return false
	}

	return bits.OnesCount(uint(num)) == 3
}
