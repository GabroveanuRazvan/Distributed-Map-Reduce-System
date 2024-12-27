package Utils

import (
	"net"
	"strings"
)

// BoolToInt converts a bool to an integer.
func BoolToInt(predicate bool) int {
	if predicate {
		return 1
	}

	return 0
}

func IsVowel(c rune) bool {
	vowels := "aeiouAEIOU"
	return strings.ContainsRune(vowels, c)
}

// Sum sums up the int values of a vector.
func Sum(values []int) int {

	sum := 0

	for _, value := range values {
		sum += value
	}

	return sum
}

// Contains checks if an element is present in a given slice.
func Contains[T comparable](slice []T, value T) bool {

	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

// Panic panics if the error is not nil.
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

// TimeoutError checks if an error is a timeout one and checks if it really did time out.
func TimeoutError(err error) bool {
	netErr, ok := err.(net.Error)
	return ok && netErr.Timeout()
}

// IsValidMatrix checks weather a matrix contains elements
func IsValidMatrix[T any](matrix [][]T) bool {

	if len(matrix) == 0 {
		return false
	}

	if len(matrix[0]) == 0 {
		return false
	}

	return true
}

func IsFibo(n int) bool {

	if n == 1 {
		return true
	}

	f1 := 1
	f2 := 1
	f3 := f1 + f2

	for f3 < n {
		f1 = f2
		f2 = f3
		f3 = f1 + f2
	}

	return f3 == n
}
