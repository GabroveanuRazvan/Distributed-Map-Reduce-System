package Utils

// BoolToInt converts a bool to an integer.
func BoolToInt(predicate bool) int {
	if predicate {
		return 1
	}

	return 0
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
