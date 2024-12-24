package Utils

// ReduceFunctionId marks the id of a reduce function.
type ReduceFunctionId = int

// This enum marks all the ids of a reduce function.
const (
	TypeReduceLeftBound ReduceFunctionId = iota
	TypeReduce1
	TypeReduceRightBound
)

// ReduceFunctionRegistry maps each reduce function id to the specific function.
var ReduceFunctionRegistry = map[ReduceFunctionId]func([]int) int{

	TypeReduce1: reduce1,
}

// reduce1 sums up the predicates.
func reduce1(predicates []int) int {

	return Sum(predicates)

}
