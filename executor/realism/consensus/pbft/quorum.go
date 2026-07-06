package pbft

func FaultTolerance(n int) int {
	if n <= 0 {
		return 0
	}
	return (n - 1) / 3
}

func Quorum(n int) int {
	return 2*FaultTolerance(n) + 1
}
