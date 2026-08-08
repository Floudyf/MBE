package pbft

func FaultTolerance(n int) int {
	if n <= 0 {
		return 0
	}
	return (n - 1) / 3
}

// Quorum returns a strict >2/3 quorum. This agrees with 2f+1 when n=3f+1 and
// remains safe when a deployment has extra replicas (for example n=8, f=2,
// where a quorum of 6 is required for an intersection containing >f replicas).
func Quorum(n int) int {
	if n <= 0 {
		return 0
	}
	return (2*n)/3 + 1
}
