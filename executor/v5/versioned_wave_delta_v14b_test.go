package v5

import "testing"

var _ WaveDeltaExecutor = serialBlockExecutor{}
var _ WaveDeltaExecutor = blockSTMBlockExecutor{}

func TestV14BWaveDeltaExecutorCoverage(t *testing.T) {
	if _, ok := any(serialBlockExecutor{}).(WaveDeltaExecutor); !ok {
		t.Fatal("serial block executor does not implement WaveDeltaExecutor")
	}
	if _, ok := any(blockSTMBlockExecutor{}).(WaveDeltaExecutor); !ok {
		t.Fatal("Block-STM block executor does not implement WaveDeltaExecutor")
	}
}
