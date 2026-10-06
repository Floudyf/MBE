package v5

import (
	"reflect"
	"sort"
	"testing"
)

func optmeWaveMembersEqual(got, want [][]int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		left := append([]int(nil), got[i]...)
		right := append([]int(nil), want[i]...)
		sort.Ints(left)
		sort.Ints(right)
		if !reflect.DeepEqual(left, right) {
			return false
		}
	}
	return true
}

func optmeScenario3V20() ([]optmeObservedTx, [][]int, [][]int) {
	return []optmeObservedTx{
		observed(1, []string{"2"}, []string{"1"}),
		observed(2, []string{"3"}, []string{"2"}),
		observed(3, []string{"4"}, []string{"2"}),
		observed(6, []string{"1"}, []string{"3"}),
		observed(5, []string{"4"}, []string{"4"}),
		observed(4, []string{"4"}, []string{"3"}),
	}, [][]int{{2}, {3, 6}, {4}, {5}}, [][]int{{1}}
}

func optmeScenario5V20() ([]optmeObservedTx, [][]int, [][]int) {
	return []optmeObservedTx{
		observed(1, []string{"2"}, []string{"1"}),
		observed(2, []string{"3"}, []string{"2"}),
		observed(3, []string{"4"}, []string{"2"}),
		observed(4, []string{"4"}, []string{"4"}),
		observed(5, []string{"4"}, []string{"4"}),
		observed(6, []string{"1"}, []string{"3"}),
		observed(7, []string{"4"}, []string{"4"}),
	}, [][]int{{1}, {2}, {3}, {4}}, [][]int{{5, 6}, {7}}
}

func optmeScenario6V20() ([]optmeObservedTx, [][]int, [][]int) {
	return []optmeObservedTx{
		observed(1, []string{"a", "b"}, []string{"a", "b"}),
		observed(2, []string{"c", "d"}, []string{"c", "d"}),
		observed(3, []string{"c", "b"}, []string{"c", "b"}),
	}, [][]int{{1, 2}}, [][]int{{3}}
}

func TestOptMEAuthorScenario3V20(t *testing.T) {
	in, main, abort := optmeScenario3V20()
	got := buildOptmeSchedule(in)
	if !reflect.DeepEqual(got.Sequences, main) || !reflect.DeepEqual(got.RescheduleEpochs, abort) {
		t.Fatalf("scenario3 main=%v abort=%v", got.Sequences, got.RescheduleEpochs)
	}
}

func TestOptMEAuthorScenario5V20(t *testing.T) {
	in, main, abort := optmeScenario5V20()
	got := buildOptmeSchedule(in)
	if !reflect.DeepEqual(got.Sequences, main) || !reflect.DeepEqual(got.RescheduleEpochs, abort) {
		t.Fatalf("scenario5 main=%v abort=%v", got.Sequences, got.RescheduleEpochs)
	}
}

func TestOptMEAuthorScenario6V20(t *testing.T) {
	in, main, abort := optmeScenario6V20()
	got := buildOptmeSchedule(in)
	if !reflect.DeepEqual(got.Sequences, main) || !reflect.DeepEqual(got.RescheduleEpochs, abort) {
		t.Fatalf("scenario6 main=%v abort=%v", got.Sequences, got.RescheduleEpochs)
	}
}

func TestOptMEParallelAuthorScenariosV20(t *testing.T) {
	cases := []struct {
		name  string
		in    []optmeObservedTx
		main  [][]int
		abort [][]int
	}{}
	for _, item := range []struct {
		name string
		fn   func() ([]optmeObservedTx, [][]int, [][]int)
	}{{"scenario3", optmeScenario3V20}, {"scenario5", optmeScenario5V20}, {"scenario6", optmeScenario6V20}} {
		in, main, abort := item.fn()
		cases = append(cases, struct {
			name  string
			in    []optmeObservedTx
			main  [][]int
			abort [][]int
		}{item.name, in, main, abort})
	}
	for _, tc := range cases {
		for _, workers := range []int{1, 2, 4, 8} {
			got := buildOptmeScheduleWithWorkers(tc.in, workers)
			if !optmeWaveMembersEqual(got.Sequences, tc.main) {
				t.Fatalf("%s workers=%d main=%v want=%v", tc.name, workers, got.Sequences, tc.main)
			}
			if !optmeWaveMembersEqual(got.RescheduleEpochs, tc.abort) {
				t.Fatalf("%s workers=%d abort=%v want=%v", tc.name, workers, got.RescheduleEpochs, tc.abort)
			}
		}
	}
}

func TestOptMEAbortStagesAndReorderTruthV20(t *testing.T) {
	// Author scenario 4: tx5 is the later same-address updater and is caught by
	// early detection; tx6 remains on the hierarchical/reschedule path.
	in := []optmeObservedTx{
		observed(1, []string{"2"}, []string{"1"}),
		observed(2, []string{"3"}, []string{"2"}),
		observed(3, []string{"4"}, []string{"2"}),
		observed(4, []string{"4"}, []string{"4"}),
		observed(5, []string{"4"}, []string{"4"}),
		observed(6, []string{"1"}, []string{"3"}),
	}
	got := buildOptmeScheduleWithWorkers(in, 8)
	if !reflect.DeepEqual(got.EarlyDetected, []int{5}) {
		t.Fatalf("early=%v want=[5]", got.EarlyDetected)
	}
	if !reflect.DeepEqual(got.HierarchicalAborted, []int{6}) {
		t.Fatalf("hierarchical=%v want=[6]", got.HierarchicalAborted)
	}
	if !reflect.DeepEqual(got.Rescheduled, []int{5, 6}) || !reflect.DeepEqual(got.EarlyAborted, got.Rescheduled) {
		t.Fatalf("rescheduled=%v legacy=%v", got.Rescheduled, got.EarlyAborted)
	}

	reorder := buildOptmeScheduleWithWorkers([]optmeObservedTx{
		observed(1, nil, []string{"1", "2"}),
		observed(2, []string{"2"}, []string{"1"}),
	}, 4)
	if !reflect.DeepEqual(reorder.HierarchicalAborted, []int{1}) || !reflect.DeepEqual(reorder.Reordered, []int{1}) || len(reorder.Rescheduled) != 0 {
		t.Fatalf("reorder truth hierarchical=%v reordered=%v rescheduled=%v", reorder.HierarchicalAborted, reorder.Reordered, reorder.Rescheduled)
	}
}
