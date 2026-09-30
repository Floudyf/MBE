package v5

import (
	"reflect"
	"testing"
)

func observed(id int, reads, writes []string) optmeObservedTx {
	return optmeObservedTx{Index: id, TxID: string(rune('a' + id)), ReadKeys: reads, WriteKeys: writes}
}

func TestOptMEAuthorScenario1(t *testing.T) {
	in := []optmeObservedTx{
		observed(1, []string{"2"}, []string{"1"}),
		observed(2, []string{"3"}, []string{"2"}),
		observed(3, []string{"4"}, []string{"2"}),
		observed(4, []string{"4"}, []string{"3"}),
		observed(5, []string{"4"}, []string{"4"}),
		observed(6, []string{"1"}, []string{"3"}),
	}
	got := buildOptmeSchedule(in)
	wantMain := [][]int{{2}, {3, 4}, {5, 6}}
	wantAbort := [][]int{{1}}
	if !reflect.DeepEqual(got.Sequences, wantMain) {
		t.Fatalf("main=%v want=%v", got.Sequences, wantMain)
	}
	if !reflect.DeepEqual(got.RescheduleEpochs, wantAbort) {
		t.Fatalf("reschedule=%v want=%v", got.RescheduleEpochs, wantAbort)
	}
}

func TestOptMEAuthorScenario2(t *testing.T) {
	in := []optmeObservedTx{
		observed(1, []string{"2"}, []string{"1"}),
		observed(3, []string{"4"}, []string{"2"}),
		observed(2, []string{"3"}, []string{"2"}),
		observed(4, []string{"4"}, []string{"3"}),
		observed(5, []string{"4"}, []string{"4"}),
		observed(6, []string{"1"}, []string{"3"}),
	}
	got := buildOptmeSchedule(in)
	wantMain := [][]int{{3}, {2}, {4}, {5, 6}}
	wantAbort := [][]int{{1}}
	if !reflect.DeepEqual(got.Sequences, wantMain) {
		t.Fatalf("main=%v want=%v", got.Sequences, wantMain)
	}
	if !reflect.DeepEqual(got.RescheduleEpochs, wantAbort) {
		t.Fatalf("reschedule=%v want=%v", got.RescheduleEpochs, wantAbort)
	}
}

func TestOptMEAuthorReordering(t *testing.T) {
	in := []optmeObservedTx{
		observed(1, nil, []string{"1", "2"}),
		observed(2, []string{"2"}, []string{"1"}),
	}
	got := buildOptmeSchedule(in)
	want := [][]int{{2}, {1}}
	if !reflect.DeepEqual(got.Sequences, want) {
		t.Fatalf("main=%v want=%v", got.Sequences, want)
	}
	if len(got.RescheduleEpochs) != 0 {
		t.Fatalf("unexpected reschedule=%v", got.RescheduleEpochs)
	}
	if !reflect.DeepEqual(got.Reordered, []int{1}) {
		t.Fatalf("reordered=%v", got.Reordered)
	}
}

func TestOptMEAuthorScenario4(t *testing.T) {
	in := []optmeObservedTx{
		observed(1, []string{"2"}, []string{"1"}),
		observed(2, []string{"3"}, []string{"2"}),
		observed(3, []string{"4"}, []string{"2"}),
		observed(4, []string{"4"}, []string{"4"}),
		observed(5, []string{"4"}, []string{"4"}),
		observed(6, []string{"1"}, []string{"3"}),
	}
	got := buildOptmeSchedule(in)
	if !reflect.DeepEqual(got.Sequences, [][]int{{1}, {2}, {3}, {4}}) {
		t.Fatalf("main=%v", got.Sequences)
	}
	if !reflect.DeepEqual(got.RescheduleEpochs, [][]int{{5, 6}}) {
		t.Fatalf("reschedule=%v", got.RescheduleEpochs)
	}
}
