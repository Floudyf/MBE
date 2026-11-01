package v5

import "testing"

func TestMTLaneFinalV1OnlyLastCrossTrackProducer(t *testing.T) {
	fast := []string{"f0"}
	cons := []string{"c0", "c1"}
	reverse := map[string][]string{"c0": {"x"}, "c1": {"f1"}}
	pending := map[string]int{"x": 1, "f1": 2}
	fastIDs := map[string]bool{"f1": true}
	done := map[string]bool{}
	if got := metaTrackLaneInheritIndexV1(fast, cons, reverse, pending, fastIDs, done); got != -1 {
		t.Fatalf("premature promote=%d", got)
	}
	pending["f1"] = 1
	if got := metaTrackLaneInheritIndexV1(fast, cons, reverse, pending, fastIDs, done); got != 1 {
		t.Fatalf("expected conservative parent c1, got=%d", got)
	}
	done["f1"] = true
	if got := metaTrackLaneInheritIndexV1(fast, cons, reverse, pending, fastIDs, done); got != -1 {
		t.Fatalf("completed child promoted=%d", got)
	}
	done["f1"] = false
	if got := metaTrackLaneInheritIndexV1(nil, cons, reverse, pending, fastIDs, done); got != -1 {
		t.Fatalf("no fast competition promoted=%d", got)
	}
	if got := metaTrackLaneInheritIndexV1(fast, nil, reverse, pending, fastIDs, done); got != -1 {
		t.Fatalf("no conservative candidate promoted=%d", got)
	}
}

func TestMTLaneFinalV1ScanCap(t *testing.T) {
	c := make([]string, 34)
	for i := range c {
		c[i] = "c"
	}
	c[33] = "tail"
	got := metaTrackLaneInheritIndexV1([]string{"ready"}, c, map[string][]string{"tail": {"f"}}, map[string]int{"f": 1}, map[string]bool{"f": true}, nil)
	if got != -1 {
		t.Fatalf("scan cap violated: %d", got)
	}
}
