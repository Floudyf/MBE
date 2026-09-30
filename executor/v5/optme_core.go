package v5

import (
	"sort"
)

// optmeObservedTx is the post-consensus simulation result consumed by the
// OptME scheduling core. ReadKeys/WriteKeys are observed from execution, never
// copied from the signed AccessList. The AccessList only bounds state materialization
// in the stateless compatibility profile.
type optmeObservedTx struct {
	Index     int
	TxID      string
	ReadKeys  []string
	WriteKeys []string
}

type optmeSchedule struct {
	Sequences        [][]int
	RescheduleEpochs [][]int
	EarlyAborted     []int
	Reordered        []int
	AddressCount     int
	UnitCount        int
	MaxWidth         int
}

type optmeUnitType uint8

const (
	optmeUnitRead optmeUnitType = iota + 1
	optmeUnitWrite
)

type optmeTxNode struct {
	index      int
	txID       string
	sequence   int
	aborted    bool
	readKeys   map[string]bool
	writeKeys  map[string]bool
	writeUnits []*optmeUnit
}

func (t *optmeTxNode) sorted() bool { return t.sequence != 0 || t.aborted }
func (t *optmeTxNode) reorderable() bool {
	if len(t.writeUnits) <= 1 {
		return false
	}
	for _, u := range t.writeUnits {
		if u.degree != 0 || u.coLocated {
			return false
		}
	}
	return true
}

type optmeUnit struct {
	tx        *optmeTxNode
	typ       optmeUnitType
	address   string
	degree    int
	coLocated bool
}

type optmeReadUnits struct {
	units  []*optmeUnit
	maxSeq int
}

func (r *optmeReadUnits) sortUnits() {
	sorted := make([]*optmeUnit, 0, len(r.units))
	remaining := make([]*optmeUnit, 0, len(r.units))
	for _, u := range r.units {
		if u.tx.sorted() {
			sorted = append(sorted, u)
		} else {
			remaining = append(remaining, u)
		}
	}
	minSeq := 1
	have := false
	maxSeq := 0
	for _, u := range sorted {
		if u.tx.aborted {
			continue
		}
		seq := u.tx.sequence
		if !have || seq < minSeq {
			minSeq = seq
		}
		if !have || seq > maxSeq {
			maxSeq = seq
		}
		have = true
	}
	if !have {
		r.maxSeq = 1
		minSeq = 1
	} else {
		r.maxSeq = maxSeq
	}
	for _, u := range remaining {
		u.tx.sequence = minSeq
	}
	r.units = append(sorted, remaining...)
}
func (r *optmeReadUnits) incrementAndGetMaxSeq() int { r.maxSeq++; return r.maxSeq }

type optmeWriteUnits struct {
	units            []*optmeUnit
	maxSeq           int
	firstUpdaterFlag bool
}

func (w *optmeWriteUnits) sortUnits(reads *optmeReadUnits) {
	sorted := make([]*optmeUnit, 0, len(w.units))
	remaining := make([]*optmeUnit, 0, len(w.units))
	for _, u := range w.units {
		if u.tx.sorted() {
			sorted = append(sorted, u)
		} else {
			remaining = append(remaining, u)
		}
	}
	// Paper/source default: first-updater-wins.
	for _, u := range sorted {
		if u.tx.aborted || !u.coLocated {
			continue
		}
		if !w.firstUpdaterFlag {
			u.tx.sequence = reads.incrementAndGetMaxSeq()
			w.firstUpdaterFlag = true
		} else {
			u.tx.aborted = true
		}
	}
	for _, u := range sorted {
		if u.tx.aborted {
			continue
		}
		if u.tx.sequence < reads.maxSeq {
			u.tx.aborted = true
		}
	}
	writeSeq := reads.incrementAndGetMaxSeq()
	used := map[int]bool{}
	for _, u := range sorted {
		used[u.tx.sequence] = true
	}
	for _, u := range remaining {
		for used[writeSeq] {
			writeSeq++
		}
		u.tx.sequence = writeSeq
		used[writeSeq] = true
	}
	w.maxSeq = writeSeq
	w.units = append(sorted, remaining...)
}

type optmeAddress struct {
	address          string
	inDegree         int
	outDegree        int
	reads            optmeReadUnits
	writes           optmeWriteUnits
	firstUpdaterFlag bool
}

func (a *optmeAddress) add(u *optmeUnit) {
	if u.typ == optmeUnitRead {
		a.inDegree += u.degree
		a.reads.units = append(a.reads.units, u)
		return
	}
	if u.coLocated {
		a.firstUpdaterFlag = true
	}
	a.outDegree += u.degree
	a.writes.units = append(a.writes.units, u)
}

type optmeGraph struct {
	addresses map[string]*optmeAddress
	txs       map[int]*optmeTxNode
	aborted   []*optmeTxNode
	unitCount int
}

func newOptmeGraph() *optmeGraph {
	return &optmeGraph{addresses: map[string]*optmeAddress{}, txs: map[int]*optmeTxNode{}}
}

func uniqueSortedStrings(xs []string) []string {
	m := map[string]bool{}
	for _, x := range xs {
		if x != "" {
			m[x] = true
		}
	}
	out := make([]string, 0, len(m))
	for x := range m {
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}

func buildOptmeSchedule(observed []optmeObservedTx) optmeSchedule {
	g := newOptmeGraph()
	for _, in := range observed {
		tx := &optmeTxNode{index: in.Index, txID: in.TxID, readKeys: map[string]bool{}, writeKeys: map[string]bool{}}
		for _, k := range uniqueSortedStrings(in.ReadKeys) {
			tx.readKeys[k] = true
		}
		for _, k := range uniqueSortedStrings(in.WriteKeys) {
			tx.writeKeys[k] = true
		}
		writeUnits := make([]*optmeUnit, 0, len(tx.writeKeys))
		writeKeys := make([]string, 0, len(tx.writeKeys))
		for k := range tx.writeKeys {
			writeKeys = append(writeKeys, k)
		}
		sort.Strings(writeKeys)
		for _, k := range writeKeys {
			writeUnits = append(writeUnits, &optmeUnit{tx: tx, typ: optmeUnitWrite, address: k, coLocated: tx.readKeys[k]})
		}
		earlyAbort := false
		for _, u := range writeUnits {
			if u.coLocated {
				if a := g.addresses[u.address]; a != nil && a.firstUpdaterFlag {
					earlyAbort = true
					break
				}
			}
		}
		if earlyAbort {
			tx.aborted = true
			g.aborted = append(g.aborted, tx)
			continue
		}
		readKeys := make([]string, 0, len(tx.readKeys))
		for k := range tx.readKeys {
			readKeys = append(readKeys, k)
		}
		sort.Strings(readKeys)
		readUnits := make([]*optmeUnit, 0, len(readKeys))
		for _, k := range readKeys {
			readUnits = append(readUnits, &optmeUnit{tx: tx, typ: optmeUnitRead, address: k})
		}
		// Source _set_wr_dependencies: every write/read pair at different addresses contributes degree to both units.
		for _, w := range writeUnits {
			for _, r := range readUnits {
				if w.address != r.address {
					w.degree++
					r.degree++
				}
			}
		}
		tx.writeUnits = append(tx.writeUnits, writeUnits...)
		g.txs[tx.index] = tx
		for _, u := range append(readUnits, writeUnits...) {
			a := g.addresses[u.address]
			if a == nil {
				a = &optmeAddress{address: u.address}
				g.addresses[u.address] = a
			}
			a.add(u)
			g.unitCount++
		}
	}
	// Algorithm 1 address ranking: in-degree asc, out-degree desc, address asc.
	addresses := make([]*optmeAddress, 0, len(g.addresses))
	for _, a := range g.addresses {
		addresses = append(addresses, a)
	}
	sort.Slice(addresses, func(i, j int) bool {
		if addresses[i].inDegree != addresses[j].inDegree {
			return addresses[i].inDegree < addresses[j].inDegree
		}
		if addresses[i].outDegree != addresses[j].outDegree {
			return addresses[i].outDegree > addresses[j].outDegree
		}
		return addresses[i].address < addresses[j].address
	})
	for _, a := range addresses {
		a.reads.sortUnits()
		a.writes.sortUnits(&a.reads)
	}

	// Source reorder(): extract aborted txs from the sorted set; only write-only txs with >1 write units are reorderable.
	extracted := append([]*optmeTxNode(nil), g.aborted...)
	for idx, t := range g.txs {
		if t.aborted {
			extracted = append(extracted, t)
			delete(g.txs, idx)
		}
	}
	sort.Slice(extracted, func(i, j int) bool { return extracted[i].index < extracted[j].index })
	reordered := []int{}
	stillAborted := []*optmeTxNode{}
	for _, t := range extracted {
		if !t.reorderable() {
			stillAborted = append(stillAborted, t)
			continue
		}
		seq := 0
		seen := map[string]bool{}
		for _, u := range t.writeUnits {
			if seen[u.address] {
				continue
			}
			seen[u.address] = true
			a := g.addresses[u.address]
			if a == nil {
				continue
			}
			x := a.reads.maxSeq
			if a.writes.maxSeq > x {
				x = a.writes.maxSeq
			}
			if x > seq {
				seq = x
			}
		}
		if seq == 0 {
			seq = 1
		}
		t.aborted = false
		t.sequence = seq
		g.txs[t.index] = t
		reordered = append(reordered, t.index)
	}

	type pair struct{ idx, seq int }
	ordered := make([]pair, 0, len(g.txs))
	for idx, t := range g.txs {
		ordered = append(ordered, pair{idx, t.sequence})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].seq != ordered[j].seq {
			return ordered[i].seq < ordered[j].seq
		}
		return ordered[i].idx < ordered[j].idx
	})
	sequences := [][]int{}
	lastSeq := -1
	for _, p := range ordered {
		if p.seq != lastSeq {
			sequences = append(sequences, []int{})
			lastSeq = p.seq
		}
		sequences[len(sequences)-1] = append(sequences[len(sequences)-1], p.idx)
	}

	// Source ScheduledInfo::_schedule_aborted_txs: tx-id/index order, first epoch whose prior writes do not conflict with this tx's reads OR writes.
	sort.Slice(stillAborted, func(i, j int) bool { return stillAborted[i].index < stillAborted[j].index })
	epochWrites := []map[string]bool{}
	epochs := [][]int{}
	early := []int{}
	for _, t := range stillAborted {
		early = append(early, t.index)
		keys := map[string]bool{}
		for k := range t.readKeys {
			keys[k] = true
		}
		for k := range t.writeKeys {
			keys[k] = true
		}
		epoch := 0
		for epoch < len(epochWrites) {
			conflict := false
			for k := range keys {
				if epochWrites[epoch][k] {
					conflict = true
					break
				}
			}
			if !conflict {
				break
			}
			epoch++
		}
		if epoch == len(epochWrites) {
			epochWrites = append(epochWrites, map[string]bool{})
			epochs = append(epochs, []int{})
		}
		for k := range t.writeKeys {
			epochWrites[epoch][k] = true
		}
		epochs[epoch] = append(epochs[epoch], t.index)
	}
	maxWidth := 0
	for _, w := range sequences {
		if len(w) > maxWidth {
			maxWidth = len(w)
		}
	}
	for _, w := range epochs {
		if len(w) > maxWidth {
			maxWidth = len(w)
		}
	}
	return optmeSchedule{Sequences: sequences, RescheduleEpochs: epochs, EarlyAborted: early, Reordered: reordered, AddressCount: len(g.addresses), UnitCount: g.unitCount, MaxWidth: maxWidth}
}
