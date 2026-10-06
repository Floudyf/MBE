package v5

import (
	"sort"
	"sync"
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
	Sequences           [][]int
	RescheduleEpochs    [][]int
	EarlyDetected       []int
	HierarchicalAborted []int
	Reordered           []int
	Rescheduled         []int
	// EarlyAborted is retained as a legacy compatibility alias for the final
	// reschedule path. New code must use EarlyDetected/HierarchicalAborted/Rescheduled.
	EarlyAborted []int
	AddressCount int
	UnitCount    int
	MaxWidth     int
}

type optmeUnitType uint8

const (
	optmeUnitRead optmeUnitType = iota + 1
	optmeUnitWrite
)

type optmeAbortStage uint8

const (
	optmeAbortNone optmeAbortStage = iota
	optmeAbortEarlyDetection
	optmeAbortHierarchical
)

type optmeTxNode struct {
	index      int
	txID       string
	sequence   int
	aborted    bool
	abortStage optmeAbortStage
	readKeys   map[string]bool
	writeKeys  map[string]bool
	writeUnits []*optmeUnit
}

func (t *optmeTxNode) sorted() bool { return t.sequence != 0 || t.aborted }
func (t *optmeTxNode) abortEarly() {
	if t.abortStage == optmeAbortNone {
		t.abortStage = optmeAbortEarlyDetection
	}
	t.aborted = true
}
func (t *optmeTxNode) abortHierarchical() {
	if t.abortStage == optmeAbortNone {
		t.abortStage = optmeAbortHierarchical
	}
	t.aborted = true
}
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
			u.tx.abortHierarchical()
		}
	}
	for _, u := range sorted {
		if u.tx.aborted {
			continue
		}
		if u.tx.sequence < reads.maxSeq {
			u.tx.abortHierarchical()
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

// MBE_OPTME_V20_PARALLEL_ACG: author-source par_construct adaptation.
// MBE supplies the experiment worker_count in place of num_cpus::get(); each
// chunk is built independently and pairwise left.merge(right) preserves the
// source First-Updater-Wins direction.
func buildOptmeSubGraph(observed []optmeObservedTx) *optmeGraph {
	g := newOptmeGraph()
	for _, in := range observed {
		tx := &optmeTxNode{index: in.Index, txID: in.TxID, readKeys: map[string]bool{}, writeKeys: map[string]bool{}}
		for _, k := range uniqueSortedStrings(in.ReadKeys) {
			tx.readKeys[k] = true
		}
		for _, k := range uniqueSortedStrings(in.WriteKeys) {
			tx.writeKeys[k] = true
		}
		writeKeys := make([]string, 0, len(tx.writeKeys))
		for k := range tx.writeKeys {
			writeKeys = append(writeKeys, k)
		}
		sort.Strings(writeKeys)
		writeUnits := make([]*optmeUnit, 0, len(writeKeys))
		for _, k := range writeKeys {
			writeUnits = append(writeUnits, &optmeUnit{tx: tx, typ: optmeUnitWrite, address: k, coLocated: tx.readKeys[k]})
		}
		earlyAbort := false
		for _, u := range writeUnits {
			if !u.coLocated {
				continue
			}
			if a := g.addresses[u.address]; a != nil && a.firstUpdaterFlag {
				earlyAbort = true
				break
			}
		}
		if earlyAbort {
			tx.abortEarly()
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
		// Source _set_wr_dependencies: every write/read pair at different
		// addresses contributes degree to both units.
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
		}
	}
	g.recountUnits()
	return g
}

func (a *optmeAddress) merge(other *optmeAddress) {
	if a.firstUpdaterFlag && other.firstUpdaterFlag {
		// Faithful to author Address::merge(): unwind the later (right-side)
		// co-located updater and do not merge that conflicting address payload.
		for _, u := range other.reads.units {
			if u.coLocated {
				u.tx.abortEarly()
				a.inDegree += other.inDegree
				a.inDegree -= u.degree
				break
			}
		}
		for _, u := range other.writes.units {
			if u.coLocated {
				u.tx.abortEarly()
				a.inDegree += other.inDegree
				a.inDegree -= u.degree
				break
			}
		}
		return
	}
	a.inDegree += other.inDegree
	a.outDegree += other.outDegree
	a.reads.units = append(a.reads.units, other.reads.units...)
	a.writes.units = append(a.writes.units, other.writes.units...)
	a.firstUpdaterFlag = a.firstUpdaterFlag || other.firstUpdaterFlag
}

func (g *optmeGraph) merge(other *optmeGraph) {
	keys := make([]string, 0, len(other.addresses))
	for key := range other.addresses {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		right := other.addresses[key]
		if left := g.addresses[key]; left != nil {
			left.merge(right)
		} else {
			g.addresses[key] = right
		}
	}
	for idx, tx := range other.txs {
		g.txs[idx] = tx
	}
	g.aborted = append(g.aborted, other.aborted...)
	g.recountUnits()
}

func (g *optmeGraph) recountUnits() {
	count := 0
	for _, a := range g.addresses {
		count += len(a.reads.units) + len(a.writes.units)
	}
	g.unitCount = count
}

func buildOptmeGraphParallel(observed []optmeObservedTx, workers int) *optmeGraph {
	if len(observed) == 0 {
		return newOptmeGraph()
	}
	if workers < 1 {
		workers = 1
	}
	chunkSize := len(observed) / workers
	if chunkSize < 1 {
		chunkSize = 1
	}
	chunkCount := (len(observed) + chunkSize - 1) / chunkSize
	subgraphs := make([]*optmeGraph, chunkCount)
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for chunkIndex, start := 0, 0; start < len(observed); chunkIndex, start = chunkIndex+1, start+chunkSize {
		end := start + chunkSize
		if end > len(observed) {
			end = len(observed)
		}
		idx, lo, hi := chunkIndex, start, end
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			subgraphs[idx] = buildOptmeSubGraph(observed[lo:hi])
		}()
	}
	wg.Wait()

	for len(subgraphs) > 1 {
		next := make([]*optmeGraph, (len(subgraphs)+1)/2)
		wg = sync.WaitGroup{}
		for pair := 0; pair < len(next); pair++ {
			pairIndex := pair
			leftIndex := pair * 2
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				left := subgraphs[leftIndex]
				if leftIndex+1 < len(subgraphs) {
					left.merge(subgraphs[leftIndex+1])
				}
				next[pairIndex] = left
			}()
		}
		wg.Wait()
		subgraphs = next
	}
	return subgraphs[0]
}

func finalizeOptmeSchedule(g *optmeGraph) optmeSchedule {
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

	// Source reorder(): collect all aborted transactions, then rescue only the
	// author-defined write-only >1-write-unit candidates.
	extracted := append([]*optmeTxNode(nil), g.aborted...)
	for idx, tx := range g.txs {
		if tx.aborted {
			extracted = append(extracted, tx)
			delete(g.txs, idx)
		}
	}
	sort.Slice(extracted, func(i, j int) bool { return extracted[i].index < extracted[j].index })
	earlyDetectedSet := map[int]bool{}
	hierarchicalSet := map[int]bool{}
	for _, tx := range extracted {
		switch tx.abortStage {
		case optmeAbortEarlyDetection:
			earlyDetectedSet[tx.index] = true
		case optmeAbortHierarchical:
			hierarchicalSet[tx.index] = true
		}
	}
	reordered := []int{}
	stillAborted := []*optmeTxNode{}
	for _, tx := range extracted {
		if !tx.reorderable() {
			stillAborted = append(stillAborted, tx)
			continue
		}
		seq := 0
		seen := map[string]bool{}
		for _, u := range tx.writeUnits {
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
		tx.aborted = false
		tx.sequence = seq
		g.txs[tx.index] = tx
		reordered = append(reordered, tx.index)
	}

	type pair struct{ idx, seq int }
	ordered := make([]pair, 0, len(g.txs))
	for idx, tx := range g.txs {
		ordered = append(ordered, pair{idx, tx.sequence})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].seq != ordered[j].seq {
			return ordered[i].seq < ordered[j].seq
		}
		return ordered[i].idx < ordered[j].idx
	})
	sequences := [][]int{}
	lastSeq := -1
	for _, item := range ordered {
		if item.seq != lastSeq {
			sequences = append(sequences, []int{})
			lastSeq = item.seq
		}
		sequences[len(sequences)-1] = append(sequences[len(sequences)-1], item.idx)
	}

	// Source ScheduledInfo::_schedule_aborted_txs: tx-id/index order, first
	// epoch whose prior writes do not conflict with this tx's reads OR writes.
	sort.Slice(stillAborted, func(i, j int) bool { return stillAborted[i].index < stillAborted[j].index })
	epochWrites := []map[string]bool{}
	epochs := [][]int{}
	rescheduled := []int{}
	for _, tx := range stillAborted {
		rescheduled = append(rescheduled, tx.index)
		keys := map[string]bool{}
		for k := range tx.readKeys {
			keys[k] = true
		}
		for k := range tx.writeKeys {
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
		for k := range tx.writeKeys {
			epochWrites[epoch][k] = true
		}
		epochs[epoch] = append(epochs[epoch], tx.index)
	}

	toSorted := func(set map[int]bool) []int {
		out := make([]int, 0, len(set))
		for idx := range set {
			out = append(out, idx)
		}
		sort.Ints(out)
		return out
	}
	earlyDetected := toSorted(earlyDetectedSet)
	hierarchicalAborted := toSorted(hierarchicalSet)
	sort.Ints(reordered)
	sort.Ints(rescheduled)
	g.recountUnits()
	maxWidth := 0
	for _, wave := range sequences {
		if len(wave) > maxWidth {
			maxWidth = len(wave)
		}
	}
	for _, wave := range epochs {
		if len(wave) > maxWidth {
			maxWidth = len(wave)
		}
	}
	return optmeSchedule{
		Sequences:           sequences,
		RescheduleEpochs:    epochs,
		EarlyDetected:       earlyDetected,
		HierarchicalAborted: hierarchicalAborted,
		Reordered:           reordered,
		Rescheduled:         rescheduled,
		EarlyAborted:        append([]int(nil), rescheduled...),
		AddressCount:        len(g.addresses),
		UnitCount:           g.unitCount,
		MaxWidth:            maxWidth,
	}
}

func buildOptmeScheduleWithWorkers(observed []optmeObservedTx, workers int) optmeSchedule {
	if workers <= 1 {
		return finalizeOptmeSchedule(buildOptmeSubGraph(observed))
	}
	return finalizeOptmeSchedule(buildOptmeGraphParallel(observed, workers))
}

func buildOptmeSchedule(observed []optmeObservedTx) optmeSchedule {
	return buildOptmeScheduleWithWorkers(observed, 1)
}
