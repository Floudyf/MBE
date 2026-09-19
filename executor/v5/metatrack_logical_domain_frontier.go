package v5

import (
	"fmt"
	"sort"
	"strings"

	"metaverse-chainlab/executor/realism/tx"
)

const metaTrackLogicalDomainFrontierPolicy = "logical_domain_frontier_v1"

type metaTrackFrontierSeal struct {
	TxID           string                      `json:"tx_id"`
	LogicalDomains []string                    `json:"logical_domains"`
	Dependencies   []string                    `json:"dependencies"`
	StateVersions  []tx.StateVersionDependency `json:"state_versions"`
	FrontierDigest string                      `json:"frontier_digest"`
	SealDigest     string                      `json:"seal_digest"`
}

type metaTrackExecutionCapsule struct {
	TxID           string                      `json:"tx_id"`
	LogicalDomains []string                    `json:"logical_domains"`
	StateVersions  []tx.StateVersionDependency `json:"state_versions"`
	Predecessors   []string                    `json:"predecessors"`
	FrontierDigest string                      `json:"frontier_digest"`
	SealDigest     string                      `json:"seal_digest"`
	CapsuleDigest  string                      `json:"capsule_digest"`
}

func buildMetaTrackFrontierSeal(item tx.SignedTransaction, dependencies []string) (metaTrackFrontierSeal, error) {
	domains, _, _, ok := metaTrackLogicalDomainBinding(item)
	if !ok || item.ExecutionRouting == nil || strings.TrimSpace(item.ExecutionRouting.FrontierDigest) == "" {
		return metaTrackFrontierSeal{}, fmt.Errorf("metatrack frontier seal requires signed logical-domain frontier for %s", txIdentifier(item))
	}
	deps := append([]string(nil), dependencies...)
	sort.Strings(deps)
	deps = uniqueStrings(deps)
	seal := metaTrackFrontierSeal{
		TxID:           txIdentifier(item),
		LogicalDomains: append([]string(nil), domains...),
		Dependencies:   deps,
		StateVersions:  append([]tx.StateVersionDependency(nil), item.ExecutionRouting.StateVersions...),
		FrontierDigest: item.ExecutionRouting.FrontierDigest,
	}
	seal.SealDigest = stableJSONDigest(struct {
		TxID           string                      `json:"tx_id"`
		LogicalDomains []string                    `json:"logical_domains"`
		Dependencies   []string                    `json:"dependencies"`
		StateVersions  []tx.StateVersionDependency `json:"state_versions"`
		FrontierDigest string                      `json:"frontier_digest"`
	}{
		TxID: seal.TxID, LogicalDomains: seal.LogicalDomains, Dependencies: seal.Dependencies,
		StateVersions: seal.StateVersions, FrontierDigest: seal.FrontierDigest,
	})
	return seal, nil
}

func buildMetaTrackExecutionCapsule(item tx.SignedTransaction, seal metaTrackFrontierSeal) (metaTrackExecutionCapsule, error) {
	_, _, bridge, ok := metaTrackLogicalDomainBinding(item)
	if !ok || !bridge {
		return metaTrackExecutionCapsule{}, fmt.Errorf("metatrack execution capsule requires bridge transaction %s", txIdentifier(item))
	}
	capsule := metaTrackExecutionCapsule{
		TxID:           seal.TxID,
		LogicalDomains: append([]string(nil), seal.LogicalDomains...),
		StateVersions:  append([]tx.StateVersionDependency(nil), seal.StateVersions...),
		Predecessors:   append([]string(nil), seal.Dependencies...),
		FrontierDigest: seal.FrontierDigest,
		SealDigest:     seal.SealDigest,
	}
	capsule.CapsuleDigest = stableJSONDigest(struct {
		TxID           string                      `json:"tx_id"`
		LogicalDomains []string                    `json:"logical_domains"`
		StateVersions  []tx.StateVersionDependency `json:"state_versions"`
		Predecessors   []string                    `json:"predecessors"`
		FrontierDigest string                      `json:"frontier_digest"`
		SealDigest     string                      `json:"seal_digest"`
	}{
		TxID: capsule.TxID, LogicalDomains: capsule.LogicalDomains, StateVersions: capsule.StateVersions,
		Predecessors: capsule.Predecessors, FrontierDigest: capsule.FrontierDigest, SealDigest: capsule.SealDigest,
	})
	return capsule, nil
}

func metaTrackBridgeWaveStats(items []tx.SignedTransaction, dependencies map[string][]string) (waveCount, maxWidth int) {
	bridge := map[string]bool{}
	for _, item := range items {
		_, _, isBridge, ok := metaTrackLogicalDomainBinding(item)
		if ok && isBridge {
			bridge[txIdentifier(item)] = true
		}
	}
	if len(bridge) == 0 {
		return 0, 0
	}
	remaining := map[string]int{}
	reverse := map[string][]string{}
	for txID := range bridge {
		for _, dep := range dependencies[txID] {
			if bridge[dep] {
				remaining[txID]++
				reverse[dep] = append(reverse[dep], txID)
			}
		}
	}
	done := map[string]bool{}
	for len(done) < len(bridge) {
		ready := []string{}
		for txID := range bridge {
			if !done[txID] && remaining[txID] == 0 {
				ready = append(ready, txID)
			}
		}
		sort.Strings(ready)
		if len(ready) == 0 {
			return waveCount, maxWidth
		}
		waveCount++
		if len(ready) > maxWidth {
			maxWidth = len(ready)
		}
		for _, txID := range ready {
			done[txID] = true
			for _, next := range reverse[txID] {
				remaining[next]--
			}
		}
	}
	return waveCount, maxWidth
}

func metaTrackLogicalDomainPolicyEnabled(config map[string]any) bool {
	if config == nil {
		return false
	}
	return strings.TrimSpace(fmt.Sprint(config["control_policy"])) == metaTrackLogicalDomainFrontierPolicy
}

func metaTrackLogicalDomainBinding(item tx.SignedTransaction) (domains []string, local, bridge bool, ok bool) {
	if item.ExecutionRouting == nil || item.ExecutionRouting.ControlPolicy != metaTrackLogicalDomainFrontierPolicy {
		return nil, false, false, false
	}
	domains = append([]string(nil), item.ExecutionRouting.LogicalDomains...)
	sort.Strings(domains)
	domains = uniqueStrings(domains)
	if len(domains) == 0 {
		return domains, false, false, false
	}
	local = len(domains) == 1 && item.ExecutionRouting.Local && !item.ExecutionRouting.Bridge
	bridge = len(domains) > 1 && item.ExecutionRouting.Bridge && !item.ExecutionRouting.Local
	return domains, local, bridge, local || bridge
}

func logicalDomainID(index int) string {
	return fmt.Sprintf("d%04d", index)
}

func logicalDomainHost(domain string, shardIDs []string) string {
	if len(shardIDs) == 0 {
		return ""
	}
	var index int
	if _, err := fmt.Sscanf(domain, "d%d", &index); err != nil || index < 0 {
		index = stableKey([]string{domain}) % len(shardIDs)
	}
	return shardIDs[index%len(shardIDs)]
}

func coaccessLocalityGainForDomain(key, candidate string, edges []CoaccessEdge, placed map[string]StatePlacement) int {
	gain := 0
	for _, edge := range edges {
		neighbor := ""
		switch key {
		case edge.LeftKey:
			neighbor = edge.RightKey
		case edge.RightKey:
			neighbor = edge.LeftKey
		}
		if neighbor == "" {
			continue
		}
		if placement, ok := placed[neighbor]; ok && placement.LogicalDomain == candidate {
			gain += edge.Weight
		}
	}
	return gain
}

func logicalDomainsForAccesses(accesses []tx.AccessItem, placement map[string]StatePlacement) []string {
	set := map[string]bool{}
	for _, access := range accesses {
		if access.Key == "" {
			continue
		}
		if row, ok := placement[access.Key]; ok && row.LogicalDomain != "" {
			set[row.LogicalDomain] = true
		}
	}
	domains := make([]string, 0, len(set))
	for domain := range set {
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	return domains
}

func logicalFrontierDigest(record WorkloadRecord, domains []string) string {
	return stableJSONDigest(struct {
		LogicalID      string                      `json:"logical_id"`
		AccessDigest   string                      `json:"access_digest"`
		LogicalDomains []string                    `json:"logical_domains"`
		StateVersions  []tx.StateVersionDependency `json:"state_versions"`
		ControlPolicy  string                      `json:"control_policy"`
	}{
		LogicalID:      firstNonEmpty(record.LogicalID, fmt.Sprintf("tx-%d", record.Index)),
		AccessDigest:   firstNonEmpty(record.SchedulingAccessDigest, record.AccessListDigest),
		LogicalDomains: append([]string(nil), domains...),
		StateVersions:  append([]tx.StateVersionDependency(nil), record.StateVersions...),
		ControlPolicy:  metaTrackLogicalDomainFrontierPolicy,
	})
}

func (p *metaTrackRouting) planLogicalDomainFrontierV1(input BatchRoutingInput) BatchRoutingPlan {
	domainCount := intValue(p.config["logical_domain_count"])
	if domainCount <= 0 {
		domainCount = 4
	}
	if domainCount < 1 {
		domainCount = 1
	}
	plan := BatchRoutingPlan{
		BatchIndex:            input.BatchIndex,
		ShardingPluginID:      shardingPluginID(input.Sharding),
		StateStorageUnitCount: p.StateStorageUnitCount(input.ShardIDs),
		ControlPolicy:         metaTrackLogicalDomainFrontierPolicy,
		LogicalDomainCount:    domainCount,
		PlacementPolicy:       "logical_domain_coaccess_affinity_v1",
		TransactionPolicy:     "local_bridge_frontier_v1",
		PlacementMu:           "1.0",
		PlacementMinBudget:    1,
		ShardLoadBefore:       map[string]int{},
		ShardLoadAfter:        map[string]int{},
	}
	if len(input.ShardIDs) == 0 {
		return plan
	}
	for _, shard := range input.ShardIDs {
		plan.ShardLoadBefore[shard] = 0
		plan.ShardLoadAfter[shard] = 0
	}

	frequency := map[string]*StateFrequencyRow{}
	coaccess := map[string]int{}
	for _, record := range input.Records {
		accesses := normalizedAccessItems(record)
		keys := make([]string, 0, len(accesses))
		seen := map[string]bool{}
		for _, access := range accesses {
			key := strings.TrimSpace(access.Key)
			if key == "" {
				continue
			}
			plan.AccessMatrix = append(plan.AccessMatrix, AccessMatrixRow{
				LogicalID: firstNonEmpty(record.LogicalID, fmt.Sprintf("tx-%d", record.Index)),
				TxIndex:   record.Index,
				Key:       key,
				Mode:      access.Mode,
			})
			row := frequency[key]
			if row == nil {
				row = &StateFrequencyRow{Key: key}
				frequency[key] = row
			}
			row.Frequency++
			if isReadMode(access.Mode) {
				row.ReadCount++
			}
			if isWriteMode(access.Mode) {
				row.WriteCount++
			}
			if !seen[key] {
				seen[key] = true
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for left := 0; left < len(keys); left++ {
			for right := left + 1; right < len(keys); right++ {
				coaccess[keyPair(keys[left], keys[right])]++
			}
		}
	}

	for _, row := range frequency {
		plan.StateFrequency = append(plan.StateFrequency, *row)
		plan.PlacementTotalFrequency += row.Frequency
		if row.Frequency > plan.PlacementMaxFrequency {
			plan.PlacementMaxFrequency = row.Frequency
		}
	}
	sort.Slice(plan.StateFrequency, func(i, j int) bool {
		if plan.StateFrequency[i].Frequency != plan.StateFrequency[j].Frequency {
			return plan.StateFrequency[i].Frequency > plan.StateFrequency[j].Frequency
		}
		if plan.StateFrequency[i].WriteCount != plan.StateFrequency[j].WriteCount {
			return plan.StateFrequency[i].WriteCount > plan.StateFrequency[j].WriteCount
		}
		return plan.StateFrequency[i].Key < plan.StateFrequency[j].Key
	})
	for pair, weight := range coaccess {
		left, right, _ := strings.Cut(pair, "\x00")
		plan.CoaccessEdges = append(plan.CoaccessEdges, CoaccessEdge{LeftKey: left, RightKey: right, Weight: weight})
	}
	sort.Slice(plan.CoaccessEdges, func(i, j int) bool {
		if plan.CoaccessEdges[i].LeftKey != plan.CoaccessEdges[j].LeftKey {
			return plan.CoaccessEdges[i].LeftKey < plan.CoaccessEdges[j].LeftKey
		}
		return plan.CoaccessEdges[i].RightKey < plan.CoaccessEdges[j].RightKey
	})

	domains := make([]string, domainCount)
	domainLoad := map[string]int{}
	for i := 0; i < domainCount; i++ {
		domains[i] = logicalDomainID(i)
		domainLoad[domains[i]] = 0
	}
	capacity := 0
	if domainCount > 0 {
		capacity = (plan.PlacementTotalFrequency+domainCount-1)/domainCount + plan.PlacementMaxFrequency
	}
	plan.PlacementCapacity = capacity
	placementByKey := map[string]StatePlacement{}
	for _, row := range plan.StateFrequency {
		best := ""
		bestAffinity := -1
		for _, domain := range domains {
			projected := domainLoad[domain] + row.Frequency
			admissible := capacity <= 0 || projected <= capacity
			if !admissible {
				continue
			}
			affinity := coaccessLocalityGainForDomain(row.Key, domain, plan.CoaccessEdges, placementByKey)
			if best == "" || affinity > bestAffinity ||
				(affinity == bestAffinity && domainLoad[domain] < domainLoad[best]) ||
				(affinity == bestAffinity && domainLoad[domain] == domainLoad[best] && domain < best) {
				best = domain
				bestAffinity = affinity
			}
		}
		if best == "" {
			best = domains[0]
			for _, domain := range domains[1:] {
				if domainLoad[domain] < domainLoad[best] ||
					(domainLoad[domain] == domainLoad[best] && domain < best) {
					best = domain
				}
			}
		}
		home := p.LogicalStateHome(row.Key, input.Sharding, input.ShardIDs)
		host := logicalDomainHost(best, input.ShardIDs)
		placement := StatePlacement{
			Key:            row.Key,
			HomeStateUnit:  home.StateUnitID,
			HomeShard:      home.ServingShard,
			ExecutionShard: host,
			LogicalDomain:  best,
			Frequency:      row.Frequency,
			Reason:         "logical_domain_coaccess_affinity",
		}
		placementByKey[row.Key] = placement
		plan.StatePlacements = append(plan.StatePlacements, placement)
		domainLoad[best] += row.Frequency
		plan.ShardLoadAfter[host] += row.Frequency
	}
	sort.Slice(plan.StatePlacements, func(i, j int) bool { return plan.StatePlacements[i].Key < plan.StatePlacements[j].Key })

	routingEpoch := uint64(maxInt(0, intValue(p.config["routing_epoch"])))
	txHostLoad := map[string]int{}
	for _, record := range input.Records {
		accesses := normalizedAccessItems(record)
		txDomains := logicalDomainsForAccesses(accesses, placementByKey)
		local := len(txDomains) == 1
		bridge := len(txDomains) > 1
		executionShard := ""
		if len(txDomains) > 0 {
			candidates := map[string]bool{}
			for _, domain := range txDomains {
				candidates[logicalDomainHost(domain, input.ShardIDs)] = true
			}
			hosts := make([]string, 0, len(candidates))
			for host := range candidates {
				hosts = append(hosts, host)
			}
			sort.Strings(hosts)
			if len(hosts) > 0 {
				executionShard = hosts[0]
				for _, host := range hosts[1:] {
					if txHostLoad[host] < txHostLoad[executionShard] ||
						(txHostLoad[host] == txHostLoad[executionShard] && host < executionShard) {
						executionShard = host
					}
				}
			}
		}
		if executionShard == "" {
			executionShard = firstNonEmpty(record.SourceShard, shardFor(input.Sharding, record.StateKeys, input.ShardIDs))
		}
		txHostLoad[executionShard]++
		remoteReads, remoteWrites := metaTrackPredictedRemoteAccessCounts(p, input.Sharding, input.ShardIDs, accesses, executionShard)
		remote := remoteReads + remoteWrites
		plan.RemoteAccessEstimate += remote
		reason := "logical_domain_unbound"
		switch {
		case local:
			reason = "logical_domain_local:" + txDomains[0]
		case bridge:
			reason = "logical_domain_bridge:" + strings.Join(txDomains, "+")
		}
		plan.TransactionPlacements = append(plan.TransactionPlacements, TransactionPlacement{
			LogicalID:             firstNonEmpty(record.LogicalID, fmt.Sprintf("tx-%d", record.Index)),
			TxIndex:               record.Index,
			SenderGroupID:         metaTrackSenderGroupID(record),
			RoutingEpoch:          routingEpoch,
			HomeShard:             firstNonEmpty(record.SourceShard, executionShard),
			ExecutionShard:        executionShard,
			TargetShard:           record.TargetShard,
			CoaccessGroup:         strings.Join(txDomains, "+"),
			LogicalDomains:        append([]string(nil), txDomains...),
			Local:                 local,
			Bridge:                bridge,
			FrontierDigest:        logicalFrontierDigest(record, txDomains),
			Reason:                reason,
			PredictedRemoteReads:  remoteReads,
			PredictedRemoteWrites: remoteWrites,
			RemoteAccessCount:     remote,
		})
	}
	sort.Slice(plan.TransactionPlacements, func(i, j int) bool {
		return plan.TransactionPlacements[i].TxIndex < plan.TransactionPlacements[j].TxIndex
	})
	plan.RoutingOverhead = len(plan.CoaccessEdges) + plan.RemoteAccessEstimate
	plan.PlanDigest = routingPlanDigest(plan)
	return plan
}
