package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/p2p"
)

const (
	calvinReadResultMessage = "CALVIN_READ_RESULT_V1"
	calvinTxOutcomeMessage  = "CALVIN_TX_OUTCOME_V2"
)

type CalvinStateHomeFunc func(string) string

type CalvinReadResult struct {
	BlockHash        string            `json:"block_hash"`
	Height           uint64            `json:"height"`
	TxID             string            `json:"tx_id"`
	ExecutionShardID string            `json:"execution_shard_id"`
	SenderNodeID     string            `json:"sender_node_id"`
	ReadKeys         []string          `json:"read_keys"`
	Values           map[string]string `json:"values"`
	Digest           string            `json:"digest"`
}

type CalvinReadExchangeInput struct {
	Result                  CalvinReadResult
	RequiredReadShards      []string
	ExpectedReadKeysByShard map[string][]string
	ActiveShards            []string
	WaitForRemote           bool
	Timeout                 time.Duration
}

type CalvinReadExchangeResult struct {
	Values               map[string]string
	RemoteReadCount      int
	MessageCount         int // logical Calvin reader-partition -> writer-partition messages
	PhysicalMessageCount int // actual TCP unicasts after PBFT replica fan-out
	WaitMS               int64
}

type CalvinReadExchangeFunc func(context.Context, CalvinReadExchangeInput) (CalvinReadExchangeResult, error)

type CalvinTxOutcome struct {
	BlockHash      string   `json:"block_hash"`
	Height         uint64   `json:"height"`
	TxID           string   `json:"tx_id"`
	OutcomeShardID string   `json:"outcome_shard_id"`
	SenderNodeID   string   `json:"sender_node_id"`
	Success        bool     `json:"success"`
	Error          string   `json:"error,omitempty"`
	ExecutionCost  int64    `json:"execution_cost"`
	StateKeys      []string `json:"state_keys,omitempty"`
	Digest         string   `json:"digest"`
}

type CalvinOutcomeExchangeInput struct {
	Outcome      CalvinTxOutcome
	OutcomeShard string
	Publish      bool
	Timeout      time.Duration
}

type CalvinOutcomeExchangeResult struct {
	Outcome              CalvinTxOutcome
	MessageCount         int // logical MBE outcome broadcasts
	PhysicalMessageCount int // actual TCP unicasts after replica fan-out
	WaitMS               int64
}

type CalvinOutcomeExchangeFunc func(context.Context, CalvinOutcomeExchangeInput) (CalvinOutcomeExchangeResult, error)

type calvinReadState struct {
	mu            sync.Mutex
	results       map[string]map[string]CalvinReadResult // block|tx -> home execution shard -> result
	readSignal    map[string]chan struct{}
	outcomes      map[string]CalvinTxOutcome
	outcomeSignal map[string]chan struct{}
}

var calvinReadStates sync.Map // map[*NodeRuntime]*calvinReadState

func (r *NodeRuntime) calvinReadState() *calvinReadState {
	if value, ok := calvinReadStates.Load(r); ok {
		return value.(*calvinReadState)
	}
	created := &calvinReadState{
		results:       map[string]map[string]CalvinReadResult{},
		readSignal:    map[string]chan struct{}{},
		outcomes:      map[string]CalvinTxOutcome{},
		outcomeSignal: map[string]chan struct{}{},
	}
	actual, _ := calvinReadStates.LoadOrStore(r, created)
	return actual.(*calvinReadState)
}

func calvinReadKey(blockHash, txID string) string { return blockHash + "|" + txID }

func calvinReadResultDigest(result CalvinReadResult) string {
	projection := result
	projection.SenderNodeID = ""
	projection.Digest = ""
	projection.ReadKeys = append([]string(nil), result.ReadKeys...)
	sort.Strings(projection.ReadKeys)
	payload, _ := json.Marshal(projection)
	return stableTextDigest(string(payload))
}

func sealCalvinReadResult(result CalvinReadResult) CalvinReadResult {
	result.ReadKeys = append([]string(nil), result.ReadKeys...)
	sort.Strings(result.ReadKeys)
	result.Digest = calvinReadResultDigest(result)
	return result
}

func exactCalvinKeySet(values map[string]string, keys []string) bool {
	if len(values) != len(keys) {
		return false
	}
	seen := map[string]bool{}
	for _, key := range keys {
		if key == "" || seen[key] {
			return false
		}
		seen[key] = true
		if _, ok := values[key]; !ok {
			return false
		}
	}
	return true
}

func sameSortedStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	a := append([]string(nil), left...)
	b := append([]string(nil), right...)
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func validateCalvinReadResult(result CalvinReadResult) error {
	if strings.TrimSpace(result.BlockHash) == "" || result.Height == 0 || strings.TrimSpace(result.TxID) == "" {
		return fmt.Errorf("calvin READ_RESULT identity is incomplete")
	}
	if strings.TrimSpace(result.ExecutionShardID) == "" || strings.TrimSpace(result.SenderNodeID) == "" {
		return fmt.Errorf("calvin READ_RESULT sender identity is incomplete")
	}
	if !exactCalvinKeySet(result.Values, result.ReadKeys) {
		return fmt.Errorf("calvin READ_RESULT key/value set mismatch tx=%s shard=%s", result.TxID, result.ExecutionShardID)
	}
	if result.Digest == "" || result.Digest != calvinReadResultDigest(result) {
		return fmt.Errorf("calvin READ_RESULT digest mismatch")
	}
	return nil
}

func calvinOutcomeDigest(outcome CalvinTxOutcome) string {
	projection := outcome
	projection.SenderNodeID = ""
	projection.Digest = ""
	payload, _ := json.Marshal(projection)
	return stableTextDigest(string(payload))
}

func sealCalvinOutcome(outcome CalvinTxOutcome) CalvinTxOutcome {
	outcome.StateKeys = append([]string(nil), outcome.StateKeys...)
	outcome.Digest = calvinOutcomeDigest(outcome)
	return outcome
}

func validateCalvinOutcome(outcome CalvinTxOutcome) error {
	if strings.TrimSpace(outcome.BlockHash) == "" || outcome.Height == 0 || strings.TrimSpace(outcome.TxID) == "" {
		return fmt.Errorf("calvin outcome identity is incomplete")
	}
	if strings.TrimSpace(outcome.OutcomeShardID) == "" || strings.TrimSpace(outcome.SenderNodeID) == "" {
		return fmt.Errorf("calvin outcome sender identity is incomplete")
	}
	if outcome.Digest == "" || outcome.Digest != calvinOutcomeDigest(outcome) {
		return fmt.Errorf("calvin outcome digest mismatch")
	}
	return nil
}

func calvinOutcomeFromReceipt(blockHash string, height uint64, shardID, nodeID string, receipt execution.Receipt) CalvinTxOutcome {
	return CalvinTxOutcome{
		BlockHash: blockHash, Height: height, TxID: receipt.TxID, OutcomeShardID: shardID, SenderNodeID: nodeID,
		Success: receipt.Success, Error: receipt.Error, ExecutionCost: receipt.ExecutionCost, StateKeys: append([]string(nil), receipt.StateKeys...),
	}
}

func calvinReceiptFromOutcome(outcome CalvinTxOutcome) execution.Receipt {
	return execution.Receipt{
		TxID: outcome.TxID, BlockHash: outcome.BlockHash, Height: outcome.Height, Success: outcome.Success,
		Error: outcome.Error, ExecutionCost: outcome.ExecutionCost, StateKeys: append([]string(nil), outcome.StateKeys...),
	}
}

func calvinOutcomesEqual(left, right CalvinTxOutcome) bool {
	return left.BlockHash == right.BlockHash && left.Height == right.Height && left.TxID == right.TxID &&
		left.Success == right.Success && left.Error == right.Error && left.ExecutionCost == right.ExecutionCost &&
		sameSortedStrings(left.StateKeys, right.StateKeys)
}

func (r *NodeRuntime) calvinExecutionShardLeader(shardID string) string {
	members := []string{}
	for _, node := range r.plan.NodeConfigs {
		if effectiveExecutionShardID(node) == shardID {
			members = append(members, node.NodeID)
		}
	}
	sort.Strings(members)
	if len(members) == 0 {
		return ""
	}
	return members[0]
}

func (r *NodeRuntime) calvinNodeIDsForExecutionShards(shards []string, excludeExecutionShard string) []string {
	allowed := map[string]bool{}
	for _, shardID := range shards {
		if shardID != "" {
			allowed[shardID] = true
		}
	}
	seen := map[string]bool{}
	out := []string{}
	for _, node := range r.plan.NodeConfigs {
		executionShard := effectiveExecutionShardID(node)
		if executionShard == excludeExecutionShard || !allowed[executionShard] || node.NodeID == "" || node.NodeID == r.node.NodeID || seen[node.NodeID] {
			continue
		}
		seen[node.NodeID] = true
		out = append(out, node.NodeID)
	}
	sort.Strings(out)
	return out
}

func (r *NodeRuntime) calvinAllOtherNodeIDs() []string {
	out := []string{}
	for _, node := range r.plan.NodeConfigs {
		if node.NodeID != "" && node.NodeID != r.node.NodeID {
			out = append(out, node.NodeID)
		}
	}
	sort.Strings(out)
	return out
}

func (r *NodeRuntime) acceptCalvinReadResult(result CalvinReadResult) error {
	if err := validateCalvinReadResult(result); err != nil {
		return err
	}
	leader := r.calvinExecutionShardLeader(result.ExecutionShardID)
	if leader == "" || result.SenderNodeID != leader {
		return fmt.Errorf("calvin READ_RESULT sender %s is not leader of execution shard %s", result.SenderNodeID, result.ExecutionShardID)
	}
	key := calvinReadKey(result.BlockHash, result.TxID)
	state := r.calvinReadState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.results[key] == nil {
		state.results[key] = map[string]CalvinReadResult{}
	}
	if prior, exists := state.results[key][result.ExecutionShardID]; exists {
		if prior.Digest != result.Digest {
			return fmt.Errorf("conflicting calvin READ_RESULT for tx=%s shard=%s", result.TxID, result.ExecutionShardID)
		}
		return nil
	}
	state.results[key][result.ExecutionShardID] = result
	if signal := state.readSignal[key]; signal != nil {
		close(signal)
		delete(state.readSignal, key)
	}
	return nil
}

func (r *NodeRuntime) acceptCalvinOutcome(outcome CalvinTxOutcome) error {
	if err := validateCalvinOutcome(outcome); err != nil {
		return err
	}
	leader := r.calvinExecutionShardLeader(outcome.OutcomeShardID)
	if leader == "" || outcome.SenderNodeID != leader {
		return fmt.Errorf("calvin outcome sender %s is not leader of execution shard %s", outcome.SenderNodeID, outcome.OutcomeShardID)
	}
	key := calvinReadKey(outcome.BlockHash, outcome.TxID)
	state := r.calvinReadState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if prior, exists := state.outcomes[key]; exists {
		if prior.Digest != outcome.Digest {
			return fmt.Errorf("conflicting calvin outcome for tx=%s", outcome.TxID)
		}
		return nil
	}
	state.outcomes[key] = outcome
	if signal := state.outcomeSignal[key]; signal != nil {
		close(signal)
		delete(state.outcomeSignal, key)
	}
	return nil
}

func (r *NodeRuntime) handleCalvinReadResult(ctx context.Context, msg p2p.MessageEnvelope) error {
	_ = ctx
	if r.plugins.BlockExecutor == nil || (r.plugins.BlockExecutor.ID() != calvinStatefulExecutorID && r.plugins.BlockExecutor.ID() != calvinStatelessExecutorID) {
		return fmt.Errorf("calvin READ_RESULT received by non-Calvin executor")
	}
	result, err := p2p.DecodePayload[CalvinReadResult](msg)
	if err != nil {
		return err
	}
	if msg.FromNode != result.SenderNodeID || msg.Height != result.Height {
		return fmt.Errorf("calvin READ_RESULT envelope identity mismatch")
	}
	return r.acceptCalvinReadResult(result)
}

func (r *NodeRuntime) handleCalvinOutcome(ctx context.Context, msg p2p.MessageEnvelope) error {
	_ = ctx
	if r.plugins.BlockExecutor == nil || (r.plugins.BlockExecutor.ID() != calvinStatefulExecutorID && r.plugins.BlockExecutor.ID() != calvinStatelessExecutorID) {
		return fmt.Errorf("calvin outcome received by non-Calvin executor")
	}
	outcome, err := p2p.DecodePayload[CalvinTxOutcome](msg)
	if err != nil {
		return err
	}
	if msg.FromNode != outcome.SenderNodeID || msg.Height != outcome.Height {
		return fmt.Errorf("calvin outcome envelope identity mismatch")
	}
	return r.acceptCalvinOutcome(outcome)
}

func calvinWaitSignal(ctx context.Context, signal <-chan struct{}, timeout time.Duration, timeoutErr func() error) error {
	if timeout <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-signal:
			return nil
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-signal:
		return nil
	case <-timer.C:
		return timeoutErr()
	}
}

func (r *NodeRuntime) calvinReadExchange(ctx context.Context, input CalvinReadExchangeInput) (CalvinReadExchangeResult, error) {
	out := CalvinReadExchangeResult{Values: map[string]string{}}
	if r.plugins.BlockExecutor == nil || (r.plugins.BlockExecutor.ID() != calvinStatefulExecutorID && r.plugins.BlockExecutor.ID() != calvinStatelessExecutorID) {
		return out, fmt.Errorf("calvin READ_RESULT exchange invoked for non-Calvin executor")
	}
	localShard := effectiveExecutionShardID(r.node)
	local := input.Result
	local.ExecutionShardID = localShard
	local.SenderNodeID = r.node.NodeID
	local.ReadKeys = append([]string(nil), input.ExpectedReadKeysByShard[localShard]...)
	if !exactCalvinKeySet(local.Values, local.ReadKeys) {
		return out, fmt.Errorf("calvin local READ_RESULT incomplete tx=%s shard=%s", local.TxID, localShard)
	}
	for key, value := range local.Values {
		out.Values[key] = value
	}

	// Paper semantics: every passive reader forwards its local read values to all
	// active writers. MBE sends once from the deterministic execution-shard leader
	// so PBFT replicas do not multiply the logical READ_RESULT traffic.
	if len(local.ReadKeys) > 0 && r.calvinExecutionShardLeader(localShard) == r.node.NodeID {
		local = sealCalvinReadResult(local)
		logicalDestinations := map[string]bool{}
		for _, shardID := range input.ActiveShards {
			if shardID != "" && shardID != localShard {
				logicalDestinations[shardID] = true
			}
		}
		out.MessageCount += len(logicalDestinations)
		for _, nodeID := range r.calvinNodeIDsForExecutionShards(input.ActiveShards, localShard) {
			envelope, err := p2p.NewEnvelope(calvinReadResultMessage, r.node.NodeID, nodeID, r.node.ShardID, local.Height, r.currentPBFTView(), local.Height, local)
			if err != nil {
				return out, err
			}
			if err := r.sendToNode(ctx, nodeID, envelope); err != nil {
				return out, err
			}
			out.PhysicalMessageCount++
		}
	}
	if !input.WaitForRemote {
		return out, nil
	}

	required := map[string]bool{}
	for _, shardID := range input.RequiredReadShards {
		if shardID != "" && shardID != localShard {
			required[shardID] = true
		}
	}
	if len(required) == 0 {
		return out, nil
	}
	started := time.Now()
	key := calvinReadKey(local.BlockHash, local.TxID)
	for {
		state := r.calvinReadState()
		state.mu.Lock()
		rows := state.results[key]
		complete := true
		for shardID := range required {
			row, exists := rows[shardID]
			if !exists {
				complete = false
				continue
			}
			expected := input.ExpectedReadKeysByShard[shardID]
			if !sameSortedStrings(row.ReadKeys, expected) || !exactCalvinKeySet(row.Values, expected) {
				state.mu.Unlock()
				return out, fmt.Errorf("calvin READ_RESULT does not match declared read set tx=%s shard=%s", local.TxID, shardID)
			}
		}
		if complete {
			for shardID := range required {
				for stateKey, value := range rows[shardID].Values {
					out.Values[stateKey] = value
					out.RemoteReadCount++
				}
			}
			delete(state.results, key)
			delete(state.readSignal, key)
			state.mu.Unlock()
			out.WaitMS += time.Since(started).Milliseconds()
			return out, nil
		}
		signal := state.readSignal[key]
		if signal == nil {
			signal = make(chan struct{})
			state.readSignal[key] = signal
		}
		state.mu.Unlock()
		err := calvinWaitSignal(ctx, signal, input.Timeout, func() error {
			missing := []string{}
			state.mu.Lock()
			rows := state.results[key]
			for shardID := range required {
				if _, ok := rows[shardID]; !ok {
					missing = append(missing, shardID)
				}
			}
			state.mu.Unlock()
			sort.Strings(missing)
			return fmt.Errorf("calvin READ_RESULT wait timed out tx=%s missing=%s", local.TxID, strings.Join(missing, ","))
		})
		if err != nil {
			return out, err
		}
	}
}

func (r *NodeRuntime) calvinOutcomeExchange(ctx context.Context, input CalvinOutcomeExchangeInput) (CalvinOutcomeExchangeResult, error) {
	out := CalvinOutcomeExchangeResult{}
	if r.plugins.BlockExecutor == nil || (r.plugins.BlockExecutor.ID() != calvinStatefulExecutorID && r.plugins.BlockExecutor.ID() != calvinStatelessExecutorID) {
		return out, fmt.Errorf("calvin outcome exchange invoked for non-Calvin executor")
	}
	outcomeShard := strings.TrimSpace(input.OutcomeShard)
	if outcomeShard == "" {
		return out, fmt.Errorf("calvin outcome shard is empty")
	}
	leader := r.calvinExecutionShardLeader(outcomeShard)
	if leader == "" {
		return out, fmt.Errorf("calvin outcome shard %s has no leader", outcomeShard)
	}
	if input.Publish && r.node.NodeID == leader {
		outcome := input.Outcome
		outcome.OutcomeShardID = outcomeShard
		outcome.SenderNodeID = r.node.NodeID
		outcome = sealCalvinOutcome(outcome)
		destinations := r.calvinAllOtherNodeIDs()
		if len(destinations) > 0 {
			out.MessageCount = 1
		}
		for _, nodeID := range destinations {
			envelope, err := p2p.NewEnvelope(calvinTxOutcomeMessage, r.node.NodeID, nodeID, r.node.ShardID, outcome.Height, r.currentPBFTView(), outcome.Height, outcome)
			if err != nil {
				return out, err
			}
			if err := r.sendToNode(ctx, nodeID, envelope); err != nil {
				return out, err
			}
			out.PhysicalMessageCount++
		}
		out.Outcome = outcome
		return out, nil
	}

	started := time.Now()
	key := calvinReadKey(input.Outcome.BlockHash, input.Outcome.TxID)
	for {
		state := r.calvinReadState()
		state.mu.Lock()
		if outcome, ok := state.outcomes[key]; ok {
			delete(state.outcomes, key)
			delete(state.outcomeSignal, key)
			state.mu.Unlock()
			if outcome.OutcomeShardID != outcomeShard {
				return out, fmt.Errorf("calvin outcome shard mismatch tx=%s expected=%s got=%s", outcome.TxID, outcomeShard, outcome.OutcomeShardID)
			}
			out.Outcome = outcome
			out.WaitMS = time.Since(started).Milliseconds()
			return out, nil
		}
		signal := state.outcomeSignal[key]
		if signal == nil {
			signal = make(chan struct{})
			state.outcomeSignal[key] = signal
		}
		state.mu.Unlock()
		if err := calvinWaitSignal(ctx, signal, input.Timeout, func() error {
			return fmt.Errorf("calvin outcome wait timed out tx=%s outcome_shard=%s", input.Outcome.TxID, outcomeShard)
		}); err != nil {
			return out, err
		}
	}
}

func calvinStateHomeForKey(sharding ShardingPlugin, key string, shards []string) string {
	if len(shards) == 0 {
		return ""
	}
	// MBE state keys may already carry an explicit shard namespace (for example
	// s1::asset:7). Re-hashing that fully-qualified key could move it to a
	// different partition and make Calvin lock/read/write different homes for the
	// same durable object. Respect a prefix only when it names an execution shard
	// in this plan; otherwise retain the ordinary deterministic sharding rule.
	if separator := strings.Index(key, "::"); separator > 0 {
		prefix := key[:separator]
		if containsString(shards, prefix) {
			return prefix
		}
	}
	return shardFor(sharding, []string{key}, shards)
}

func (r *NodeRuntime) calvinStateHome(key string) string {
	return calvinStateHomeForKey(r.plugins.Sharding, key, calvinExecutionShardIDsFromPlan(r.plan))
}
