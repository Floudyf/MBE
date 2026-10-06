package v5

import (
	"fmt"
	"strings"

	"metaverse-chainlab/executor/realism/tx"
)

const (
	porygonAdmissionID = "porygon_access_admission"
	porygonShardingID  = "porygon_object_sharding"
)

type porygonAdmission struct{ basicPlugin }
type porygonObjectSharding struct{ basicPlugin }

func (p porygonAdmission) Admit(item tx.SignedTransaction) error {
	if err := tx.Verify(item); err != nil {
		return err
	}
	if len(item.AccessList) == 0 {
		return fmt.Errorf("porygon_access_violation: missing signed access list")
	}
	seen := map[string]bool{}
	for _, access := range item.AccessList {
		key := strings.TrimSpace(access.Key)
		if key == "" || seen[key] {
			return fmt.Errorf("porygon_access_violation: empty or duplicate access key")
		}
		seen[key] = true
		switch access.Mode {
		case tx.AccessRead, tx.AccessWrite, tx.AccessReadWrite, tx.AccessCommutativeDelta:
		default:
			return fmt.Errorf("porygon_access_violation: unsupported access mode %s for %s", access.Mode, key)
		}
	}
	if item.AccessListDigest != "" && item.AccessListDigest != CanonicalAccessListDigest(item.AccessList) {
		return fmt.Errorf("porygon_access_violation: signed access-list digest mismatch")
	}
	return nil
}

func (p porygonObjectSharding) ShardFor(keys, shards []string) string {
	if len(shards) == 0 {
		return ""
	}
	for _, key := range keys {
		owner := porygonStateOwnerIdentity(key)
		if owner != "" {
			return shards[stableKey([]string{owner})%len(shards)]
		}
	}
	return shards[stableKey(keys)%len(shards)]
}

func registerPorygonTruthPlugins(register func(string, string, Factory)) {
	register("transaction_admission", porygonAdmissionID, func(c map[string]any) (Plugin, error) {
		return porygonAdmission{makeBasic("transaction_admission", porygonAdmissionID, c)}, nil
	})
	register("sharding", porygonShardingID, func(c map[string]any) (Plugin, error) {
		return porygonObjectSharding{makeBasic("sharding", porygonShardingID, c)}, nil
	})
}

func validatePorygonTruthPluginCombination(plugins RuntimePlugins) error {
	selected := []bool{
		plugins.Admission != nil && plugins.Admission.ID() == porygonAdmissionID,
		plugins.Sharding != nil && plugins.Sharding.ID() == porygonShardingID,
		plugins.Routing != nil && plugins.Routing.ID() == porygonRoutingID,
		plugins.BlockProducer != nil && plugins.BlockProducer.ID() == porygonBlockProducerID,
		plugins.Execution != nil && plugins.Execution.ID() == porygonExecutionID,
		plugins.Scheduler != nil && plugins.Scheduler.ID() == porygonSchedulerID,
		plugins.BlockExecutor != nil && plugins.BlockExecutor.ID() == porygonBlockExecutorID,
		plugins.StateAccess != nil && plugins.StateAccess.ID() == porygonStateAccessID,
		plugins.StateStorage != nil && plugins.StateStorage.ID() == porygonStateStorageID,
		plugins.CrossShard != nil && plugins.CrossShard.ID() == porygonCrossShardID,
	}
	anySelected := false
	allSelected := true
	for _, value := range selected {
		anySelected = anySelected || value
		allSelected = allSelected && value
	}
	if !anySelected {
		return nil
	}
	if !allSelected {
		return fmt.Errorf("Porygon admission, sharding, routing, block producer, execution, scheduler, block executor, state access/storage, and cross-shard plugins must be selected together")
	}
	return nil
}

var _ AdmissionPlugin = porygonAdmission{}
var _ ShardingPlugin = porygonObjectSharding{}
