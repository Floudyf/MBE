package v5

import "fmt"

func registerLiteratureBaselinePlugins(register func(string, string, Factory)) {
	register("execution", cgExecutionID, func(config map[string]any) (Plugin, error) {
		return cgExecution{makeBasic("execution", cgExecutionID, config)}, nil
	})
	register("scheduler", cgSchedulerID, func(config map[string]any) (Plugin, error) {
		return cgScheduler{makeBasic("scheduler", cgSchedulerID, config)}, nil
	})
	register("block_executor", cgBlockExecutorID, func(config map[string]any) (Plugin, error) {
		return cgBlockExecutor{makeBasic("block_executor", cgBlockExecutorID, config)}, nil
	})

	register("execution", acgExecutionID, func(config map[string]any) (Plugin, error) {
		return acgExecution{makeBasic("execution", acgExecutionID, config)}, nil
	})
	register("scheduler", acgSchedulerID, func(config map[string]any) (Plugin, error) {
		return acgScheduler{makeBasic("scheduler", acgSchedulerID, config)}, nil
	})
	register("block_executor", acgBlockExecutorID, func(config map[string]any) (Plugin, error) {
		return acgBlockExecutor{makeBasic("block_executor", acgBlockExecutorID, config)}, nil
	})

	register("execution", bsxExecutionID, func(config map[string]any) (Plugin, error) {
		return bsxExecution{makeBasic("execution", bsxExecutionID, config)}, nil
	})
	register("scheduler", bsxSchedulerID, func(config map[string]any) (Plugin, error) {
		return bsxScheduler{makeBasic("scheduler", bsxSchedulerID, config)}, nil
	})
	register("block_executor", bsxBlockExecutorID, func(config map[string]any) (Plugin, error) {
		return bsxBlockExecutor{makeBasic("block_executor", bsxBlockExecutorID, config)}, nil
	})
}

func validateLiteratureBaselineCombination(plugins RuntimePlugins) error {
	profiles := []struct{ name, executionID, schedulerID, executorID string }{
		{"CG", cgExecutionID, cgSchedulerID, cgBlockExecutorID},
		{"ACG", acgExecutionID, acgSchedulerID, acgBlockExecutorID},
		{"BSX", bsxExecutionID, bsxSchedulerID, bsxBlockExecutorID},
	}
	for _, profile := range profiles {
		execSelected := plugins.Execution != nil && plugins.Execution.ID() == profile.executionID
		schedSelected := plugins.Scheduler != nil && plugins.Scheduler.ID() == profile.schedulerID
		blockSelected := plugins.BlockExecutor != nil && plugins.BlockExecutor.ID() == profile.executorID
		if !execSelected && !schedSelected && !blockSelected {
			continue
		}
		if !(execSelected && schedSelected && blockSelected) {
			return fmt.Errorf("%s execution, scheduler, and block executor must be selected together", profile.name)
		}
		required := []struct {
			category, id string
			plugin       Plugin
		}{
			{"routing", "hash_routing_baseline", plugins.Routing},
			{"block_producer", "time_or_count_block_producer", plugins.BlockProducer},
			{"state_access", "direct_state_access", plugins.StateAccess},
			{"state_storage", "persistent_local_state_store", plugins.StateStorage},
			{"commit", "normal_commit", plugins.Commit},
		}
		for _, item := range required {
			if item.plugin == nil || item.plugin.ID() != item.id {
				actual := "<nil>"
				if item.plugin != nil {
					actual = item.plugin.ID()
				}
				return fmt.Errorf("%s requires %s:%s, got %s", profile.name, item.category, item.id, actual)
			}
		}
	}
	return nil
}
