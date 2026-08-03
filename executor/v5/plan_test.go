package v5

import (
	"path/filepath"
	"testing"
)

func TestPlanRoundTripPreservesArtifactContractV2(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compiled_run_plan.json")
	initial := Plan{
		PlanID: "plan", ExecutionBackend: "real_cluster", NoFallback: true,
		ArtifactContractVersion: 2,
		ArtifactContract: []ArtifactContractEntry{{
			PathPattern: "nodes/*/block_stm_summary.json", Scope: "node", PerNode: true,
			NodeIDs: []string{"n0", "n1"}, MinCount: 1, RequiredForFormalEligibility: true,
		}},
	}
	if err := SaveJSON(path, initial); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadPlan(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ArtifactContractVersion != 2 || len(loaded.ArtifactContract) != 1 {
		t.Fatalf("v2 artifact contract was not preserved: %#v", loaded)
	}
	entry := loaded.ArtifactContract[0]
	if entry.PathPattern != "nodes/*/block_stm_summary.json" || !entry.PerNode || len(entry.NodeIDs) != 2 || !entry.RequiredForFormalEligibility {
		t.Fatalf("artifact contract entry changed during round trip: %#v", entry)
	}
}
