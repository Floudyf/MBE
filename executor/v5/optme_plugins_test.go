package v5

import "testing"

func TestOptMEPluginsRegisterThroughBuiltinRegistry(t *testing.T) {
	r := BuiltinRegistry()
	for _, x := range []struct{ cat, id string }{{"execution", optmeExecutionID}, {"scheduler", optmeSchedulerID}, {"block_executor", optmeStatefulExecutorID}, {"block_executor", optmeStatelessExecutorID}, {"routing", optmeStatelessRoutingID}} {
		if _, err := r.Create(x.cat, x.id, map[string]any{"worker_count": 4}); err != nil {
			t.Fatalf("%s/%s: %v", x.cat, x.id, err)
		}
	}
}

// MBE_OPTME_V22_4_LEGACY_TEST_RETIREMENT
func TestStatelessOptMERoutingUsesBlockProjectionWithoutVersionAdmission(t *testing.T) {
	p := statelessOptmeRouting{}
	if !p.StatelessDirectExecution() {
		t.Fatal("stateless direct execution capability missing")
	}
	// Routing metadata still carries placement/projection identity, but v22 must
	// never encode workload/source order as transaction Required/ProducedVersion.
	if !p.BindExecutionRoutingMetadata() {
		t.Fatal("Stateless-OptME must retain projection routing metadata")
	}
	if p.StatelessVersionAdmission() {
		t.Fatal("Stateless-OptME v22 must not perform transaction exact-version admission")
	}
	if p.NativeVersionedStateReady() {
		t.Fatal("Stateless-OptME must not import MetaTrack StateReady")
	}
	if p.SignedBatchExecutionPlan() {
		t.Fatal("Stateless-OptME block projection must not become a signed OptME execution schedule")
	}
	if got := p.BatchExecutionPlanAlgorithmID(); got != "stateless_optme_projection_v1" {
		t.Fatalf("unexpected Stateless-OptME projection algorithm id: %q", got)
	}
}

func TestStatelessOptMEDeclaresNonMetaTrackArtifactFamily(t *testing.T) {
	p := statelessOptmeRouting{}
	if !p.StatelessDirectExecution() {
		t.Fatal("Stateless-OptME must opt into the generic stateless state substrate")
	}
	if p.BatchRoutingArtifactFamily() == "metatrack" || p.BatchRoutingArtifactFamily() == "" {
		t.Fatalf("Stateless-OptME must not emit MetaTrack batch artifacts: %q", p.BatchRoutingArtifactFamily())
	}
}
