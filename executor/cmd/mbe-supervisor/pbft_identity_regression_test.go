package main

import (
	"encoding/json"
	"strings"
	"testing"

	"metaverse-chainlab/executor/v5"
)

func TestPreparePBFTIdentitiesKeepsPrivateKeysOutOfPlan(t *testing.T) {
	plan := v5.Plan{NodeConfigs: []v5.NodePlan{
		{NodeID: "n0", ShardID: "s0", Leader: true, Validators: []string{"n0", "n1"}},
		{NodeID: "n1", ShardID: "s0", Validators: []string{"n0", "n1"}},
	}}
	privateKeys, err := preparePBFTIdentities(&plan)
	if err != nil {
		t.Fatal(err)
	}
	if plan.PBFTIdentityScheme != v5.PBFTIdentitySchemeEd25519V1 {
		t.Fatalf("unexpected PBFT identity scheme %q", plan.PBFTIdentityScheme)
	}
	if len(plan.PBFTPublicKeys) != 2 || len(privateKeys) != 2 {
		t.Fatalf("unexpected identity material sizes public=%d private=%d", len(plan.PBFTPublicKeys), len(privateKeys))
	}
	if plan.PBFTPublicKeys["n0"] == plan.PBFTPublicKeys["n1"] {
		t.Fatal("distinct validator identities must not reuse one public key")
	}
	encodedPlan, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	for nodeID, privateKey := range privateKeys {
		if privateKey == "" {
			t.Fatalf("missing private key for %s", nodeID)
		}
		if strings.Contains(string(encodedPlan), privateKey) {
			t.Fatalf("private key for %s leaked into serialized plan", nodeID)
		}
	}
}

func TestNodePBFTEnvironmentReplacesInheritedIdentityWithExactlyOneChildKey(t *testing.T) {
	t.Setenv(v5.PBFTPrivateKeyEnv, "stale-parent-key")
	const childKey = "child-private-key"
	environment := nodePBFTEnvironment(childKey)
	count := 0
	for _, item := range environment {
		if strings.HasPrefix(strings.ToUpper(item), strings.ToUpper(v5.PBFTPrivateKeyEnv)+"=") {
			count++
			if item != v5.PBFTPrivateKeyEnv+"="+childKey {
				t.Fatalf("unexpected child PBFT identity environment entry %q", item)
			}
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one child PBFT private-key environment entry, got %d", count)
	}
}
