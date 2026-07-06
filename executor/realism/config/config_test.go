package config

import "testing"

func TestGenerateConfigAndAddressTable(t *testing.T) {
	cfg, err := Generate(3, 2, "runs")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Nodes) != 3 || cfg.Nodes[2].ShardID != "s0" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	table := BuildAddressTable(cfg)
	if table.RealP2P || len(table.Entries) != 3 {
		t.Fatalf("unexpected address table: %+v", table)
	}
}
