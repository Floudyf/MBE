package v3runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type NodeProcessPreviewInput struct {
	NodeID       string
	Role         string
	ShardID      int
	HasShardID   bool
	TopologyFile string
	OutputDir    string
	StatusFile   string
	LogFile      string
	PreviewOnly  bool
}

type NodeProcessPreviewResult struct {
	Node      NodeAddressEntry
	StatusCSV string
	Manifest  string
	LogFile   string
}

type topologyPreviewDocument struct {
	Stage        string             `json:"stage"`
	RuntimeTruth string             `json:"runtime_truth"`
	Nodes        []NodeAddressEntry `json:"nodes"`
}

func RunNodeProcessPreview(input NodeProcessPreviewInput) (NodeProcessPreviewResult, error) {
	if input.NodeID == "" {
		return NodeProcessPreviewResult{}, fmt.Errorf("node-id is required")
	}
	if input.TopologyFile == "" {
		return NodeProcessPreviewResult{}, fmt.Errorf("topology-file is required")
	}
	if !input.PreviewOnly {
		return NodeProcessPreviewResult{}, fmt.Errorf("node process runtime currently requires preview-only mode")
	}
	doc, err := loadTopologyPreview(input.TopologyFile)
	if err != nil {
		return NodeProcessPreviewResult{}, err
	}
	node, ok := findTopologyNode(doc.Nodes, input.NodeID)
	if !ok {
		return NodeProcessPreviewResult{}, fmt.Errorf("node-id %s not found in topology", input.NodeID)
	}
	if input.Role != "" && input.Role != node.Role {
		return NodeProcessPreviewResult{}, fmt.Errorf("role mismatch for %s: got %s want %s", input.NodeID, input.Role, node.Role)
	}
	if input.HasShardID && input.ShardID != node.ShardID {
		return NodeProcessPreviewResult{}, fmt.Errorf("shard mismatch for %s: got %d want %d", input.NodeID, input.ShardID, node.ShardID)
	}

	outputDir := input.OutputDir
	if outputDir == "" {
		outputDir = filepath.Dir(input.TopologyFile)
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return NodeProcessPreviewResult{}, err
	}
	statusFile := input.StatusFile
	if statusFile == "" {
		statusFile = filepath.Join(outputDir, "node_process_status.csv")
	}
	manifestFile := filepath.Join(outputDir, "node_process_manifest.json")
	logFile := input.LogFile
	if logFile == "" {
		logFile = filepath.Join(outputDir, "node_process_log_sample.log")
	}
	if err := writeNodeProcessStatusCSV(statusFile, input.TopologyFile, node, input.PreviewOnly, "preview_ready", "local node process preview completed"); err != nil {
		return NodeProcessPreviewResult{}, err
	}
	if err := writeNodeProcessManifest(manifestFile); err != nil {
		return NodeProcessPreviewResult{}, err
	}
	if err := writeNodeProcessLog(logFile, input.TopologyFile, node, input.PreviewOnly); err != nil {
		return NodeProcessPreviewResult{}, err
	}
	return NodeProcessPreviewResult{Node: node, StatusCSV: statusFile, Manifest: manifestFile, LogFile: logFile}, nil
}

func loadTopologyPreview(path string) (topologyPreviewDocument, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return topologyPreviewDocument{}, err
	}
	var doc topologyPreviewDocument
	if err := json.Unmarshal(bytes, &doc); err != nil {
		return topologyPreviewDocument{}, err
	}
	if len(doc.Nodes) == 0 {
		return topologyPreviewDocument{}, fmt.Errorf("topology contains no nodes")
	}
	return doc, nil
}

func findTopologyNode(nodes []NodeAddressEntry, nodeID string) (NodeAddressEntry, bool) {
	for _, node := range nodes {
		if node.NodeID == nodeID {
			return node, true
		}
	}
	return NodeAddressEntry{}, false
}

func writeNodeProcessStatusCSV(path string, topologyFile string, node NodeAddressEntry, previewOnly bool, status string, message string) error {
	fields := []string{"node_id", "role", "shard_id", "node_index", "logical_address", "preview_host", "preview_port", "topology_file", "preview_only", "status", "message"}
	rows := [][]string{{
		node.NodeID,
		node.Role,
		strconv.Itoa(node.ShardID),
		strconv.Itoa(node.NodeIndex),
		node.LogicalAddress,
		node.PreviewHost,
		strconv.Itoa(node.PreviewPort),
		topologyFile,
		strconv.FormatBool(previewOnly),
		status,
		message,
	}}
	return writeCSV(path, fields, rows)
}

func writeNodeProcessManifest(path string) error {
	payload := map[string]any{
		"stage":                      "V3.6.1",
		"runtime_truth":              "localhost_tcp_typed_message_preview_not_real_pbft",
		"preview_only":               true,
		"tcp_preview_only":           true,
		"not_production_network":     true,
		"not_real_pbft":              true,
		"not_blockemulator_backend":  true,
		"typed_message_preview_only": true,
		"node_process_entrypoint":    true,
	}
	bytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, bytes, 0o644)
}

func writeNodeProcessLog(path string, topologyFile string, node NodeAddressEntry, previewOnly bool) error {
	lines := []string{
		"stage=V3.6.1 NetworkAdapter TCP Typed Message Preview",
		"node_id=" + node.NodeID,
		"role=" + node.Role,
		"shard_id=" + strconv.Itoa(node.ShardID),
		"node_index=" + strconv.Itoa(node.NodeIndex),
		"logical_address=" + node.LogicalAddress,
		"preview_host=" + node.PreviewHost,
		"preview_port=" + strconv.Itoa(node.PreviewPort),
		"network_adapter=" + firstNonEmpty(node.NetworkAdapter, node.NetworkMode),
		"topology_file=" + topologyFile,
		"preview_only=" + strconv.FormatBool(previewOnly),
		"truth_boundary=localhost TCP typed message preview only; not production networking; not real PBFT; not BlockEmulator backend",
		"status=preview_ready",
	}
	return os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644)
}
