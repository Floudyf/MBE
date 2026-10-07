from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_shared_compact_transport_is_method_neutral_and_fail_closed():
    src = read("executor/v5/pbft_compact_transport_v1.go")
    assert "MBE_SHARED_PBFT_COMPACT_V1" in src
    assert 'pbftCompactTxRequestMessage  = "PBFT_TX_BODY_REQUEST"' in src
    assert 'pbftCompactTxResponseMessage = "PBFT_TX_BODY_RESPONSE"' in src
    assert "wire.Block.TxList = nil" in src
    assert "realblock.Hash(pre.Block) != pre.BlockHash" in src
    assert "realblock.TxRoot(pre.Block.TxIDs) != pre.Block.TxRoot" in src
    assert "r.pool.LookupMany(pre.Block.TxIDs)" in src
    assert "tx.Verify(item)" in src
    assert "return r.handlePBFTPrePrepare(ctx, envelope)" in src
    lower = src.lower()
    for method_name in ("metatrack_latest", "optme_execution", "txallo", "porygon_block_producer"):
        assert method_name not in lower


def test_pbft_wire_path_compacts_only_network_preprepare_and_hydrates_before_validation():
    src = read("executor/v5/runtime_pbft.go")
    handler = src.split("func (r *NodeRuntime) handlePBFTPrePrepare", 1)[1].split("func (r *NodeRuntime) handlePBFTPrepare", 1)[0]
    assert "hydratePBFTCompactPrePrepare" in handler
    assert handler.index("hydratePBFTCompactPrePrepare") < handler.index("validateConsensusBlockBody(pre.Block)")
    broadcaster = src.split("func (r *NodeRuntime) broadcastPBFTPrePrepare", 1)[1].split("func (r *NodeRuntime)", 1)[0]
    assert "compactPBFTPrePrepareForWire(pre)" in broadcaster
    assert "wirePre" in broadcaster
    # Local leader PBFT still consumes the complete block before the network copy is compacted.
    begin = src.split("func (r *NodeRuntime) beginPBFTProposal", 1)[1].split("func (r *NodeRuntime) broadcastPBFTPrePrepare", 1)[0]
    assert "state.OnPrePrepare(pre)" in begin
    compact = "".join(begin.split())
    assert "Block:block" in compact


def test_runtime_routes_repair_messages_and_cleans_per_runtime_state():
    src = read("executor/v5/runtime.go")
    assert "case pbftCompactTxRequestMessage:" in src
    assert "handlePBFTCompactTxRequest(ctx, msg)" in src
    assert "case pbftCompactTxResponseMessage:" in src
    assert "handlePBFTCompactTxResponse(ctx, msg)" in src
    stop = src.split("func (r *NodeRuntime) Stop() error", 1)[1].split("func ", 1)[0]
    assert "cleanupPBFTCompactRuntime(r)" in stop


def test_mempool_exposes_single_lock_exact_body_lookup():
    src = read("executor/realism/mempool/mempool.go")
    assert "MBE_PBFT_COMPACT_TRANSPORT_V1" in src
    lookup = src.split("func (m *Mempool) LookupMany", 1)[1].split("func (m *Mempool)", 1)[0]
    assert "m.mu.Lock()" in lookup and "defer m.mu.Unlock()" in lookup
    assert "m.byID[id]" in lookup
    assert "out[id] = e.tx" in lookup


def test_observability_separates_payload_control_and_repair_bytes():
    src = read("backend/app/services/v5_observability_metrics.py")
    for token in (
        "pbft_preprepare_network_bytes",
        "pbft_vote_control_network_bytes",
        "pbft_compact_tx_body_request_count",
        "pbft_compact_tx_body_response_count",
        "pbft_compact_repair_network_bytes",
    ):
        assert token in src
    # Repair messages retain PBFT_ prefix, so the existing shared classifier counts them as consensus.
    assert 'if kind.startswith("PBFT_") or kind == "BLOCK_PROPOSAL":' in src


def test_focused_go_regressions_cover_normal_repair_and_tamper_paths():
    src = read("executor/v5/pbft_compact_transport_v1_test.go")
    for name in (
        "TestPBFTCompactV1PrePrepareElidesTxBodiesOnWire",
        "TestPBFTCompactV1FollowerHydratesFromLocalMempool",
        "TestPBFTCompactV1MissingBodyRepairRestoresProposal",
        "TestPBFTCompactV1RejectsTamperedRepairBody",
    ):
        assert name in src
