package xshard

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"time"

	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

type Result struct {
	RuntimeStage                string        `json:"runtime_stage"`
	RuntimeTruth                string        `json:"runtime_truth"`
	RealCrossShardStateMachine  bool          `json:"real_cross_shard_state_machine"`
	DeterministicCertificateMVP bool          `json:"deterministic_certificate_mvp"`
	ByzantineSecureRelay        bool          `json:"byzantine_secure_relay"`
	ProductionAtomicCommit      bool          `json:"production_atomic_commit"`
	Events                      []Event       `json:"events"`
	Certificates                []Certificate `json:"certificates"`
	RefundEvents                int           `json:"refund_events"`
	CrossShardTxCount           int           `json:"cross_shard_tx_count"`
}

func IsCrossShard(item tx.SignedTransaction) bool {
	shards := map[string]bool{}
	for _, key := range item.StateKeys {
		if len(key) >= 8 && key[:6] == "shard:" {
			shards[key[:8]] = true
		}
	}
	return len(shards) > 1
}

func RunSuccess(item tx.SignedTransaction, sourceShard, targetShard, sourceBlockHash string, sourceDB, targetDB *state.DB, outDir string) (Result, error) {
	result := baseResult()
	result.CrossShardTxCount = 1
	result.Events = append(result.Events, event(item.TxID, sourceShard, targetShard, Detected, true, ""))
	lock := event(item.TxID, sourceShard, targetShard, SourceLock, true, "")
	result.Events = append(result.Events, lock)
	cert := NewCertificate(item.TxID, sourceShard, targetShard, sourceBlockHash, lock.EventHash, sourceDB.Root())
	result.Certificates = append(result.Certificates, cert)
	result.Events = append(result.Events, event(item.TxID, sourceShard, targetShard, RelayCertificate, true, ""))
	result.Events = append(result.Events, event(item.TxID, sourceShard, targetShard, TargetVerify, true, ""))
	targetDB.Set("xshard_credit:"+item.Receiver+":"+item.TxID, fmt.Sprint(item.Value))
	result.Events = append(result.Events, event(item.TxID, sourceShard, targetShard, TargetCommit, true, ""))
	sourceDB.Set("xshard_finalized:"+item.TxID, "true")
	result.Events = append(result.Events, event(item.TxID, sourceShard, targetShard, SourceFinalize, true, ""))
	return result, writeLogs(outDir, result)
}

func RunRefund(item tx.SignedTransaction, sourceShard, targetShard string, sourceDB *state.DB, outDir string) (Result, error) {
	result := baseResult()
	result.CrossShardTxCount = 1
	result.Events = append(result.Events, event(item.TxID, sourceShard, targetShard, Detected, true, ""))
	result.Events = append(result.Events, event(item.TxID, sourceShard, targetShard, Timeout, true, "target_timeout"))
	sourceDB.Set("xshard_refund:"+item.TxID, fmt.Sprint(item.Value))
	result.Events = append(result.Events, event(item.TxID, sourceShard, targetShard, Refund, true, ""))
	result.Events = append(result.Events, event(item.TxID, sourceShard, targetShard, Abort, true, ""))
	result.RefundEvents = 1
	return result, writeLogs(outDir, result)
}

func baseResult() Result {
	return Result{RuntimeStage: "v4_2_state_cross_shard_recovery_frontend", RuntimeTruth: "v4_real_state_cross_shard_recovery", RealCrossShardStateMachine: true, DeterministicCertificateMVP: true, ByzantineSecureRelay: false, ProductionAtomicCommit: false}
}

func event(txID, sourceShard, targetShard string, stage Stage, success bool, errText string) Event {
	e := Event{Timestamp: time.Now().UnixMilli(), TxID: txID, SourceShard: sourceShard, TargetShard: targetShard, Stage: stage, Success: success, Error: errText}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%s:%s:%t:%s", txID, sourceShard, targetShard, stage, success, errText)))
	e.EventHash = hex.EncodeToString(sum[:])
	return e
}

func writeLogs(outDir string, result Result) error {
	if outDir == "" {
		return nil
	}
	stateRows := [][]string{}
	eventRows := [][]string{}
	refundRows := [][]string{}
	for _, e := range result.Events {
		row := []string{fmt.Sprint(e.Timestamp), e.TxID, e.SourceShard, e.TargetShard, string(e.Stage), fmt.Sprint(e.Success), e.EventHash, e.Error}
		stateRows = append(stateRows, row)
		eventRows = append(eventRows, row)
		if e.Stage == Refund || e.Stage == Abort || e.Stage == Timeout {
			refundRows = append(refundRows, row)
		}
	}
	if err := metrics.WriteCSV(filepath.Join(outDir, "xshard_state_log.csv"), []string{"timestamp", "tx_id", "source_shard", "target_shard", "stage", "success", "event_hash", "error"}, stateRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(outDir, "xshard_event_log.csv"), []string{"timestamp", "tx_id", "source_shard", "target_shard", "stage", "success", "event_hash", "error"}, eventRows); err != nil {
		return err
	}
	certRows := [][]string{}
	for _, cert := range result.Certificates {
		certRows = append(certRows, []string{cert.TxID, cert.SourceShard, cert.TargetShard, cert.SourceBlockHash, cert.LockEventHash, cert.StateRoot, cert.CertificateDigest, "true"})
	}
	if err := metrics.WriteCSV(filepath.Join(outDir, "relay_certificate_log.csv"), []string{"tx_id", "source_shard", "target_shard", "source_block_hash", "lock_event_hash", "state_root", "certificate_digest", "deterministic_certificate_mvp"}, certRows); err != nil {
		return err
	}
	return metrics.WriteCSV(filepath.Join(outDir, "refund_log.csv"), []string{"timestamp", "tx_id", "source_shard", "target_shard", "stage", "success", "event_hash", "error"}, refundRows)
}
