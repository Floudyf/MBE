"""Validated, source-byte-preserving fast previews for three V4 workloads.

The staged installer first audits every original event and stores the exact
canonical gzip digest and an optional sparse physical-CSV row index.  The
preview computes the identical canonical-hash-anchored selection as normal
materialization, then validates only that selected window.  The full-source
SHA/source semantics are ALWAYS revalidated during real materialization.
"""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
from typing import Any

DATASETS = frozenset({'axie_day_layered_v4','dcl_sales_layered_v4','tapos_layered_v4'})
POLICY = 'mv_verified_window_index_v1'


def preview_index_path(cache_root: Path, dataset_id: str) -> Path:
    if dataset_id not in DATASETS:
        raise ValueError('unsupported MV preview dataset')
    return cache_root / 'mv_preview_indexes' / f'{dataset_id}.json'


def fast_preview(*, csv_path: Path, manifest: dict[str, Any], cache_root: Path,
                 requested_tx_count: int, seed: int, variant_mode: str,
                 target_alpha: float | None, skew_axis: str | None, shards: int,
                 selection_mode: str, supported_counts: set[int] | frozenset[int] | None,
                 variant_parameters: dict[str, Any] | None) -> dict[str, Any]:
    from backend.app.services import v5_workload_data_plane as w
    dataset_id = manifest['dataset_id']
    if dataset_id not in DATASETS or str(manifest.get('preview_policy')) != POLICY:
        raise w.WorkloadDataError('MV fast-preview policy is unavailable for this dataset')
    derived = (dataset_id == 'tapos_layered_v4' and variant_mode == 'key_zipf')
    if (variant_mode != 'original_window' and not derived) or selection_mode != 'contiguous_window':
        raise w.WorkloadDataError('MV V4 indexed preview only supports original windows or Tapos key_zipf')
    if variant_mode == 'original_window' and target_alpha is not None:
        raise w.WorkloadDataError('original window does not permit target_alpha')
    source = csv_path.stat()
    index_file = preview_index_path(cache_root, dataset_id)
    if not index_file.is_file():
        raise w.WorkloadDataError('MV V4 quick preview index is missing: rerun MV_UIFix_v1 VERIFY_ONLY/APPLY')
    try:
        index = json.loads(index_file.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        raise w.WorkloadDataError('MV V4 quick preview index cannot be read: rerun install') from exc
    if (index.get('policy') != POLICY or index.get('dataset_id') != dataset_id
        or index.get('adapter_id') != manifest.get('adapter_id')
        or index.get('source_sha256') != manifest.get('source_sha256')
        or index.get('source_size_bytes') != source.st_size
        or index.get('source_mtime_ns') != source.st_mtime_ns
        or index.get('row_count') != int(manifest.get('row_count') or 0)
        or not isinstance(index.get('canonical_sha256'), str)
        or len(index['canonical_sha256']) != 64):
        raise w.WorkloadDataError('MV V4 source/index identity changed: rerun full index verification')
    spec, count, mode, axis = w._selection_spec(
        dataset_id=dataset_id, source_sha256=str(manifest['source_sha256']),
        canonical_sha256=index['canonical_sha256'], requested_tx_count=requested_tx_count,
        seed=seed, total=int(index['row_count']), variant_mode=variant_mode,
        target_alpha=target_alpha, skew_axis=skew_axis,
        selection_mode=selection_mode, supported_counts=supported_counts,
        variant_parameters=variant_parameters)
    start = w._selection_start(spec,int(index['row_count']),count)
    # Apply exactly the same canonical validation and source order contract as
    # the slow path; the only difference is bounded source iteration.
    adapter = w.adapter_for_manifest(manifest)
    selected: list[dict[str, Any]] = []
    digest = hashlib.sha256()
    previous: tuple[int,int] | None = None
    for i, item in enumerate(adapter.iter_canonical_window(csv_path, manifest, start=start,
                                  stop=start+count, sparse_offsets=index.get('csv_offsets') or [])):
        record = w._validate_canonical_record(item,dataset_id=dataset_id,row_number=start+i)
        key=(int(record['timestamp_ms']),int(record['source_row_index']))
        if previous is not None and key < previous:
            raise w.WorkloadDataError('MV quick preview selected window source order regressed')
        previous=key
        digest.update(w._canonical_bytes(record))
        selected.append(record)
    if len(selected)!=count:
        raise w.WorkloadDataError(f'MV V4 quick preview stopped early: {len(selected)} != {count}')
    if derived:
        identity = f"{dataset_id}|{manifest['source_sha256']}|{digest.hexdigest()}|{axis}|{target_alpha}|{seed}|{w.GENERATOR_VERSION}"
        selected = w._zipf_records(selected, float(target_alpha), str(axis), identity)
    preview = w._selected_window_preview(spec,selected,start=start,count=count,
        selected_start_ms=selected[0]['timestamp_ms'],
        selected_end_ms=selected[-1]['timestamp_ms'],
        base_window_sha256=digest.hexdigest(),shards=shards)
    preview['preview_policy'] = POLICY
    preview['full_source_verified_by'] = 'installer_exact_original_sha256_and_canonical_gzip_digest'
    preview['preview_window_truth'] = 'exact_same_window_as_materializer'
    return preview
