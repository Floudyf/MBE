# V5 Dataset Adapter Contract

V5 formal runs consume a `WorkloadSourceRef` and a `DatasetManifest` through a
streaming `WorkloadSession`. The session exposes cursor, checkpoint,
backpressure, and close semantics. A `CanonicalTraceRecord` is adapted to a
signed transaction through a `CanonicalToSignedTransactionAdapter` interface.

The only runnable source in this stage is `synthetic`. The planned sources
`canonical_trace`, `external_dataset`, `derived_trace`, and `existing_trace`
must return `blocked` when no adapter is installed. They must not silently fall
back to synthetic data.

This contract does not read Decentraland data, generate Zipf or hotspot traces,
copy external datasets, or use `ReadAll` as the formal streaming entry point.
Decentraland dataset integration remains false for V5.2.
