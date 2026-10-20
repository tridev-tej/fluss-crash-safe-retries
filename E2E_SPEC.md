# Failure specification - written before worker implementation

All seven cases require a real Apache Fluss 1.0.0 cluster and independent localhost HTTP service with a durable SQLite ledger. Inspect the matching run's emitted manifests, assertions and complete process captures to establish acceptance. `python3 scripts/e2e.py verify` is the sole E2E entry point. It requires a new evidence directory, a built Java 17 client, and a fresh lab table initialized by its actual offset probe. No unit test substitutes for these cases.

| Case | Real input or fault | Independent assertions |
|---|---|---|
| Baseline | attempt 1, synthetic ticket-1042 | fresh reader sees ordered six-event completion; one computation; one committed effect; one POST |
| Repeated computation | barrier A, SIGKILL worker PID | barrier after durable independent compute receipt, before COMPUTE_FINISHED append invocation; exit -SIGKILL; fresh attempt 2; two compute receipts; both attempts in log; one effect |
| Before external call | barrier B1, SIGKILL | ACTION_REQUESTED future completed and flush returned; no HTTP POST yet; replay unresolved; zero effects and zero POSTs before recovery; contract-bound negative lookup then one POST/effect |
| After external commit | barrier B2, SIGKILL | successful HTTP response received and receipt validated after service commit, before ACTION_CONFIRMED append invocation; worker memory lost, durable Fluss intent unresolved; one effect and one POST before recovery; receipt GET then reconciliation with no second POST; no response loss, network timeout or crash before the HTTP response is simulated |
| Unknown outcome | B2 then fixture rejects receipt lookup | attempt 2 emits HOLD_UNKNOWN; no extra POST/effect or computation; attempts 3-5 hold on ordinary 404, unknown contract and unauthorized lookup; attempt 6 after lookup restored reconciles to same receipt; no extra effect |
| Duplicate event | exact event through two writer lifetimes | two distinct real offsets, two raw rows, one logical event; changed immutable detail with same event_id causes reader failure; all three raw rows retained |
| Conflicting action | exact repeated request, then same key with changed digest | duplicate response equals original receipt; conflicting Java action exits nonzero with HTTP 409; one effect; audit records original, duplicate and conflict POSTs |

Frozen payload and receipt checks are specified before their implementation. ACTION_REQUESTED persists the complete canonical immutable request bytes and digest. A fresh recovery process must send or reconcile those exact bytes, with no extra inference invocation. Missing payload, noncanonical or tampered bytes, changed digest, invalid job/action binding, and a receipt with the wrong action ID or payload digest must each reject before any POST or successful reconciliation. Independent HTTP audit counts, computation receipts, persisted payload bytes and Fluss readback prove these conditions. Digest alone cannot reconstruct a request.

Positive frozen-payload correction case is specified before its implementation. Within the before-external-call case, seed actual Fluss ACTION_REQUESTED with a valid canonical payload whose ASCII title differs from the default Event.payload template and includes quotes and a backslash. Record the seeded PID and actual persisted detail/digest. Recover in a fresh JVM. Require the independent service's captured raw HTTP request bytes to contain the exact persisted payload bytes, without regenerating an expected request object. Also require its stored payload bytes and receipt action/digest to match, every recovered event to preserve that digest, zero new computation receipts, one POST and exactly one committed effect. This positive input complements the existing rejection guards and detects use of the default payload template. All four payload fields contain ASCII strings; unknown/duplicate keys, missing/null/non-string/non-ASCII values reject.

Supervisor consumes an explicit JSON barrier only as fault coordination. It validates the announced PID against `Popen.pid`, sends SIGKILL, waits for process death, records return code, and starts a different process with attempt 2. Fluss and the ledger service stay alive. Recovery authority is exclusively a fresh bounded Fluss read. stdout, barrier messages and local evidence files cannot feed worker recovery.

The twin action-window normalization retains every event field and bucket, maps each case's job/event/action identity to JOB, maps display timestamps to a display marker, and expresses global offsets as within-job positions. It first asserts every actual payload digest against the actual canonical immutable payload and asserts ACTION_REQUESTED detail against that payload. It then maps the embedded job identity and derived digest to the normalized payload. All other detail survives unchanged. Both actual pre-recovery traces end with ACTION_REQUESTED but independent ledgers differ 0 versus 1. Raw evidence is retained unchanged.

Before these cases, init appends two probe rows to the fresh one-bucket table. It verifies initial earliest/latest 0, then actual scan offsets 0 and 1 and latest offsets 1 and 2. Every later bounded read verifies the persisted probe prefix, earliest 0, contiguous raw offsets, and a stable captured latest boundary. Empty polls never complete a read. Missing prefix, retention gap, timeout, schema mismatch, dependency failure or unexpected process exit fails loudly.

Each assertion saves its name, expected, observed and pass flag in `assertions.json` immediately. Each command saves raw stdout/stderr bytes, argv, PID, natural exit and GST lifecycle timing. Failed capture receipts retain acquired raw bytes explicitly and cannot count as complete. Real reader JSON, ledger snapshots, source hashes, versions and the run manifest live only in the selected evidence directory. Failed runs save failure status and exit nonzero; unexecuted cases never receive success records.

## Revised package failure checks (specified before revised implementation)

For each run, require all seven cases to start and pass, every recorded assertion to pass and complete process captures. Retain failures and match the evidence to the executed source hashes.

- Writer recreation or ACK before flush/future completion: baseline asserts one writer lifetime and exact ACK/event order; every A/B1/B2 crash keeps persisted prefix checks.
- Two reappends accidentally use one writer after refactor: duplicate command explicitly closes/reopens; E2E asserts two different lifetime IDs, two offsets and one logical event.
- Partial store open or close leaks owned resources: retain owning Table, close it before admin/connection, preserve suppressed close failures; source review plus fresh JVM exits required.
- Wrong schema/table replacement/TTL: metadata guard at open and identity/schema guard around bounded reads; real incompatible-table and active-writer negative checks precede final projection.
- Missing prefix or concurrent write gives false absence: earliest-zero, exclusive progress, contiguous raw prefix and stable table/end checks remain mandatory; new active-writer test must produce failure.
- Arbitrary 404 or unavailable lookup permits POST: nested unknown-case checks configure ordinary 404, then verify HOLD_UNKNOWN with no extra POST and no new effect.
- PK import hides conflicting payload: reducer rejects conflicts before SQL selection; select only validated jobs, including real duplicate log IDs without conflicts.
- Second import grows current PK row count: SQL exports first/second snapshots, compares all nine fields against fresh Java logical rows and asserts one row/event ID. Raw duplicate rows remain in log.
- SQL trace differs from real run: prepare captures fresh Java reads, bounded SQL exports ordered attempt/step/type/action, checker compares ordered rows.
- Compose depends on host/private state: Dockerfile copies allowlisted source only, named owned volumes, no host ports or privileges; clean exported checkout requires Docker/Compose alone.
- SQL succeeds only from hot cache: shared remote-data volume at identical path in coordinator/tablet/Flink; create PK snapshot and force a cold reader execution before recording success.
- Process startup mistaken for passing E2E: all seven case statuses/assertions and independent SQLite ledger required, finite subprocess/service timeouts, failed run writes FAILED manifest and nonzero exit.

The execution operator records runtime results, image digests and current resource capacity.

Unrecognized positive lookup and unauthorized lookup checks are specified before their implementation: configure a 200 receipt with an unknown contract and a 401 lookup response after B2. Each fresh recovery must append HOLD_UNKNOWN, keep exactly the original effect, and make no new POST. Wrong action/digest receipts under the known contract continue to reject.

## Revision 1 failure admission (specified before correction)

A real child that emits a valid RESULT, then malformed JSON, then a tail and exits zero must still fail supervision. Structured parsing must not stop raw capture through EOF. Injected capture-file write failure must fail admission, retain the acquired tail in explicit failed-capture evidence and never be labelled complete merely because the thread stopped. stdout/stderr acquisition errors must reach process admission.

The pinned SQL client can naturally exit zero after an SQL error. Every submitted program must be bound to prepared, launch, executed-copy and check-time hashes. Require no CLI error, ordered executed statement markers, the expected count of synchronous DML completions and independently queried FINISHED job identities. A reusable negative phase executes real valid all-field output first and then an invalid final SQL statement; the wrapper and ordinary admission must reject, despite existing correct sink rows.

Exact all-field equality is necessary but insufficient for stored-snapshot proof. Without an independent proof record bound to current source/run/prepared SQL/executed SQL/results/job identity, full SQL acceptance must reject and report OUTPUT_EQUALITY_ONLY. The proof must identify the current table/bucket, a snapshot offset covering both imports, recorded snapshot-file hashes, a fresh reader after an actual restart and snapshot consumption by that reader. The original revision 1 proposal required per-file opens; revision 2 below replaces that overstatement with one source-backed causal witness. Never synthesize physical evidence.

ZooKeeper readiness must run an actual bounded srvr request without starting an extra JVM, preserve health dependencies and stay within the existing service resource caps.

## Revision 2 causal-witness admission

Private failure cases were specified before this correction. The sole cold-proof method requires an actual new reader, current table/bucket/snapshot metadata and matching stored SST/file hashes/sizes, with snapshot offset exactly equal to the stopped positive PK end. The exact DEBUG HybridSnapshotLogSplit assignment must have isBatch true, isSnapshotFinished false, recordsToSkip 0 and equal positive start/stop offsets. Same-bucket download start, successful completion and the corresponding split finish must occur in that restarted reader's new FINISHED job window. A fresh empty sink and exact nonempty nine-field equality are mandatory.

The empty log range disables log scanning in the pinned scanner; SnapshotFilesReader opens downloaded RocksDB. This is a source-backed causal witness of snapshot consumption, not a syscall trace or a claim that every file opens. Missing/nonempty range, wrong identity, stale process/job/sink, missing download completion, failed job, changed raw/source/SQL/result hashes or absent independent attestation rejects. No-proof full admission still rejects with OUTPUT_EQUALITY_ONLY / stored_path NOT_VERIFIED. Synthetic contract checks are not physical evidence.

Coordinator/tablet health probes must target their bound service-DNS interfaces. Keep ZooKeeper srvr/nc, existing healthy dependencies, no host ports and 7.5 GiB/2 CPU ceilings. Source correction alone cannot establish live readiness.
