# Crash-safe retries with Apache Fluss

An unofficial, author-owned experiment with Apache Fluss 1.0.0, Java 17 and a deterministic HTTP/SQLite fixture. A supervisor kills a worker with real `SIGKILL`, starts a fresh JVM and compares Fluss history with the service's independent durable effect ledger.

B1 dies after `ACTION_REQUESTED` is acknowledged but before HTTP. B2 dies after a successful HTTP response and validated receipt but before `ACTION_CONFIRMED` is appended. The normalized histories match; the service has zero effects at B1 and one at B2. Recovery needs the service's receipt contract to distinguish them.

These are separate runs of the same serial worker: one active worker, one shared log bucket, replay from offset 0. All writers to that bucket must be stopped during bounded reads. It is not a concurrent-worker protocol.

## Verify each run

Inspect the matching run's emitted `core/manifest.json`, `case-status.json`, `assertions.json` and command/process captures. Require `PASSED`, all seven cases started and passed, every assertion passing and complete stdout/stderr capture through EOF for every finished process. Check SQL output equality, actual FINISHED jobs and negative-phase rejection separately, then require the independent cold-snapshot witness for full admission. Source/code checks do not establish runtime behavior.

## Quickstart

Requires Docker with Compose v2, network access to the pinned image tags and Maven repositories, at least 8 GiB available RAM and sufficient disk space for images and owned volumes. No host Java, Maven, Python, sudo, filesystem formatting or published ports are needed. Record the actual host, architecture, resolved image digests, connector hash and source identity with each run. Check compatibility against that host's actual evidence; no Docker Desktop or ARM compatibility is implied.

Use a new project name for each complete run. The initializer refuses an existing `retry_lab.job_events`; a new run ID alone does not create a fresh cluster.

```bash
export COMPOSE_PROJECT_NAME=fluss-retry-lab-01
export LAB_RUN_ID=compose-01
docker compose --profile tools build runner
docker compose up -d --wait zookeeper coordinator-server tablet-server
docker compose run --rm runner core
```

Success means `core/manifest.json` reports `PASSED`, seven started/passed cases and all recorded assertions passing. Merely starting the containers is not success. Failures exit nonzero and leave evidence in the project's `evidence` volume.

ZooKeeper readiness sends a bounded `srvr` request with the existing `nc`/`timeout` tools; it does not start a second JVM inside the 0.10 CPU quota. Coordinator and tablet health checks target their bound service-DNS interfaces, not loopback.

The runner image builds the source with Maven 3.9.9/Java 17, then runs Python's standard-library supervisor. The SQLite fixture is an independent process inside the runner container, bound to `127.0.0.1`; it stays alive across worker kills. Its database lives in the evidence volume. The Java worker reads only Fluss and HTTP receipts to recover, never supervisor files or stdout.

The full configured service ceiling is 7.5 GiB and 2 CPUs. Core needs only ZooKeeper, coordinator, tablet server and runner; SQL runs after the core runner exits. Image builds have their own Docker build process. Check host capacity before building and starting the stack. JVM settings, filesystem use, actual cgroup limits and image compatibility must be verified at execution time; do not raise the disk guard or remove limits to hide a failure.

## Bounded Flink SQL and event-ID projection

After core passes, prepare SQL from fresh Java reads of the same run:

```bash
docker compose run --rm runner sql-prepare
docker compose --profile sql build sql-client
docker compose --profile sql up -d --wait jobmanager taskmanager
docker compose --profile sql run --rm sql-client
docker compose run --rm runner sql-phase-check --sql-phase program
```

Preparation substitutes the real run's job IDs into the authored templates, then records every statement, expected synchronous DML completion, unique query/job name, marker and SQL hash. After preparing all files, it assigns only the new SQL directory to UID/GID 9999 so the pinned SQL-client image can create launch and sink directories. The wrapper copies the exact SQL into an exclusive launch directory and checks its hash before and after the client. It preserves the native exit separately from wrapper exit. Flink 1.20 can exit zero after SQL errors, so the wrapper also rejects error output, missing/out-of-order executed statement markers and missing synchronous DML completions. The Python phase admission repeats those checks, rehashes the original/executed SQL and execution inputs, and queries the JobManager REST API for each expected job's actual FINISHED state. Raw client logs and REST receipts stay in evidence. No success is inferred from native exit zero alone.

The program executes the ordered bounded trace, exports JSON, creates `events_by_id` with `PRIMARY KEY (event_id) NOT ENFORCED`, imports actual validated log records twice and exports a snapshot after each import. The reducer validates the completed B2 job and a distinct-writer duplicate job first; the deliberately conflicting event is excluded. All nine fields are compared, not only counts.

Run the real negative phase after the normal program. It produces valid all-field output from the current PK table, then ends with invalid SQL:

```bash
docker compose --profile sql run --rm -e SQL_PROGRAM=reject sql-client
docker compose run --rm runner sql-phase-check --sql-phase reject
docker compose run --rm runner sql-rejection-check
```

The first two commands **must exit nonzero**. The third succeeds only if the valid prefix's jobs finished, all nine output fields match, the invalid statement was last, the pinned client's natural exit was zero, and the wrapper/common verifier rejected its error. Retain both expected failures. A failed negative test is not permission to skip it.

Every Fluss/Flink process that reads remote files mounts the same project-owned `remote-data` disk volume at `/var/lib/fluss/remote-data`. The tablet requests KV snapshots every 2 seconds. Record the actual PK table/bucket and the changelog end after both import jobs. Then capture the corresponding snapshot metadata and snapshot-file hashes, requiring its offset to equal that stopped PK end, greater than zero. A listing of unrelated files is insufficient.

```bash
docker compose exec -T tablet-server find /var/lib/fluss/remote-data -type f
docker inspect "${COMPOSE_PROJECT_NAME}-taskmanager-1"
docker compose --profile sql restart taskmanager
docker inspect "${COMPOSE_PROJECT_NAME}-taskmanager-1"
docker compose --profile sql run --rm -e SQL_PROGRAM=verify sql-client
docker compose run --rm runner sql-proof-rejection-check
```

The process identities must come from actual before/after inspect records, including host PID, start time, container ID and image ID; a container name or in-container PID 1 is insufficient. Record the actual connector JAR hash and independently establish its relationship to the pinned source. Enable the enumerator's DEBUG assignment logger and retain the new reader's INFO download and split-completion logs. Verify the active logging configuration before changing runtime logging; these commands alone do not enable or capture it. Before verification, capture an empty fresh `pk-cold` sink and the job overview with no active jobs. Then collect the exact new job's terminal receipt and timestamped logs.

`sql-proof-rejection-check` runs the complete output/job comparison through ordinary full admission with no proof. It must reject stored-path admission and preserve `OUTPUT_EQUALITY_ONLY` / `stored_path: NOT_VERIFIED`, then record the successful negative test. Ordinary `sql-check` without proof exits nonzero. Matching rows and a TaskManager restart alone can never produce full PASSED.

## Independent cold-proof contract

After the no-proof rejection, `sql/cold-context.json` provides the exact binding identity. Supply an independently checked record through an explicit argument:

```bash
docker compose run --rm runner sql-check --cold-proof /evidence/compose-01/sql/cold-proof.json
```

Replace `compose-01` with the actual `LAB_RUN_ID`. This command does not generate a proof record. To place an externally reviewed file in the named volume, use a separate runner container and `docker cp`; keep all evidence paths relative to that run's `sql/` directory.

Required top-level JSON fields:

| Field | Required value |
| --- | --- |
| `version` | integer 1 |
| `method` | `INDEPENDENT_SNAPSHOT_CAUSAL_REVIEW`, the sole accepted method |
| `run_id`, `context_sha256`, `source_manifest_sha256` | exact current context values |
| `executed_verify_sql_sha256` | `launches.verify.executed.sql` hash from that context |
| `fluss_commit` | `5c07f88e50a8ff41b0ebc214a0458e5dba60be37` |
| `fluss_tree` | `a84280af47d967e0172101c409572515014e49fe` |
| `checked_by`, `checked_at_gst` | actual independent reviewer and ISO timestamp with `+04:00` |
| `independent_review` | Boolean true, only after inspecting actual primary evidence |
| `evidence` | the twelve receipt descriptors below |

The release identifier beginning `5c07f88` is a commit, not a tree. Its tree is the distinct value above. An image tag alone does not establish connector code identity.

Each descriptor contains a lab-relative `path` and SHA-256 of its normalized JSON receipt. Each receipt contains current `run_id`, literal acquisition `command` argv, `exit_code: 0`, `captured_at_gst` and a `raw` descriptor (`path`, `sha256`) pointing to retained non-synthetic primary capture. The independent reviewer checks every normalized fact and command against the primary capture, including the actual reader/process/job and split-ID association. Normalization does not replace the raw evidence. No populated proof or private one-off metadata helper is distributed.

| Receipt | Required raw capture and normalized facts |
| --- | --- |
| `table` | metadata JSON capture; `table_path: retry_lab.events_by_id`, integer `table_id`, `bucket: 0`, positive `imports_end_offset_exclusive` and ordered `import_job_ids` matching both FINISHED projection imports in the context |
| `snapshot` | same metadata JSON; `table_id`, `bucket`, nonnegative integer `snapshot_id`, positive `offset_exclusive` exactly equal to stopped PK end; nonempty `files` with metadata `uri`/`name`, SHA-256 and positive `bytes`, including SST data |
| `file_hashes`, `file_sizes` | raw `sha256sum` and size captures, one `hash path` or `bytes path` per line; every metadata file must match the same shared-volume path and recorded hash/size |
| `reader_before`, `restart` | each raw Docker inspect JSON array contains exactly one actual TaskManager; nonempty `Id`/`Image`, positive `State.Pid` and explicit-offset `State.StartedAt`; new process start must be later, with the same image |
| `connector` | raw single `sha256sum` line; actual `jar_path`, `sha256`, `image_id` matching the restarted reader; independent reviewer verifies the runtime artifact against the pinned mechanism |
| `jobs_before` | raw JobManager overview JSON before the query; new verify job absent and no active jobs |
| `sink_before` | raw JSON containing exactly `path` (the actual run's absolute `sql/pk-cold` directory) and `files: []`, captured after reader restart and before verification |
| `job` | raw REST job-detail JSON; current verify `jid`, `name`, `state: FINISHED`, `start-time`, `end-time` must match the ordinary verifier's independently obtained job record |
| `assignment` | raw timestamped JobManager log; `job_id`, independently matched `split_id`, one-based `line` pointing to the exact DEBUG `FlinkSourceEnumerator` assignment |
| `reader` | raw timestamped TaskManager log; current `job_id`, `container_id`, `pid`, `started_at`, `table_id`, `bucket`, `snapshot_id`, `split_id`; one-based `download_start_line`, `download_complete_line`, `split_finished_line` |

Metadata stdout must be JSON with schema `fluss-snapshot-metadata-v1`, current `run_id`, `timestamp`, `table: {path, id, bucket}` and `snapshot: {id, log_offset, pk_end_offset, files: [{path, name}]}` from the actual metadata API. Both offsets must equal the stopped PK end. The checker derives identities from this capture and matches every stored file to it. Only absolute local paths or `file:` URIs under `/var/lib/fluss/remote-data` are admitted. All snapshot metadata and stored-file captures must precede the reader restart.

The assignment must contain exactly the current unpartitioned table/bucket/snapshot `HybridSnapshotLogSplit`, with `isBatch=true`, `isSnapshotFinished=false`, `recordsToSkip=0` and `logStartingOffset == logStoppingOffset == stopped PK end > 0`. The same bucket's INFO download-start, successful download-completion and corresponding split-finished entries must occur in that new FINISHED job's window, in order after assignment. Docker timestamp prefixes are required. The independent reviewer binds the enumerator assignment and split ID to this sole job and confirms the logs belong to the inspected restarted process. Missing or nonempty log range, wrong identity, missing completion, stale process/sink or failed job rejects.

The empty range is load-bearing. The pinned [KvSnapshotAndLogBatchScanner](https://github.com/apache/fluss/blob/5c07f88e50a8ff41b0ebc214a0458e5dba60be37/fluss-client/src/main/java/org/apache/fluss/client/table/scanner/batch/KvSnapshotAndLogBatchScanner.java) sets `logScanner=null` for that range. [KvSnapshotBatchScanner](https://github.com/apache/fluss/blob/5c07f88e50a8ff41b0ebc214a0458e5dba60be37/fluss-client/src/main/java/org/apache/fluss/client/table/scanner/batch/KvSnapshotBatchScanner.java) completes the download before creating [SnapshotFilesReader](https://github.com/apache/fluss/blob/5c07f88e50a8ff41b0ebc214a0458e5dba60be37/fluss-client/src/main/java/org/apache/fluss/client/table/scanner/batch/SnapshotFilesReader.java), which opens the downloaded RocksDB directory and reads its iterator. With exact nonempty nine-field results, this is a **source-backed causal witness of snapshot consumption**. It is not a syscall trace and does not assert that RocksDB opens every downloaded file. Use the default snapshot-merge batch strategy; `server-scan`, point lookups, counts and lake union reads cannot satisfy this contract.

Full admission rereads the actual sink files and independent expected rows. All nine field names and values must match; rows must be nonempty, while individual values may be null or empty. Each current sink file's hash must match the context and its modification time must fall within the actual job window. Source, executed SQL, raw receipts, query/import jobs and results remain bound through the ordinary verifier. A fabricated record is not physical evidence; independent inspection remains mandatory. Only actual accepted evidence can change the full report to `PASSED` / `stored_path: PASSED`. Every verification attempt has a retained unique report; `verification.json` is the latest materialized result.

The PK table remains a query projection. It does not deduplicate its changelog, provide ownership or atomically commit with the log or HTTP effect. Current-row equality and storage-path evidence are separate checks. No lakehouse runtime is claimed.

## Evidence and shutdown

Export the run without host Python or a bind mount:

```bash
mkdir -p results
docker compose run --rm -T runner export > results/run.tar
docker compose --profile sql --profile tools config > results/compose-resolved.yaml
docker compose images --format json > results/images.json
docker image inspect apache/fluss:1.0.0 zookeeper:3.9.2 apache/fluss-quickstart-flink:1.20-1.0.0 "${COMPOSE_PROJECT_NAME}-runner:1.0.0" "${COMPOSE_PROJECT_NAME}-sql:1.0.0" > results/image-inspect.json
docker version > results/docker-version.txt
docker compose version > results/compose-version.txt
docker compose --profile sql --profile tools down
```

Keep the shell's actual command/exit records and resolved image digests/platform with the results. No digest is inferred from a tag. `down` stops this project and removes its network; named volumes and results remain. Use a new project for another clean run. Never prune unrelated Docker resources.

Inside the archive:

- `core/manifest.json`, `case-status.json`, `assertions.json`: actual executed cases, assertions and failures.
- `core/commands.json` and process logs: argv, PID, natural exit or `SIGKILL`, and GST timing.
- `*-fluss.json`: fresh Java reads, raw offsets and logical reduction. `*-ledger.json` and `ledger.sqlite`: independent computations, effects, payload bytes, receipts and HTTP audit.
- `twin-normalization.json`: B1/B2 logical-prefix comparison, with raw evidence retained.
- `source-hashes.json`, runtime/version records: exact source identity and execution environment.
- `sql/program.sql`, `verify.sql`, `reject.sql`, execution-input hashes, native/wrapper exits, raw logs, statement markers, REST job receipts, JSON sinks, negative-test reports, `cold-context.json` and verification attempts: exact execution, equality and separate stored-path admission.

Generated results, logs, databases, archives and build artifacts are ignored by Git. No private historical evidence is distributed with the source.

## Failure cases

[E2E_SPEC.md](E2E_SPEC.md) defines the seven cases and rejection conditions. Nested checks include immutable payload preservation, wrong receipt bindings, unavailable lookup, ordinary undocumented 404, unknown receipt contract, unauthorized lookup, schema mismatch, table replacement and committed appends after the snapshot end is captured and before scanning resumes. The duplicate-event command explicitly closes/reopens the store, and tests assert distinct writer lifetime IDs.

A baseline new action must return HTTP 201 **and** create exactly one independent effect row. Existing-key retries return the original receipt; changed payloads return HTTP 409. Persisted intents retain exact canonical ASCII JSON bytes, stable action ID and digest across attempts.

FlussStore owns one connection, one thread-confined table and one append writer per worker lifetime. It opens lazily after initialization can create the table, validates metadata, and retains `append -> flush -> future.get -> ACK`. Scanner lifetime is bounded; table/admin/connection close attempts continue after a close failure, with suppressed failures retained. `TableWriter` in 1.0.0 has no `close()` method.

## Worker read/write and SQL checks

The nine fields are `job_id`, `event_id`, `attempt`, `step`, `event_type`, `action_id`, `payload_digest`, `emitted_at_ms` and `detail`. Attempt/step determine logical order; timestamps are for display.

The append ACK/barrier follows blocking `flush()` and successful `future.get()`. The future timeout bounds only the wait after blocking flush, not the entire append method. The table, admin and connection close when the store ends. Cleanup continues after a close failure and retains suppressed failures. Writer lifetime IDs establish that the duplicate fixture closes/reopens the store rather than sharing one writer across simulated restarts.

Initialization writes probes at offsets 0 and 1 and verifies latest offsets 1 and 2. Each later bounded read requires earliest offset 0, the probe prefix, contiguous raw offsets, unchanged table metadata and a final committed end equal to the captured end. Completion uses exclusive consumed progress, including progress-only empty polls. Recovery aborts if it detects a missing prefix, changed table metadata or a changed committed-log end. These checks do not detect writer presence or enforce exclusion; all writers to the bucket must remain stopped throughout the read.

The SQL check compares the exact ordered trace with an independent fresh Java read. Both repeated imports must produce all nine fields equal to the Java logical events. The selected distinct-writer duplicate contributes two identical raw log records but one current PK row. A conflicting event ID must reject in the reducer before import, because an upsert can otherwise hide the conflict. After SQL, fresh Java reads independently confirm unchanged logical events and retained raw duplicates. Every expected query/import job must independently be FINISHED; native SQL exit zero is insufficient.

## Receipt contract

Only the fixture's atomic contract permits POST after an explicit contract-bound negative lookup. A single SQLite transaction binds action ID, exact payload, digest, effect and receipt; the fixture serializes requests and does not expire receipts. It is not an example of production API authentication.

For a real service, document authenticated lookup semantics, idempotency scope and retention, eventual consistency, in-flight requests, expired receipts and immutable payload binding. An ordinary 404 is insufficient. Transport failures, unavailable/auth/unknown responses and unrecognized absence produce `HOLD_UNKNOWN`; malformed positive receipts reject. Reuse the original action ID and saved payload. A Fluss acknowledgment or PK row cannot settle the external effect.

## Retention and scaling

`table.log.ttl=0ms` disables TTL deletion only. Remote log tiering stores log segments; lakehouse format tiering is separate and needs a configured tiering service. Neither silently replaces this worker's native offset-0 recovery. A production design must preserve the complete recovery prefix or implement a recoverable checkpoint/projection before deleting it.

More buckets hashed by `job_id` allow different jobs to run concurrently. Same-job execution still needs a fenced/leased owner, per-bucket progress and a recoverable current-job PK projection. Hashing provides no same-job exclusion or cross-system transaction.

One replica and minimum ISR 1 mean `acks=all` adds no follower redundancy over leader acknowledgment. Worker SIGKILL recovery does not prove host durability, cluster failover or general exactly-once external effects. B2 excludes response loss, timeouts and requests still in flight. A later email or notification adds another boundary.

## Trace normalization

Normalization maps each case's job/event/action IDs to a common job, display timestamps to a marker and global offsets to within-job positions. It validates actual canonical payloads/digests first, then normalizes the embedded job ID and recomputes the normalized digest. Event types, attempt/step ordering, bucket and remaining request content stay intact. Equal normalized prefixes are not byte-identical raw traces.

## References and license

[Java client](https://fluss.apache.org/docs/apis/java/), [architecture/high watermark](https://fluss.apache.org/docs/concepts/architecture/), [Flink reads](https://fluss.apache.org/docs/engine-flink/reads/), [table types](https://fluss.apache.org/docs/table-design/overview/), [lakehouse tiering](https://fluss.apache.org/docs/streaming-lakehouse/overview/).

Authored source: Apache-2.0, Copyright 2026 Tejesh Vaish. [NOTICE](NOTICE) attributes the adapted Apache Docker/Flink examples. Dependencies and images retain their own notices. No upstream binary, private trace or historical database is vendored. The locally shaded executable is not offered as a release download.
