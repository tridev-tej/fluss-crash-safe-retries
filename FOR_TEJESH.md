# Lab map

`src/main/java/lab/retry/` holds the CLI, serial worker, bounded Fluss store, immutable action request, reducer and HTTP client. `scripts/service.py` is the independent atomic SQLite fixture. `scripts/e2e.py` kills real worker JVMs and compares fresh Fluss reads with the independent ledger. `scripts/quickstart.py` prepares/verifies SQL from that same run. `scripts/sql_evidence.py` admits exact executed statements, finished jobs and the independently reviewed cold-snapshot proof. `sql/` holds the readable query/projection templates.

Run core first, then SQL using README commands and a fresh Compose project. The initializer refuses an existing log. The complete service caps total 7.5 GiB/2 CPUs. All storage/evidence uses project-owned named disk volumes. No host ports or special host storage setup.

The append log is recovery authority. The PK event-ID table is a bounded query projection only. Recovery must not infer an external effect from either table. Keep the stable action ID and exact saved request; unknown lookup means hold.

Inspect each run's emitted manifests and process captures. Freeze all source hashes before core and preserve that full-source binding through SQL and cold-proof admission. Record the actual host/architecture, image digests and connector identity with the matching evidence. Publish only reviewed source; keep generated evidence outside Git.
