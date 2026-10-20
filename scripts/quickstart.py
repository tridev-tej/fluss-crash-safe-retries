"""Run the owned serial lab and compare bounded SQL reads with fresh Java reads."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import socket
import subprocess
import sys
import tarfile
import time

from e2e import save_json, timestamp
from sql_evidence import (ProofRequired, admit_phase, terminal_jobs, cold_context, admit_cold_proof,
                          prepare_phase, source_hashes, phase_hashes, client_output, completed_markers)
import uuid

ROOT = Path(__file__).resolve().parents[1]
FIELDS = ["job_id", "event_id", "attempt", "step", "event_type", "action_id",
          "payload_digest", "emitted_at_ms", "detail"]
TRACE_FIELDS = ["attempt", "step", "event_type", "action_id"]


def checked(argv, directory, label, seconds=120):
    started = timestamp()
    try:
        result = subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True,
                                timeout=seconds)
    except subprocess.TimeoutExpired as failure:
        if failure.stdout is not None:
            (directory / (label + ".stdout.log")).write_bytes(failure.stdout)
        if failure.stderr is not None:
            (directory / (label + ".stderr.log")).write_bytes(failure.stderr)
        save_json(directory / (label + "-command.json"), {"argv": argv, "status": "TIMEOUT",
                  "started_gst": started, "ended_gst": timestamp(),
                  "stdout_acquired": failure.stdout is not None, "stderr_acquired": failure.stderr is not None})
        raise TimeoutError(label + " timed out") from failure
    (directory / (label + ".stdout.log")).write_bytes(result.stdout)
    (directory / (label + ".stderr.log")).write_bytes(result.stderr)
    save_json(directory / (label + "-command.json"), {"argv": argv, "exit": result.returncode,
              "started_gst": started, "ended_gst": timestamp()})
    if result.returncode != 0:
        raise RuntimeError(label + " exited " + str(result.returncode))
    return result.stdout.decode("utf-8")


def java(argv):
    return ["java", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-jar",
            str(ROOT / "target/fluss-retry-lab.jar")] + argv


def wait_port(host, port, seconds=120):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError as failure:
            last = failure
            time.sleep(1)
    raise TimeoutError(str(host) + ":" + str(port) + " not ready: " + str(last))


def core(args, directory):
    directory.mkdir(parents=True, exist_ok=False)
    runtime = {"status": "STARTED", "started_gst": timestamp(), "platform": platform.platform(),
               "machine": platform.machine(), "python": sys.version,
               "server_image": "apache/fluss:1.0.0", "client_version": "1.0.0",
               "image_digests": "capture with Docker image inspect on execution host",
               "scope": "one bucket, serial workers, worker-process loss only"}
    save_json(directory / "runtime.json", runtime)
    try:
        wait_port("coordinator-server", 9123)
        wait_port("tablet-server", 9123)
        checked(java(["ready", "--bootstrap", "coordinator-server:9123"]), directory, "ready")
        command = [sys.executable, str(ROOT / "scripts/e2e.py"), "verify", "--run-id", args.run_id,
                   "--evidence", str(directory / "core"), "--cluster-evidence", str(directory / "runtime.json"),
                   "--bootstrap", "coordinator-server:9123", "--timeout", "90"]
        checked(command, directory, "core", seconds=1800)
        runtime["status"] = "PASSED"
    except Exception as failure:
        runtime.update(status="FAILED", error=str(failure))
        raise
    finally:
        runtime["ended_gst"] = timestamp()
        save_json(directory / "runtime.json", runtime)


def fresh_read(job, directory, label):
    output = checked(java(["inspect", "--bootstrap", "coordinator-server:9123", "--job", job,
                           "--timeout-seconds", "90"]), directory, label)
    messages = [json.loads(line) for line in output.splitlines() if line.startswith('{')]
    results = [m for m in messages if m.get("kind") == "RESULT"]
    if len(results) != 1:
        raise AssertionError("Expected exactly one fresh Java RESULT")
    save_json(directory / (label + ".json"), results[0])
    return results[0]


def render(template, trace_job, duplicate_job):
    return template.replace('@TRACE_JOB@', trace_job).replace('@DUPLICATE_JOB@', duplicate_job)


def sink_ddl(name, fields, location):
    types = dict.fromkeys(FIELDS, "STRING")
    types.update(attempt="INT", step="INT", emitted_at_ms="BIGINT")
    columns = ', '.join(field + ' ' + types[field] for field in fields)
    return (f"CREATE TEMPORARY TABLE default_catalog.default_database.{name} ({columns}) "
            f"WITH ('connector' = 'filesystem', 'path' = '{location.as_uri()}', 'format' = 'json');\n")


def snapshot_sql(name, location):
    return (sink_ddl(name, FIELDS, location) + f"INSERT INTO {name} SELECT " + ', '.join(FIELDS)
            + " FROM fluss_catalog.retry_lab.events_by_id ORDER BY event_id;\n")


def prepare_sql(args, directory):
    manifest = json.loads((directory / "core/manifest.json").read_text())
    if manifest["status"] != "PASSED" or manifest["cases_started"] != 7:
        raise AssertionError("All seven core cases must pass before SQL preparation")
    sql = directory / "sql"
    sql.mkdir(exist_ok=False)
    trace_job = args.run_id + "/after-commit/ticket-1042"
    duplicate_job = args.run_id + "/projection-duplicate/ticket-1042"
    trace = fresh_read(trace_job, sql, "fresh-trace")
    duplicate = fresh_read(duplicate_job, sql, "fresh-duplicate")
    if duplicate["raw_count"] != 2 or duplicate["logical_count"] != 1:
        raise AssertionError("Projection duplicate source must retain two raw rows and one valid event")
    expected = {"trace_job": trace_job, "duplicate_job": duplicate_job,
                "trace": [{k: event[k] for k in TRACE_FIELDS} for event in trace["logical"]],
                "projection": sorted(trace["logical"] + duplicate["logical"], key=lambda event: event["event_id"])}
    save_json(sql / "expected.json", expected)
    trace_sql = render((ROOT / "sql/trace.sql").read_text(), trace_job, duplicate_job)
    projection = render((ROOT / "sql/projection.sql").read_text(), trace_job, duplicate_job)
    header, query = trace_sql.split("SELECT attempt", 1)
    query = "SELECT attempt" + query
    trace_sink = sink_ddl("trace_out", TRACE_FIELDS, sql / "trace")
    import_statement = projection[projection.index("INSERT INTO"):]
    settings = "SET 'table.dml-sync' = 'true';\nSET 'parallelism.default' = '1';\n"
    program = (header + settings + query + trace_sink + "INSERT INTO trace_out " + query
               + projection + snapshot_sql("pk_first", sql / "pk-first")
               + import_statement + snapshot_sql("pk_second", sql / "pk-second"))
    phases = {
        "program": prepare_phase(sql, "program", program, args.run_id),
        "verify": prepare_phase(sql, "verify", header + settings + snapshot_sql("pk_cold", sql / "pk-cold"), args.run_id),
        "reject": prepare_phase(sql, "reject", header + settings + snapshot_sql("reject_out", sql / "reject-output"),
                                args.run_id, invalid_final=True)
    }
    save_json(sql / "preparation.json", {"version": 1, "run_id": args.run_id,
              "source_hashes": source_hashes(ROOT), "phases": phases})
    save_json(sql / "sql-hashes.json", {name + ".sql": contract["sql_sha256"] for name, contract in phases.items()})
    os.chown(sql, 9999, 9999)
    print("Prepared SQL from fresh Java reads; SQL execution and cold-read comparison still required", flush=True)


def json_rows(directory):
    if not directory.is_dir():
        raise AssertionError("Missing bounded SQL output: " + str(directory))
    files = sorted(p for p in directory.rglob('*') if p.is_file() and not p.name.startswith('.'))
    if not files:
        raise AssertionError("Empty bounded SQL output: " + str(directory))
    rows = []
    for path in files:
        rows.extend(json.loads(line) for line in path.read_text().splitlines() if line.strip())
    return rows


def assertion(checks, name, expected, observed):
    passed = expected == observed
    checks.append({"name": name, "expected": expected, "observed": observed, "passed": passed})
    if not passed:
        raise AssertionError(name)


def compare_sql(directory, checks):
    sql = directory / "sql"
    expected = json.loads((sql / "expected.json").read_text())
    admit_phase(sql, "program")
    admit_phase(sql, "verify")
    assertion(checks, "ordered trace equals fresh Java", expected["trace"], json_rows(sql / "trace"))
    for name in ["pk-first", "pk-second", "pk-cold"]:
        rows = sorted(json_rows(sql / name), key=lambda event: event["event_id"])
        assertion(checks, name + " all fields equal Java", expected["projection"], rows)
        assertion(checks, name + " one current row/event_id", len(rows), len({r["event_id"] for r in rows}))
    fresh = fresh_read(expected["trace_job"], sql, "after-sql-trace")
    duplicate = fresh_read(expected["duplicate_job"], sql, "after-sql-duplicate")
    assertion(checks, "raw log duplicates retained", 2, duplicate["raw_count"])
    assertion(checks, "Java trace unchanged after SQL", expected["projection"],
              sorted(fresh["logical"] + duplicate["logical"], key=lambda event: event["event_id"]))


def phase_check(directory, phase):
    sql = directory / 'sql'
    report = {'status': 'FAILED', 'scope': 'SQL_EXECUTION_ONLY', 'phase': phase, 'started_gst': timestamp()}
    try:
        contract = admit_phase(sql, phase)
        report.update(status='PASSED', sql_sha256=contract['sql_sha256'], jobs=terminal_jobs(sql, phase))
    except Exception as failure:
        report['error'] = str(failure)
        raise
    finally:
        report['ended_gst'] = timestamp()
        save_json(sql / ('phase-' + phase + '-' + uuid.uuid4().hex + '.json'), report)
    return report


def rejection_check(directory, write=True):
    sql = directory / "sql"
    checks = []
    expected = json.loads((sql / "expected.json").read_text())
    contract, launch = phase_hashes(sql, "reject")
    try:
        phase_check(directory, "reject")
    except (AssertionError, RuntimeError) as failure:
        rejected = str(failure)
    else:
        raise AssertionError("Invalid final SQL statement was admitted")
    assertion(checks, "invalid final is exact last statement", True,
              (launch / "executed.sql").read_text().rstrip().endswith('THIS IS INVALID SQL;'))
    assertion(checks, "invalid final native exit witness", '0', (launch / "native.exit").read_text().strip())
    assertion(checks, "invalid final wrapper rejects", True, (launch / "wrapper.exit").read_text().strip() != '0')
    assertion(checks, "invalid final CLI error", True, '[ERROR]' in client_output(launch))
    assertion(checks, "valid prefix completed first", contract['markers'], completed_markers(launch))
    assertion(checks, "valid all-field work before rejection", expected['projection'],
              sorted(json_rows(sql / "reject-output"), key=lambda event: event['event_id']))
    jobs = terminal_jobs(sql, "reject")
    report = {"status": "PASSED", "admission": "REJECTED", "reason": rejected,
              "assertions": checks, "jobs": jobs, "sql_sha256": contract['sql_sha256']}
    if write:
        save_json(sql / "rejection-test.json", report)
    return report


def save_verification(sql, report):
    attempt = sql / ('verification-' + uuid.uuid4().hex + '.json')
    report['attempt_file'] = attempt.name
    save_json(attempt, report)
    save_json(sql / 'verification.json', report)


def check_sql(directory, proof=None):
    checks = []
    report = {"status": "FAILED", "stored_path": "NOT_VERIFIED", "started_gst": timestamp()}
    try:
        compare_sql(directory, checks)
        negative = rejection_check(directory, write=False)
        jobs = {phase: terminal_jobs(directory / 'sql', phase) for phase in ['program', 'verify']}
        jobs['reject'] = negative['jobs']
        context = cold_context(directory, ROOT, jobs)
        report.update(status='OUTPUT_EQUALITY_ONLY', output_equality='PASSED', context_sha256=context['context_sha256'])
        report['cold_proof'] = admit_cold_proof(directory / 'sql', proof, context)
        report.update(status='PASSED', stored_path='PASSED')
    except Exception as failure:
        report['error'] = str(failure)
        raise
    finally:
        report.update(ended_gst=timestamp(), assertions=checks, assertion_count=len(checks))
        save_verification(directory / 'sql', report)


def proof_rejection_check(directory):
    try:
        check_sql(directory)
    except ProofRequired as failure:
        report = json.loads((directory / 'sql/verification.json').read_text())
        if report['status'] != 'OUTPUT_EQUALITY_ONLY' or report['stored_path'] != 'NOT_VERIFIED':
            raise AssertionError('No-proof check did not preserve partial status')
        save_json(directory / 'sql/proof-rejection-test.json', {
            'status': 'PASSED', 'admission': 'REJECTED', 'reason': str(failure),
            'context_sha256': report['context_sha256'], 'verification_attempt': report['attempt_file']})
        return
    raise AssertionError('Missing cold proof was admitted')


def export(directory):
    if not directory.is_dir():
        raise FileNotFoundError(directory)
    with tarfile.open(fileobj=sys.stdout.buffer, mode='w|') as archive:
        archive.add(directory, arcname=directory.name)


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['core', 'sql-prepare', 'sql-check', 'sql-phase-check', 'sql-rejection-check', 'sql-proof-rejection-check', 'export'])
    parser.add_argument('--cold-proof', type=Path)
    parser.add_argument('--sql-phase', choices=['program', 'verify', 'reject'])
    parser.add_argument('--run-id', default=os.environ.get('LAB_RUN_ID', 'compose-01'))
    parser.add_argument('--evidence-root', type=Path, default=Path('/evidence'))
    args = parser.parse_args()
    if args.command == 'sql-phase-check' and args.sql_phase is None:
        parser.error('--sql-phase required for sql-phase-check')
    if not re.fullmatch(r'[A-Za-z0-9_-]+', args.run_id):
        parser.error('run-id must contain only ASCII letters, digits, underscore or hyphen')
    return args


def main():
    args = arguments()
    directory = args.evidence_root.resolve() / args.run_id
    operations = {'core': lambda: core(args, directory),
                  'sql-prepare': lambda: prepare_sql(args, directory),
                  'sql-check': lambda: check_sql(directory, args.cold_proof),
                  'sql-phase-check': lambda: phase_check(directory, args.sql_phase),
                  'sql-rejection-check': lambda: rejection_check(directory),
                  'sql-proof-rejection-check': lambda: proof_rejection_check(directory),
                  'export': lambda: export(directory)}
    operations[args.command]()


if __name__ == '__main__':
    main()
