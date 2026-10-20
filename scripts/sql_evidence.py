"""Bind SQL execution, independent job outcomes and externally checked snapshot evidence."""
import hashlib
import json
from pathlib import Path
import re
import urllib.request
from urllib.parse import urlsplit
import uuid
from datetime import datetime

from e2e import save_json, source_files, timestamp

ANSI = re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
MARKER = re.compile(r'\s*\|\s*(L\d{3}_[0-9a-f]{8})\s*\|\s*')
FLUSS_COMMIT = '5c07f88e50a8ff41b0ebc214a0458e5dba60be37'
FLUSS_TREE = 'a84280af47d967e0172101c409572515014e49fe'
EVENT_FIELDS = {'job_id', 'event_id', 'attempt', 'step', 'event_type', 'action_id',
                'payload_digest', 'emitted_at_ms', 'detail'}


class ProofRequired(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def positive_integer(value):
    return type(value) is int and value > 0


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def identity(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def source_hashes(root):
    return {str(path.relative_to(root)): digest(path) for path in source_files()}


def statements(text):
    """Split only the lab's authored templates, whose literals contain no semicolons."""
    return [part.strip() + ';' for part in text.split(';') if part.strip()]


def instrument(text, run_id, phase):
    token = uuid.uuid4().hex[:8]
    preamble = "SET 'execution.runtime-mode' = 'batch';\nSET 'sql-client.execution.result-mode' = 'tableau';\nSET 'table.dml-sync' = 'true';\n"
    output = [preamble]
    markers = []
    jobs = []
    sequence = []
    for index, statement in enumerate(statements(text)):
        marker = f'L{index:03d}_{token}'
        job = f'lab_{run_id}_{phase}_{index:03d}_{token}'
        if statement.startswith(('INSERT INTO ', 'SELECT ')):
            output.append(f"SET 'pipeline.name' = '{job}';\n")
            jobs.append({'name': job, 'statement': index, 'dml': statement.startswith('INSERT INTO '),
                         'role': 'projection-import' if statement.startswith('INSERT INTO fluss_catalog.retry_lab.events_by_id') else 'query'})
        output.append(statement + '\n')
        output.append(f"SET 'pipeline.name' = 'marker_{phase}_{index:03d}_{token}';\n")
        output.append(f"SELECT '{marker}' AS statement_done;\n")
        markers.append(marker)
        sequence.append({'index': index, 'sha256': hashlib.sha256(statement.encode()).hexdigest()})
    return ''.join(output), {'markers': markers, 'jobs': jobs, 'statements': sequence,
                            'dml_count': sum(job['dml'] for job in jobs)}


def prepare_phase(sql, phase, text, run_id, invalid_final=False):
    program, contract = instrument(text, run_id, phase)
    if invalid_final:
        program += 'THIS IS INVALID SQL;\n'
    path = sql / (phase + '.sql')
    path.write_text(program)
    contract.update(sql_sha256=digest(path), invalid_final=invalid_final)
    (sql / (phase + '.expected.sha256')).write_text(contract['sql_sha256'] + '\n')
    (sql / (phase + '.markers')).write_text('\n'.join(contract['markers']) + '\n')
    (sql / (phase + '.dml-count')).write_text(str(contract['dml_count']) + '\n')
    return contract


def phase_hashes(sql, phase):
    prepared = json.loads((sql / 'preparation.json').read_text())
    contract = prepared['phases'][phase]
    launch = sql / ('launch-' + phase)
    expected = contract['sql_sha256']
    for name in ['preparation.json', 'sql-hashes.json', 'expected.json']:
        require((launch / (name + '.sha256')).read_text().strip() == digest(sql / name), 'SQL admission inputs changed: ' + name)
    require((launch / 'wrapper.sha256').read_text().strip() == prepared['source_hashes']['scripts/run-sql.sh'], 'Executed wrapper hash mismatch')
    require(json.loads((sql / 'sql-hashes.json').read_text())[phase + '.sql'] == expected, 'Prepared SQL hash disagreement')
    for path in [sql / (phase + '.sql'), launch / 'executed.sql']:
        require(digest(path) == expected, 'SQL file changed: ' + str(path))
    for name in ['prepared.sha256', 'launch.sha256', 'after.sha256']:
        require((launch / name).read_text().strip() == expected, 'Executed SQL hash mismatch: ' + name)
    return contract, launch


def client_output(launch):
    return ANSI.sub('', (launch / 'client.stdout.log').read_text() + '\n'
                    + (launch / 'client.stderr.log').read_text())


def completed_markers(launch):
    lines = ANSI.sub('', (launch / 'client.stdout.log').read_text()).splitlines()
    return [match.group(1) for line in lines if (match := MARKER.fullmatch(line))]


def admit_phase(sql, phase):
    contract, launch = phase_hashes(sql, phase)
    require((launch / 'native.exit').read_text().strip() == '0', phase + ' native client failure')
    require((launch / 'wrapper.exit').read_text().strip() == '0', phase + ' wrapper rejected SQL')
    require('[ERROR]' not in client_output(launch), phase + ' SQL error output')
    require(completed_markers(launch) == contract['markers'], phase + ' incomplete statement sequence')
    count = client_output(launch).count('Complete execution of the SQL update statement.')
    require(count == contract['dml_count'], phase + ' incomplete synchronous DML jobs')
    return contract


def terminal_jobs(sql, phase):
    contract, launch = phase_hashes(sql, phase)
    with urllib.request.urlopen('http://jobmanager:8081/jobs/overview', timeout=15) as response:
        payload = response.read()
    save_path = launch / ('jobs-' + uuid.uuid4().hex + '.json')
    save_path.write_bytes(payload)
    jobs = json.loads(payload)['jobs']
    selected = []
    for expected in contract['jobs']:
        matching = [job for job in jobs if job['name'] == expected['name']]
        require(len(matching) == 1, 'Missing or ambiguous SQL job: ' + expected['name'])
        job = matching[0]
        require(job['state'] == 'FINISHED', 'SQL job did not FINISH: ' + expected['name'])
        selected.append({'jid': job['jid'], 'name': job['name'], 'state': job['state'],
                         'start-time': job['start-time'], 'end-time': job['end-time'], 'role': expected['role']})
    return selected


def output_hashes(sql):
    roots = ['trace', 'pk-first', 'pk-second', 'pk-cold', 'reject-output']
    return {str(p.relative_to(sql)): digest(p) for name in roots for p in sorted((sql / name).rglob('*'))
            if p.is_file() and not p.name.startswith('.')}


def cold_context(directory, root, jobs):
    sql = directory / 'sql'
    prepared = json.loads((sql / 'preparation.json').read_text())
    current = source_hashes(root)
    require(prepared['run_id'] == directory.name, 'SQL preparation run identity mismatch')
    require(current == prepared['source_hashes'], 'Current source differs from SQL preparation')
    require(current == json.loads((directory / 'core/source-hashes.json').read_text()), 'Current source differs from core run')
    launches = {phase: {name: digest(sql / ('launch-' + phase) / name)
                       for name in ['executed.sql', 'client.stdout.log', 'client.stderr.log', 'native.exit', 'wrapper.exit', 'wrapper.sha256']}
                for phase in ['program', 'verify', 'reject']}
    value = {'version': 1, 'run_id': prepared['run_id'], 'source_manifest_sha256': identity(current),
             'prepared_sql_sha256': digest(sql / 'preparation.json'), 'launches': launches,
             'outputs': output_hashes(sql), 'jobs': jobs, 'expected_sha256': digest(sql / 'expected.json')}
    value['context_sha256'] = identity(value)
    save_json(sql / 'cold-context.json', value)
    return value


def evidence_file(sql, descriptor):
    relative = Path(descriptor['path'])
    require(not relative.is_absolute() and '..' not in relative.parts, 'Proof evidence must be lab-relative')
    path = sql / relative
    require(not path.is_symlink() and path.resolve().is_relative_to(sql.resolve()), 'Proof evidence must stay inside the lab')
    require(path.stat().st_size > 0, 'Proof evidence is empty')
    require(digest(path) == descriptor['sha256'], 'Proof evidence digest mismatch')
    return path


def proof_receipt(sql, descriptor):
    path = evidence_file(sql, descriptor)
    value = json.loads(path.read_text())
    require(type(value['exit_code']) is int and value['exit_code'] == 0, 'Proof acquisition command failed')
    require(isinstance(value['command'], list) and bool(value['command']), 'Proof command must be literal argv')
    require(all(isinstance(arg, str) and bool(arg) for arg in value['command']), 'Proof command argv is invalid')
    evidence_file(sql, value['raw'])
    require(datetime.fromisoformat(value['captured_at_gst']).utcoffset().total_seconds() == 14400, 'Proof time must include GST offset')
    return value


def raw_json(sql, receipt):
    return json.loads(evidence_file(sql, receipt['raw']).read_text())


def millis(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(parsed.utcoffset() is not None, 'Timestamp needs an explicit offset')
    return int(parsed.timestamp() * 1000)


def captured_ms(receipt):
    return millis(receipt['captured_at_gst'])


def nonnegative_integer(value):
    return type(value) is int and value >= 0


def inventory(sql, receipt):
    entries = [line.split(maxsplit=1) for line in evidence_file(sql, receipt['raw']).read_text().splitlines()]
    require(all(len(entry) == 2 for entry in entries), 'Malformed stored-file inventory')
    result = {path: value for value, path in entries}
    require(len(result) == len(entries), 'Duplicate stored-file inventory path')
    return result


def stored_path(uri):
    parsed = urlsplit(uri)
    require(parsed.scheme in {'', 'file'} and not parsed.netloc, 'Snapshot must use the shared local volume')
    path = Path(parsed.path)
    require(path.is_relative_to('/var/lib/fluss/remote-data') and '..' not in path.parts, 'Snapshot outside shared path')
    return str(path)


def admit_files(sql, receipts, snapshot, metadata):
    files = snapshot['files']
    require(bool(files), 'No snapshot files')
    declared = [{'path': item['uri'], 'name': item['name']} for item in files]
    require(declared == metadata['files'], 'Snapshot metadata file identities differ')
    require(len({item['name'] for item in files}) == len(files), 'Duplicate snapshot filename')
    require(any(item['name'].endswith('.sst') for item in files), 'Snapshot has no SST data')
    hashes = inventory(sql, receipts['file_hashes'])
    sizes = inventory(sql, receipts['file_sizes'])
    for item in files:
        path = stored_path(item['uri'])
        require(bool(re.fullmatch('[0-9a-f]{64}', item['sha256'])), 'Invalid stored-file hash')
        require(positive_integer(item['bytes']), 'Invalid stored-file size')
        require(hashes[path] == item['sha256'] and int(sizes[path]) == item['bytes'], 'Stored-file hash/size mismatch')


def admit_snapshot(sql, receipts, context):
    table, snapshot = receipts['table'], receipts['snapshot']
    metadata = raw_json(sql, snapshot)
    require(metadata['schema'] == 'fluss-snapshot-metadata-v1' and metadata['run_id'] == context['run_id'], 'Wrong metadata run/schema')
    require(raw_json(sql, table) == metadata, 'Table receipt differs from snapshot metadata')
    require(table['table_path'] == 'retry_lab.events_by_id', 'Wrong proof table')
    require(nonnegative_integer(table['table_id']), 'Missing actual table ID')
    require(type(table['bucket']) is int and table['bucket'] == 0, 'Wrong proof bucket')
    require(all(nonnegative_integer(metadata['table'][key]) for key in ['id', 'bucket']), 'Invalid metadata table integers')
    require(metadata['table'] == {'path': table['table_path'], 'id': table['table_id'], 'bucket': 0}, 'Metadata table/bucket mismatch')
    require(snapshot['table_id'] == table['table_id'] and type(snapshot['bucket']) is int and snapshot['bucket'] == 0, 'Snapshot table/bucket mismatch')
    require(nonnegative_integer(snapshot['snapshot_id']), 'Snapshot identity absent')
    require(positive_integer(snapshot['offset_exclusive']), 'Invalid snapshot offset')
    require(snapshot['offset_exclusive'] == table['imports_end_offset_exclusive'], 'Snapshot is not at stopped PK end')
    actual = metadata['snapshot']
    require(all(nonnegative_integer(actual[key]) for key in ['id', 'log_offset', 'pk_end_offset']), 'Invalid metadata snapshot integers')
    require((actual['id'], actual['log_offset'], actual['pk_end_offset']) ==
            (snapshot['snapshot_id'], snapshot['offset_exclusive'], snapshot['offset_exclusive']), 'Metadata snapshot/end mismatch')
    require(millis(metadata['timestamp']) <= captured_ms(snapshot), 'Metadata timestamp follows capture')
    admit_imports(table, context, metadata)
    admit_files(sql, receipts, snapshot, actual)
    return table, snapshot


def admit_imports(table, context, metadata):
    require(positive_integer(table['imports_end_offset_exclusive']), 'Invalid stopped PK end')
    imports = [job for job in context['jobs']['program'] if job['role'] == 'projection-import']
    require(len(imports) == 2 and table['import_job_ids'] == [job['jid'] for job in imports], 'Import job binding mismatch')
    require(all(job['state'] == 'FINISHED' and job['end-time'] <= millis(metadata['timestamp']) for job in imports), 'Snapshot predates finished imports')


def inspected_process(sql, receipt):
    records = raw_json(sql, receipt)
    require(isinstance(records, list) and len(records) == 1, 'Require exactly one inspected reader')
    process = records[0]
    require(all(isinstance(process[key], str) and bool(process[key]) for key in ['Id', 'Image']), 'Missing reader container/image identity')
    require(positive_integer(process['State']['Pid']), 'Reader process is not running')
    require(positive_integer(millis(process['State']['StartedAt'])), 'Missing process start time')
    return process


def admit_restart(sql, receipts):
    before = inspected_process(sql, receipts['reader_before'])
    after = inspected_process(sql, receipts['restart'])
    started = millis(after['State']['StartedAt'])
    require(millis(before['State']['StartedAt']) < started, 'Reader process was not replaced')
    require(before['Image'] == after['Image'], 'Reader image changed across restart')
    require(captured_ms(receipts['reader_before']) < started <= captured_ms(receipts['restart']), 'Restart capture timing mismatch')
    for name in ['snapshot', 'file_hashes', 'file_sizes']:
        require(captured_ms(receipts[name]) <= started, 'Snapshot/files were not captured before restart')
    connector = receipts['connector']
    require(connector['image_id'] == after['Image'], 'Connector image mismatch')
    require(bool(re.fullmatch('[0-9a-f]{64}', connector['sha256'])), 'Missing actual connector hash')
    tokens = evidence_file(sql, connector['raw']).read_text().split()
    require(len(tokens) == 2 and tokens[0] == connector['sha256'], 'Connector raw hash mismatch')
    require(tokens[1] == connector['jar_path'], 'Connector path mismatch')
    require(started <= captured_ms(connector), 'Connector capture predates restart')
    return after


def admit_job(sql, receipts, context, process):
    jobs = context['jobs']['verify']
    require(len(jobs) == 1, 'Require exactly one current verify job')
    job = jobs[0]
    actual = raw_json(sql, receipts['job'])
    for key in ['jid', 'name', 'state', 'start-time', 'end-time']:
        require(actual[key] == job[key], 'Verification job receipt mismatch: ' + key)
    require(job['state'] == 'FINISHED', 'Verification job did not FINISH')
    require(positive_integer(job['start-time']) and job['start-time'] <= job['end-time'], 'Invalid job window')
    before = raw_json(sql, receipts['jobs_before'])['jobs']
    require(job['jid'] not in {item['jid'] for item in before}, 'Verification job is stale')
    require(all(item['state'] in {'FINISHED', 'FAILED', 'CANCELED'} for item in before), 'Another job is active in proof window')
    started = millis(process['State']['StartedAt'])
    require(started <= captured_ms(receipts['jobs_before']) <= job['start-time'], 'Jobs-before window mismatch')
    sink = raw_json(sql, receipts['sink_before'])
    require(sink == {'path': str(sql / 'pk-cold'), 'files': []}, 'Verification sink is stale or unrelated')
    require(started <= captured_ms(receipts['sink_before']) <= job['start-time'], 'Fresh-sink window mismatch')
    require(job['end-time'] <= captured_ms(receipts['job']), 'Job capture predates completion')
    return job


def log_line(sql, receipt, selector, logger, pattern):
    line = receipt[selector]
    require(positive_integer(line), 'Invalid witness line number')
    text = evidence_file(sql, receipt['raw']).read_text().splitlines()[line - 1]
    require(logger in text and re.search(pattern, text) is not None, 'Missing exact ' + selector + ' witness')
    return millis(text.split()[0]), text


def split_text(table, snapshot):
    return (f"HybridSnapshotLogSplit{{tableBucket=TableBucket{{tableId={table['table_id']}, bucket=0}}, "
            f"partitionName='null', snapshotId={snapshot['snapshot_id']}, isSnapshotFinished=false, "
            f"logStartingOffset={snapshot['offset_exclusive']}, logStoppingOffset={snapshot['offset_exclusive']}, "
            "isBatch=true, recordsToSkip=0}")


def admit_reader(sql, receipts, job, process, table, snapshot):
    assignment, reader = receipts['assignment'], receipts['reader']
    require(assignment['job_id'] == reader['job_id'] == job['jid'], 'Witness job identity mismatch')
    require(reader['container_id'] == process['Id'] and reader['pid'] == process['State']['Pid'], 'Witness reader process mismatch')
    require(reader['started_at'] == process['State']['StartedAt'], 'Witness reader start mismatch')
    require((reader['table_id'], reader['bucket'], reader['snapshot_id']) ==
            (table['table_id'], 0, snapshot['snapshot_id']), 'Reader snapshot identity mismatch')
    split = reader['split_id']
    require(split == assignment['split_id'], 'Finished split identity mismatch')
    require(split == f"hybrid-snapshot-log-{table['table_id']}-0", 'Finished split table/bucket mismatch')
    assigned, text = log_line(sql, assignment, 'line', 'FlinkSourceEnumerator',
                              r'DEBUG.*Assigning splits to readers .*' + re.escape(split_text(table, snapshot)))
    require(text.count('HybridSnapshotLogSplit{') == 1, 'Ambiguous snapshot assignment')
    bucket = re.escape(f"TableBucket{{tableId={table['table_id']}, bucket=0}}")
    start, _ = log_line(sql, reader, 'download_start_line', 'KvSnapshotBatchScanner',
                        r'INFO.*Start to download kv snapshot files to local directory for bucket ' + bucket + r'\.')
    complete, _ = log_line(sql, reader, 'download_complete_line', 'KvSnapshotBatchScanner',
                           r'INFO.*Download kv snapshot files to local directory for bucket ' + bucket + r' cost \d+ ms\.')
    finished, _ = log_line(sql, reader, 'split_finished_line', 'FlinkSourceSplitReader',
                           r'INFO.*split ' + re.escape(split) + r' is finished(?:\s|$)')
    require(job['start-time'] <= assigned <= start <= complete <= finished <= job['end-time'], 'Witness outside job window or out of order')
    require(job['end-time'] <= min(captured_ms(assignment), captured_ms(reader)), 'Logs captured before job completion')


def result_rows(sql, context, job):
    paths = sorted(path for path in (sql / 'pk-cold').rglob('*') if path.is_file() and not path.name.startswith('.'))
    require(bool(paths), 'Missing cold result files')
    rows = []
    for path in paths:
        require(job['start-time'] <= path.stat().st_mtime_ns // 1000000 <= job['end-time'], 'Cold result file outside job window')
        require(digest(path) == context['outputs'][str(path.relative_to(sql))], 'Cold result hash changed')
        rows.extend(json.loads(line) for line in path.read_text().splitlines() if line.strip())
    return rows


def admit_rows(sql, context, job):
    require(digest(sql / 'expected.json') == context['expected_sha256'], 'Expected row hash changed')
    expected = json.loads((sql / 'expected.json').read_text())['projection']
    rows = result_rows(sql, context, job)
    require(bool(rows) and bool(expected), 'Cold proof requires nonempty rows')
    require(all(set(row) == EVENT_FIELDS for row in rows + expected), 'Cold proof requires exact nine-field rows')
    require(sorted(rows, key=lambda row: row['event_id']) == expected, 'Cold result differs from independent Java rows')


def admit_cold_proof(sql, path, context):
    if path is None:
        raise ProofRequired('Independent cold snapshot proof required; output equality alone is insufficient')
    proof = json.loads(path.read_text())
    require(type(proof['version']) is int and proof['version'] == 1, 'Unknown cold proof version')
    require(proof['method'] == 'INDEPENDENT_SNAPSHOT_CAUSAL_REVIEW', 'Unknown cold proof method')
    for key in ['run_id', 'context_sha256', 'source_manifest_sha256']:
        require(proof[key] == context[key], 'Cold proof binding mismatch: ' + key)
    require(proof['executed_verify_sql_sha256'] == context['launches']['verify']['executed.sql'], 'Cold proof SQL mismatch')
    require((proof['fluss_commit'], proof['fluss_tree']) == (FLUSS_COMMIT, FLUSS_TREE), 'Wrong pinned Fluss mechanism')
    require(proof['checked_by'].strip() and proof['independent_review'] is True, 'Independent proof review absent')
    require(datetime.fromisoformat(proof['checked_at_gst']).utcoffset().total_seconds() == 14400, 'Proof review must be GST')
    names = ['table', 'snapshot', 'file_hashes', 'file_sizes', 'reader_before', 'restart', 'connector',
             'assignment', 'reader', 'jobs_before', 'sink_before', 'job']
    receipts = {name: proof_receipt(sql, proof['evidence'][name]) for name in names}
    require(all(receipt['run_id'] == context['run_id'] for receipt in receipts.values()), 'Receipt run mismatch')
    table, snapshot = admit_snapshot(sql, receipts, context)
    process = admit_restart(sql, receipts)
    job = admit_job(sql, receipts, context, process)
    admit_reader(sql, receipts, job, process, table, snapshot)
    admit_rows(sql, context, job)
    return {'status': 'PASSED', 'method': 'SOURCE_BACKED_CAUSAL_WITNESS',
            'proof_sha256': digest(path), 'context_sha256': context['context_sha256']}
