CREATE TABLE fluss_catalog.retry_lab.events_by_id (
    job_id STRING,
    event_id STRING NOT NULL,
    attempt INT,
    step INT,
    event_type STRING,
    action_id STRING,
    payload_digest STRING,
    emitted_at_ms BIGINT,
    detail STRING,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'bucket.num' = '1',
    'table.log.ttl' = '0ms',
    'table.replication.factor' = '1'
);
INSERT INTO fluss_catalog.retry_lab.events_by_id
SELECT job_id, event_id, attempt, step, event_type, action_id,
       payload_digest, emitted_at_ms, detail
FROM fluss_catalog.retry_lab.job_events
WHERE job_id IN ('@TRACE_JOB@', '@DUPLICATE_JOB@');
