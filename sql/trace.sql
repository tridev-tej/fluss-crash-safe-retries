SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
SELECT attempt, step, event_type, action_id
FROM fluss_catalog.retry_lab.job_events
WHERE job_id = '@TRACE_JOB@'
ORDER BY attempt, step;
