-- 002_backoff.sql — back off before retrying an abandoned run (issue #20).
--
-- Without this, a requeued run is claimable the moment it is abandoned, so the
-- very next tick picks it up. With the defaults (15s ticks, 3 attempts) a
-- persistently failing run exhausts every attempt in about 45 seconds, which
-- turns transient problems — a Docker daemon mid-restart, a managed-Postgres
-- failover, a five-hour rate limit — into permanent give-ups.
ALTER TABLE agent_runs
    ADD COLUMN not_before timestamptz NOT NULL DEFAULT now();

-- Rebuild the dispatch index with not_before as an index condition. Backed-off
-- rows would otherwise accumulate at the head of the created_at scan and be
-- walked past (heap fetch included) on every claim. As an index condition they
-- are never visited at all.
--
-- Side effect worth naming: a backed-off run no longer head-of-line blocks the
-- queue. It keeps its created_at position, but while its window is open the
-- claim query simply does not see it, so newer work runs instead.
DROP INDEX agent_runs_dispatch_idx;
CREATE INDEX agent_runs_dispatch_idx
    ON agent_runs (not_before, created_at, id)
    WHERE status = 'QUEUED';
