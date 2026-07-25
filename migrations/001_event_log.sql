-- 001_event_log.sql — append-only event log (issue #3).
--
-- The log is the source of truth. `agent_runs.status` is a derived cache that
-- exists only so the dispatcher can index on it; replaying a run's events must
-- reproduce it exactly. `orchestrator/log.py` guarantees that by deriving both
-- from the same fold.

-- Status vocabulary is a CHECK constraint rather than a Postgres enum type:
-- widening a CHECK is a one-line ALTER, while adding an enum value needs a
-- migration and takes a lock. Same convention as feliu-dev.
CREATE TABLE agent_runs (
    id               bigserial PRIMARY KEY,
    kind             text        NOT NULL,
    subject          text        NOT NULL,
    status           text        NOT NULL,
    attempts         integer     NOT NULL DEFAULT 0,
    worker_id        text,
    lease_expires_at timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT agent_runs_status_check CHECK (status IN (
        'QUEUED', 'LEASED', 'RUNNING', 'AWAITING_HUMAN',
        'DONE', 'FAILED', 'ABANDONED'
    )),
    CONSTRAINT agent_runs_attempts_check CHECK (attempts >= 0)
);

-- At most one *open* run per (kind, subject). Terminal runs are excluded so the
-- same Sentry issue can legitimately come back weeks later and get a new run.
-- Issue #6 leans on this as its dedup backstop rather than trusting its own
-- bookkeeping.
CREATE UNIQUE INDEX agent_runs_open_subject_key
    ON agent_runs (kind, subject)
    WHERE status NOT IN ('DONE', 'FAILED', 'ABANDONED');

-- Dispatch path (#4): oldest QUEUED first.
CREATE INDEX agent_runs_dispatch_idx
    ON agent_runs (created_at)
    WHERE status = 'QUEUED';

-- Reconcile path (#4): expired leases.
CREATE INDEX agent_runs_lease_idx
    ON agent_runs (lease_expires_at)
    WHERE status IN ('LEASED', 'RUNNING');

CREATE TABLE agent_events (
    -- ON DELETE RESTRICT, deliberately not CASCADE: deleting a run must not
    -- silently erase its history. If a run ever needs to go, its events have to
    -- be dealt with explicitly.
    id         bigserial PRIMARY KEY,
    run_id     bigint      NOT NULL REFERENCES agent_runs (id) ON DELETE RESTRICT,
    seq        integer     NOT NULL,
    type       text        NOT NULL,
    payload    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT agent_events_seq_check CHECK (seq > 0),
    CONSTRAINT agent_events_run_seq_key UNIQUE (run_id, seq)
);

CREATE INDEX agent_events_type_idx ON agent_events (type, created_at);

-- Append-only is enforced, not merely intended. A REVOKE on the orchestrator's
-- role would be bypassed the moment anyone connects as the owner (which is how
-- migrations run), so the rule lives in the database instead of in the grants.
CREATE FUNCTION agent_events_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'agent_events is append-only (attempted %)', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER agent_events_no_mutate
    BEFORE UPDATE OR DELETE ON agent_events
    FOR EACH ROW EXECUTE FUNCTION agent_events_append_only();

CREATE TRIGGER agent_events_no_truncate
    BEFORE TRUNCATE ON agent_events
    FOR EACH STATEMENT EXECUTE FUNCTION agent_events_append_only();
