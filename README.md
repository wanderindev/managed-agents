# managed-agents

A poor man's managed-agents setup: a durable-log orchestrator that runs Claude
Code in disposable sandboxes on a personal DigitalOcean droplet.

**Outer harness only.** Claude Code stays the inner harness. Rebuilding that part
is the collision risk, so this repo does not try.

Roadmap and reasoning live in [issue #1](https://github.com/wanderindev/managed-agents/issues/1).

## The idea

The append-only event log is the source of truth. Sandboxes are disposable. The
orchestrator is stateless and reconstructs everything by replaying the log, so any
job can be killed and resumed without losing the work that came before it.

```
work source  ->  agent_runs (queue)  ->  sandbox container  ->  artifact  ->  human gate
                       |                        |
                       +----- agent_events (append-only log) ----+
```

The first workload is Sentry triage for `feliu-dev` and `panama-in-context`: poll
for unresolved issues, investigate in a sandbox, patch, test, open a draft PR,
have a fresh-context adversarial reviewer try to refute the patch, then email a
human. Nothing merges without a person.

## Layout

```
migrations/          numbered plain SQL, applied in filename order
orchestrator/
  config.py          environment, four values
  db.py              psycopg3 connection helper
  enums.py           run statuses and event types
  log.py             the append-only log and the status fold
  migrate.py         migration runner
tests/               real PostgreSQL via testcontainers
```

## Deliberate non-choices

- **No Kubernetes.** Pods buy isolation from untrusted code. This runs our own
  repos with our own Claude Code, so Docker plus a systemd timer is enough. If
  concurrent sandboxes ever become the bottleneck, revisit.
- **No ORM.** Two tables and a fold do not need a mapper.
- **No Alembic.** Numbered SQL files and a 60-line runner.
- **No headless browser on the droplet.** Automating claude.ai or
  gemini.google.com from a datacenter IP is fragile and would force a long-lived
  session cookie into a sandbox. Those steps get replaced with APIs or stay
  attended.
- **No secrets vault.** Short-lived, repo-scoped GitHub App installation tokens
  minted per job (issue #11) beat a vault for a two-credential system.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

pytest                    # spins up a PostgreSQL container
pytest --cov              # enforces fail_under in .coveragerc
ruff check . && ruff format --check .
```

Tests need a working Docker daemon; testcontainers starts `postgres:16`.

### Migrations

```bash
export ORCHESTRATOR_DATABASE_URL=postgresql://...
python -m orchestrator.migrate
```

Idempotent. Each file runs in its own transaction and is recorded in
`schema_migrations`, so a failure part way through a batch leaves the earlier
files applied and a retry resumes.

`agent_events` refuses `UPDATE`, `DELETE` and `TRUNCATE` via a trigger. That is
intentional and enforced in the database rather than in the grants, because
migrations connect as the owner and grants would not bind them.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `ORCHESTRATOR_DATABASE_URL` | local dev DSN | log database, its own DB on the managed instance |
| `ORCHESTRATOR_DB_POOL_MAX` | `2` | keep off the managed instance's connection headroom |
| `ORCHESTRATOR_MAX_CONCURRENT_RUNS` | `1` | in-flight runs |
| `ORCHESTRATOR_WORKER_ID` | `orchestrator-1` | identifies this process in `agent_runs.worker_id` |
