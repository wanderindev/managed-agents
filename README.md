# managed-agents

A poor man's managed-agents setup: a durable-log orchestrator that runs Claude
Code in disposable sandboxes on a personal DigitalOcean droplet.

**Outer harness only.** Claude Code stays the inner harness. Rebuilding that part
is the collision risk, so this repo does not try.

The build log lives in [issues #1–#15](https://github.com/wanderindev/managed-agents/issues?q=is%3Aissue) (all closed; the epic is complete). The design write-up, with the live record and the lessons, is at [feliu.dev/projects/managed-agents](https://feliu.dev/projects/managed-agents).

## The idea

The append-only event log is the source of truth. Sandboxes are disposable. The
orchestrator is stateless and reconstructs everything by replaying the log, so any
job can be killed and resumed without losing the work that came before it.

```
work source  ->  agent_runs (queue)  ->  sandbox container  ->  artifact  ->  adversarial review  ->  human gate
                       |                        |                                     |
                       +----- agent_events (append-only log) ----+--------------------+
```

Three workloads run through the loop today. Sentry triage polls unresolved
issues on `feliu-dev` and `panama-in-context`, investigates in a sandbox, and
lands one of three verdicts: a fix with a failing-then-passing test and a draft
PR, `NOT_A_BUG` with evidence, or `NEEDS_HUMAN` with a brief. Every FIX is
attacked by a fresh-context adversarial reviewer that must return STANDS before
the PR leaves draft. The change-request loop turns human review comments on the
orchestrator's own PRs into guarded revision runs. A nightly dreaming job
audits each repo's memory file against digests of recent runs. On Saturdays a
driver works PIC's agent-task queue through a weekly article series, with a
rubric verifier grading output against a rubric written at planning time.
Nothing merges without a person.

## Layout

```
migrations/          numbered plain SQL, applied in filename order
orchestrator/
  config.py          environment, read once
  db.py              psycopg3 connection helper
  dream.py           nightly memory-audit ("dreaming") entry point
  driver.py          Saturday driver for PIC's agent-task queue
  enums.py           run statuses and event types
  github.py          GitHub App auth, per-job installation tokens
  jobs.py            job specs, prompts, and the followup chain
  log.py             the append-only log and the status fold
  loop.py            the stateless loop: reconcile, then dispatch
  main.py            entry point for the loop
  migrate.py         migration runner
  notify.py          outcome email; every silence explains itself
  poll.py            work-source poll: Sentry issues, PR change requests
  queue.py           queue reads and the lease write
  research_gate.py   research-gate experiment: write, rubric-verify, revise
  runner.py          Runner protocol, the seam to container mechanics
  sandbox.py         the Docker runner
  sources/           the poll's work sources: sentry.py, github_prs.py
docker/sandbox/      the disposable sandbox image
scripts/             host provisioning, sandbox launch
docs/runbook.md      how the host and database were built, and what bit
tests/               real PostgreSQL via testcontainers
```

The loop keeps nothing between ticks and nothing between processes. A sandbox
handle lives in its `sandbox_started` event payload rather than in memory, so a
restarted orchestrator finds its containers by reading the log. There is no boot
path: a tick is idempotent and self-healing, so the first tick after a restart
reconciles whatever it inherits.

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

Everything is read once at import in `orchestrator/config.py`, which documents
the full set (loop, sandbox, Sentry poll, GitHub App, notifier, PIC driver);
`docs/runbook.md` covers how each was provisioned. The knobs that matter most:

| Variable | Default | Purpose |
|---|---|---|
| `ORCHESTRATOR_DATABASE_URL` | local dev DSN | log database, its own DB on the managed instance |
| `ORCHESTRATOR_MAX_CONCURRENT_RUNS` | `1` | in-flight runs |
| `ORCHESTRATOR_TICK_SECONDS` | `15` | loop cadence; also the heartbeat/transcript-drain interval |
| `ORCHESTRATOR_SANDBOX_IMAGE` | `managed-agents/sandbox:latest` | the disposable sandbox image |
| `ORCHESTRATOR_SANDBOX_WITH_DOCKER` | `0` | mount the Docker socket, only for jobs that run test suites |
| `ORCHESTRATOR_SENTRY_TOKEN` | empty | Sentry API token for the triage poll |
| `ORCHESTRATOR_GITHUB_APP_ID` (+ key path, installation id) | empty | mints the per-job installation tokens |
| `ORCHESTRATOR_NOTIFY_TO` / `_FROM` | empty | where outcome emails go |
| `ORCHESTRATOR_PIC_DRIVER_TOKEN` | empty | scoped token for PIC's agent-task queue |
