# managed-agents

A poor man's managed-agents setup: a durable-log orchestrator that runs Claude
Code in disposable sandboxes on a personal DigitalOcean droplet.

**Outer harness only.** Claude Code stays the inner harness. Rebuilding that part
is the collision risk, so this repo does not try.

This repo is a periodic public snapshot (as of October 16, 2026) of a private working repo, so the issue numbers cited in code and docs (#NN) refer to that private tracker. The design write-up, with the live record and the lessons, is at [feliu.dev/projects/managed-agents](https://feliu.dev/projects/managed-agents).

## The idea

The append-only event log is the source of truth. Sandboxes are disposable. The
orchestrator is stateless and reconstructs everything by replaying the log, so any
job can be killed and resumed without losing the work that came before it.

```
work source  ->  agent_runs (queue)  ->  sandbox container  ->  artifact  ->  adversarial review  ->  human gate
                       |                        |                                     |
                       +----- agent_events (append-only log) ----+--------------------+
```

Five workloads run through the loop today. Sentry triage polls unresolved
issues on `feliu-dev`, `panama-in-context`, `atelier-loyalty-app` and the
storefront theme (`atelier-new-cli`), investigates in a sandbox, and
lands one of four verdicts: a fix with a failing-then-passing test and a draft
PR, a `MITIGATION` that ships a self-contained partial change and leaves the
issue open, `NOT_A_BUG` with evidence, or `NEEDS_HUMAN` with a brief. Every FIX is
attacked by a fresh-context adversarial reviewer that must return STANDS before
the PR leaves draft. The change-request loop turns human review comments on the
orchestrator's own PRs into guarded revision runs, and the same hourly poll
closes a parked run once a human merges or closes its PR, so a regression of a
fixed issue is triaged again after the cooldown. A weekly dreaming job
audits each repo's memory file against the week's runs and merged pull
requests, accumulating its findings onto one open pull request until a human
merges it. Every day a
driver works PIC's agent-task queue through its article series (one a day or
one a week, as PIC's cadence says), reporting steps that wait on the operator
(deep research, Grammarly) as WAITING and emailing what they wait for, with a
rubric verifier grading output against a rubric written at planning time.
When a goal fails or stalls, an hourly pipeline supervisor diagnoses it in a
read-only sandbox through PIC's MCP server and emails the diagnosis; behind a
kill switch, the orchestrator host can carry out a recommended retry within
limits written in code. Nothing merges without a person.

## Layout

```
migrations/          numbered plain SQL, applied in filename order
orchestrator/
  auth.py            dead-login detection: pauses the queue instead of burning runs
  auto_retry.py      supervisor phase 2: host-side PIC task retries, limited
  config.py          environment, read once
  db.py              psycopg3 connection helper
  dream.py           weekly memory-audit ("dreaming") entry point
  driver.py          daily driver for PIC's agent-task queue
  enums.py           run statuses and event types
  github.py          GitHub App auth, per-job installation tokens
  jobs.py            job specs, prompts, and the followup chain
  log.py             the append-only log and the status fold
  loop.py            the stateless loop: reconcile, then dispatch
  main.py            entry point for the loop
  mcp_client.py      minimal host-side MCP client (initialize + tools/call)
  migrate.py         migration runner
  notify.py          outcome email; every silence explains itself
  pic_mcp.py         read-only PIC MCP config for PIC jobs' sandboxes
  poll.py            work-source poll: Sentry issues, PR change requests
  queue.py           queue reads, the lease write, `cancel` CLI, coalescing
  redact.py          keeps the orchestrator's own secrets out of the log and email
  report.py          read-only Markdown reports over the log (`baseline`, `supervisor`)
  research_gate.py   research-gate experiment: write, rubric-verify, revise
  runner.py          Runner protocol, the seam to container mechanics
  sandbox.py         the Docker runner
  supervisor.py      pipeline supervisor: diagnose stuck PIC goals (read-only sandbox)
  sources/           the poll's work sources: sentry.py, github_prs.py
docker/sandbox/      the disposable sandbox image
scripts/             host provisioning, sandbox launch
docs/runbook.md      how the host and database were built, and what bit
docs/results/        published report output (baseline.md, pipeline-supervisor.md)
tests/               real PostgreSQL via testcontainers
```

The loop keeps nothing between ticks and nothing between processes. A sandbox
handle lives in its `sandbox_started` event payload rather than in memory, so a
restarted orchestrator finds its containers by reading the log. There is no boot
path: a tick is idempotent and self-healing, so the first tick after a restart
reconciles whatever it inherits.

## Deliberate non-choices

- **No Kubernetes.** Pods buy isolation from untrusted code. This runs our own
  repos with our own Claude Code, so Docker plus a crontab is enough. If
  concurrent sandboxes ever become the bottleneck, revisit.
- **No ORM.** Two tables and a fold do not need a mapper.
- **No Alembic.** Numbered SQL files and a 70-line runner.
- **No headless browser on the droplet.** Automating claude.ai or
  gemini.google.com from a datacenter IP is fragile and would force a long-lived
  session cookie into a sandbox. Those steps get replaced with APIs or stay
  attended.
- **No secrets vault.** The credentials are few, and each is short-lived or
  narrowly scoped: GitHub App installation tokens minted per job and expiring
  in an hour, plus scoped Sentry and PIC tokens. The one that cannot be scoped
  down is Claude's own login.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

pytest                    # spins up a PostgreSQL container
pytest --cov              # enforces fail_under in .coveragerc
ruff check . && ruff format --check .
```

Tests need a working Docker daemon; testcontainers starts `postgres:17`.

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

So queued work is never cleared by hand either. The supported way writes proper
events (`run_abandoned` marked `cancelled`) and only touches QUEUED runs:

```bash
python -m orchestrator.queue cancel --kind memory_dream --older-than 1d --dry-run
python -m orchestrator.queue cancel --kind memory_dream --older-than 1d
```

`docs/runbook.md` ("When the login dies anyway") covers when to use it.

Reading the log is just as supported. `orchestrator.report` prints read-only
Markdown over `agent_runs` / `agent_events`; `baseline` (#79) covers driver
sessions, sandbox runs by workload (duration, cost, tokens, outcomes) and the
login-expiry pauses, with subjects and error text folded into fixed buckets so
the output is safe to publish. It needs the DB, so run it on the droplet:

```bash
python -m orchestrator.report baseline [--since YYYY-MM-DD]
```

The latest output is kept in `docs/results/baseline.md`.
`python -m orchestrator.report supervisor` reports the pipeline supervisor's
time to diagnosis and its agreement with the operator; its latest output is in
`docs/results/pipeline-supervisor.md`.

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
| `ORCHESTRATOR_PIC_MCP_READ_TOKEN` | empty | read-scope PIC MCP token handed to PIC jobs' sandboxes (`mcp__pic__*` tools); redacted from the log and emails. Empty = no MCP (runbook: "PIC MCP") |
| `ORCHESTRATOR_PIC_MCP_URL` | `https://api.panamaincontext.com/mcp` | PIC's MCP endpoint |
| `ORCHESTRATOR_PIC_MCP_PIPELINE_TOKEN` | empty | `read,pipeline` PIC MCP token held by the orchestrator process only (never a sandbox), for the supervisor's auto-retry; redacted from the log and emails (runbook: "Pipeline supervisor") |
| `ORCHESTRATOR_PIC_MCP_TIMEOUT_SECONDS` | `30` | HTTP timeout for one host-side MCP request (the auto-retry's `retry_agent_task` calls) |
| `ORCHESTRATOR_SUPERVISOR_AUTO_RETRY` | off | kill switch: `1` lets the orchestrator carry out a diagnosis's `retry` via `retry_agent_task` (one per task per rolling 24h, `_MAX_RETRIES_PER_RUN` (3) per diagnosis, never skip or publish). Needs the pipeline token; off = read-only |
| `ORCHESTRATOR_DRIVER_CADENCE` | `daily` | goal-key shape `driver --dry-run` lists (`daily_series:` or `weekly_series:`); mirror PIC's `WEEKLY_PLAN_DAILY_CADENCE`. A real drive takes goal keys from PIC |
| `ORCHESTRATOR_SUPERVISOR_STALL_DAYS` | `3` | days a PIC goal's counts may sit unchanged in the log before `orchestrator.supervisor` diagnoses it (runbook: "Pipeline supervisor"); also `_RECENT_HOURS` (48), `_LOOKBACK_DAYS` (30), `_MAX_PER_SCAN` (3), `_MAX_TURNS` (15), `_MODEL` |
| `ORCHESTRATOR_DREAM_REPOS` | empty | comma list of repos the weekly dream audits; empty = every repo with orchestrator runs in the window. Each needs the App installed and a `/srv/repos` clone (runbook: "Dreaming") |
| `ORCHESTRATOR_DREAM_MODEL` | `claude-opus-5-5` | model the dreaming job runs on; set `claude-opus-5` to roll back |
| `ORCHESTRATOR_DREAM_CLAUDE_MD_MAX_CHARS` | `40000` | largest CLAUDE.md a dream may leave; over it the dreamer splits detail into `docs/claude/<topic>.md`, and a run still over it parks for a human (runbook: "Dreaming") |
