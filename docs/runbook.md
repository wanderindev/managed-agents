# Runbook

How the orchestrator host and its log database were built, and the traps that
cost time the first time. Issue #2.

## Host

| | |
|---|---|
| Droplet | `agents`, id 587510123 |
| Address | 159.223.174.185, private 10.136.109.229 |
| Size | `s-2vcpu-4gb`, 80GB, $24/mo |
| Region / VPC | nyc1, `default-nyc1` |
| Operator user | `wanderindev` (in the `docker` group) |

4GB rather than 2GB because a sandbox holds Claude Code, a Postgres
testcontainer, and a full test suite at the same time. 2GB OOMs on the first
real run and costs more in debugging than the $12 saves.

**Not** the prod `pic` droplet (474003017). That host's frontend container is the
single nginx entry point for every site, with SSL at `/opt/letsencrypt`. Keep
this cluster entirely separate.

```bash
scp scripts/provision-droplet.sh root@159.223.174.185:/root/
ssh root@159.223.174.185 'bash /root/provision-droplet.sh'
```

Then, once, by hand:

```bash
ssh wanderindev@159.223.174.185
claude          # interactive OAuth, cannot be automated
```

### Traps

- **A fresh droplet holds the dpkg lock.** cloud-init is still running apt for
  the first minute or two, so the very first provisioning run dies on "could not
  get lock". The script now waits it out.
- **Claude Code requires Node >= 22.** On Node 20, `npm install -g` does not
  fail. It prints an `EBADENGINE` warning and installs the last 20-compatible
  release instead, which strands the host on an old CLI that cannot reach
  current models. This cost a version-mismatch investigation. Do not silence
  npm's output here.

### Verifying the host

```bash
ssh wanderindev@159.223.174.185
cd /tmp && claude -p "Reply with exactly: SMOKE OK" \
    --model claude-opus-5 --output-format stream-json --verbose
```

Confirmed on 2026-07-25 with Claude Code 2.1.220 and Node 22.23.1. Two things
worth reading in that output:

- `"apiKeySource":"none"` — the run is on the Max subscription, not an API key.
  That is the premise the whole project's affordability rests on.
- A `rate_limit_event` line reports a `five_hour` window. The sandbox runner
  (#5) has to log these; hitting the limit mid-run is a real failure mode for an
  unattended orchestrator, not a theoretical one.

## Sandbox image

One disposable container per run. Built from `docker/sandbox/`.

```bash
cd /srv/managed-agents
docker build -t managed-agents/sandbox:latest docker/sandbox
scripts/run-sandbox.sh /srv/worktrees/<job> "<prompt>"
```

Contains Node 22, a pinned Claude Code, git, `gh`, Python, and the Docker
**client**. Nothing job-specific is baked in: the worktree, the credential and
any secrets arrive at run time.

### The credential is mounted read-write, deliberately

Claude Code refreshes its OAuth token. A read-only mount works right up until the
access token expires, and then every unattended run fails at once. So
`~/.claude/.credentials.json` (471 bytes) is bind-mounted read-write.

Only that one file. The host's `~/.claude.json` is 38KB of machine id, feature
caches and every project the CLI has ever opened, and none of that belongs in a
sandbox. The entrypoint writes a minimal replacement containing just
`hasCompletedOnboarding`, without which `claude -p` refuses to run.

This is the one long-lived secret a sandbox has to hold, and there is no way
around it because running the CLI is the entire point. #11 gets GitHub down to
1-hour scoped tokens; this credential cannot be reduced the same way, which is
worth stating plainly rather than pretending the container solves it.

### `bypassPermissions`, and what the Docker socket costs

The entrypoint defaults to `--permission-mode bypassPermissions`. That is right
*here* and nowhere else: the container is disposable, holds one worktree, and an
interactive approval prompt in an unattended job just deadlocks.

The honest caveat: `AGENT_WITH_DOCKER=1` mounts the host's Docker socket, which
a job needs to run feliu-dev's testcontainers suite or PIC's compose commands.
That hands the job control of the host Docker daemon, so with it enabled the
container is an *isolation* boundary, not a *security* boundary. Acceptable
under this project's stated trust model (our repos, our Claude Code, per #1),
but it means "sandbox" is a slight overclaim and the flag should stay opt-in.

Runtime limits are set in `scripts/run-sandbox.sh`: 3g memory, 4g with swap,
512 pids, `--init` so the CLI's child processes get reaped.

## Log database

Managed cluster `d699bab0-c322-4b58-b90b-98bae0632b30`, PostgreSQL **17.10**,
2GB / 50 connections, shared with `feliudev` and `loyalty`.

```bash
C=d699bab0-c322-4b58-b90b-98bae0632b30
doctl databases db create       $C orchestrator
doctl databases user create     $C orchestrator
doctl databases firewalls append $C --rule droplet:587510123
```

### Traps

- **The firewall rule is mandatory, not hardening.** The cluster uses a
  trusted-sources allowlist. Without the rule the droplet cannot reach Postgres
  at all.
- **PostgreSQL 15 removed the default `CREATE` privilege on schema `public` for
  non-owner roles.** A freshly created managed user connects fine and then fails
  on the first `CREATE TABLE`. Run once, as `doadmin`, against the new database
  only:

  ```sql
  GRANT CREATE, USAGE ON SCHEMA public TO orchestrator;
  ```

- **Use the private connection host** (`doctl databases connection <id>
  --private`) so traffic stays inside the VPC.
- Keep `ORCHESTRATOR_DB_POOL_MAX` small. The instance is 50 connections total
  and feliu-dev and PIC were already trimmed to 2 and 3 to fit.

### Environment

`/srv/orchestrator.env`, chmod 600, owned by `wanderindev`:

```
ORCHESTRATOR_DATABASE_URL=postgresql://orchestrator:<pw>@<private-host>:25060/orchestrator?sslmode=require
ORCHESTRATOR_DB_POOL_MAX=2
ORCHESTRATOR_MAX_CONCURRENT_RUNS=1
ORCHESTRATOR_WORKER_ID=agents-1
```

### Migrations

```bash
ssh wanderindev@159.223.174.185
cd /srv/orchestrator
set -a; . /srv/orchestrator.env; set +a
.venv/bin/python -m orchestrator.migrate
```

Idempotent; a second run reports `nothing to apply`.

### Smoke-testing the log by hand

`agent_events` is append-only, so **a test row cannot be deleted afterwards**.
Only ever exercise it inside a transaction that rolls back:

```sql
BEGIN;
INSERT INTO agent_runs (kind, subject, status) VALUES ('smoke', 'smoke:1', 'QUEUED');
-- ...
ROLLBACK;
```
