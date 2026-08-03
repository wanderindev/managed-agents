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
**client**. Nothing job-specific is baked in: the workspace clone, the
credential and any secrets arrive at run time.

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
*here* and nowhere else: the container is disposable, holds one job's clone, and
an interactive approval prompt in an unattended job just deadlocks.

The honest caveat: `AGENT_WITH_DOCKER=1` mounts the host's Docker socket, which
a job needs to run feliu-dev's testcontainers suite or PIC's compose commands.
That hands the job control of the host Docker daemon, so with it enabled the
container is an *isolation* boundary, not a *security* boundary. Acceptable
under this project's stated trust model (our repos, our Claude Code, per #1),
but it means "sandbox" is a slight overclaim and the flag should stay opt-in.

Runtime limits are set in `scripts/run-sandbox.sh`: 3g memory, 4g with swap,
512 pids, `--init` so the CLI's child processes get reaped.

## Running the orchestrator

```bash
ssh wanderindev@159.223.174.185
cd /srv/orchestrator
set -a; . /srv/orchestrator.env; set +a
.venv/bin/python -m orchestrator.main
```

`SIGTERM` and `SIGINT` ask it to stop after the current tick. Killing it outright
is also safe: the loop is built to be resumed, and the first tick of the next
process reconciles whatever it inherits.

Enqueue an end-to-end check:

```python
from orchestrator.db import connect
from orchestrator.log import create_run

with connect() as conn:
    create_run(conn, "smoke", "smoke:1")
    conn.commit()
```

A finished run's `agent_events` are meant to be enough on their own:

```sql
SELECT seq, type, left(payload::text, 90) FROM agent_events WHERE run_id = 2 ORDER BY seq;
```

```
 1 | run_queued      | {"why": "issue #5 end to end"}
 2 | run_leased      | {"attempt": 1, "worker_id": "agents-1"}
 3 | sandbox_started | {"container": "ma-run-2-1"}
 4 | claude_event    | {"cwd": "/workspace", "type": "system", ...
 …
 8 | claude_event    | {"type": "rate_limit_event", ...
10 | claude_event    | {"type": "result", ...
11 | stage_completed | {"result": {"ok": true, "checked": "sandbox"}, "outcome": "SUCCEEDED", ...
12 | run_done        | {"exit_code": 0}
```

### Where a job's pieces live

| Path | Contents |
|---|---|
| `/srv/repos/<repo>` | host template clone; each job gets a standalone `git clone --local` off it (#33 — a worktree's `.git` links at a host path the sandbox can't see) |
| `/srv/worktrees/ma-run-<id>-<attempt>` | the job's standalone clone, `origin` pointed at GitHub, mounted at the SAME path inside the container (#43 — the host daemon resolves bind mounts against its own filesystem, so a container-only `/workspace` made repo tooling that mounts the repo into a sibling container silently mount empty dirs; directory keeps its worktree-era name) |
| `/srv/jobs/ma-run-<id>-<attempt>` | `prompt.txt` in, `result.json` out, mounted at `/work` |

The prompt lives outside the repo on purpose, so a job cannot commit its own
instructions. Prompts reference the workspace as `/workspace`; the runner
rewrites that to the real per-attempt path when it writes `prompt.txt`. The
workspace is always deleted when the run finishes — a job's work reaches the
world by being pushed to origin from inside the sandbox, never by surviving
on disk.

## Sentry work source

```bash
python -m orchestrator.poll            # fetch, filter, enqueue
python -m orchestrator.poll --dry-run  # same, but enqueue nothing
```

A separate command from the loop rather than a step inside a tick, so scheduling
is the operating system's job and there is no "when did I last poll" state to
keep anywhere. Hourly is the intended cadence. Running it twice by accident is
harmless: the dedup and the unique index both hold.

### The token is a manual step

Create a Sentry auth token by hand (there is no API for minting one) with
**`event:read`** and **`org:read`**, and add it to `/srv/orchestrator.env`:

```
ORCHESTRATOR_SENTRY_TOKEN=...
```

The org is `javier-feliu` and it lives in the **US region**, so the base URL is
`https://us.sentry.io`, not `sentry.io`. Sending to the wrong region fails.

### Which projects, and which are deliberately excluded

| Sentry project | Repo |
|---|---|
| `trd-python`, `trd-javascript-react` | `feliu-dev` |
| `pic-python-fastapi`, `pic-javascript-react` | `panama-in-context` |

`atelier-loyalty-app` and `pic-cert-watcher` are real projects in the same org
and are deliberately absent: there is no clone of either here, so an agent could
only ever report that it cannot help.

### Tuning the filters

`--dry-run` prints the same drop tally without enqueuing, which is how a filter
change gets evaluated before anyone lives with it. Every drop is counted **by
reason**, because a source that filters silently reads as "nothing was wrong"
when it actually means "I decided none of this was worth your time".

`min_events` defaults to **1**, not 2. These are low-traffic personal sites, so a
single occurrence is already signal rather than noise. A floor of 2 was tried
first and threw away 10 of 25 issues, including `PIC-PYTHON-FASTAPI-1H`, a real
missing-column error that had happened exactly once. Raise it with
`ORCHESTRATOR_SENTRY_MIN_EVENTS` if the volume ever justifies it.

At a floor of 1 the ungroupable-issue filter starts earning its keep: an issue
with no usable title *and* no culprit gives an agent nothing to start from, so
it is dropped rather than turned into a run that can only conclude the same.

### Closing the loop back to Sentry

The GitHub integration is installed, which means a commit message containing
`Fixes PIC-PYTHON-FASTAPI-1Q` **resolves that issue automatically when the PR
merges**. #7 should put the short id in every commit it writes. The orchestrator
therefore never needs write access to Sentry: resolution is a side effect of a
human merging, which is exactly where the decision belongs.
## GitHub App (#11)

A GitHub App, not a personal access token. Per job the orchestrator mints an
**installation token**: one hour, scoped to the two installed repos, limited to
the two permissions granted. A PAT would carry the whole account and never
expire; a Claude connector would not help at all, because `git clone` and
`git push` need a credential in the git transport rather than an API tool.

### Creating it, by hand

There is no API for creating an App, so this part is a browser job.

1. github.com/settings/apps → **New GitHub App**.
2. Name it something recognisable in a PR author line, e.g. `wanderindev-agents`.
   Homepage URL can be the repo. Uncheck **Webhook → Active**; nothing listens.
3. Repository permissions, and nothing else:
   - **Contents: Read and write** (push a branch)
   - **Pull requests: Read and write** (open and update a PR)
4. Create, then **Generate a private key**. A `.pem` downloads once.
5. **Install App** → *Only select repositories* → `feliu-dev` and
   `panama-in-context`. Not "All repositories".

### Putting it on the droplet

The key goes in a file rather than an environment variable: it is multi-line, and
a file can be `chmod 600` while an env var is readable by anything that can see
`/proc` for the process.

```bash
ssh root@159.223.174.185 'install -d -o wanderindev -g wanderindev -m 700 /srv/secrets'
scp your-app.private-key.pem wanderindev@159.223.174.185:/srv/secrets/github-app.pem
ssh wanderindev@159.223.174.185 'chmod 600 /srv/secrets/github-app.pem'
```

Then add the App id to `/srv/orchestrator.env`:

```
ORCHESTRATOR_GITHUB_APP_ID=<from the App settings page>
```

### Finding the installation id, and checking the blast radius

```bash
python -m orchestrator.github
```

Prints every installation with its id and whether it is scoped to `selected` or
`all` repositories. Add the id as `ORCHESTRATOR_GITHUB_APP_INSTALLATION_ID` and
run it again: it then lists the repositories the App can actually reach and
**warns if that is more than the intended two**. Installing on "all repositories"
is one careless click during setup and it would hand every sandbox far more reach
than this is meant to give.

### How the token reaches the work

`JobSpec.needs_github` is opt-in, so a job with no business pushing never
receives a credential at all. When set, the runner mints a token at container
launch (so the sandbox gets the full hour, not the remains of an earlier one) and
injects it as `GH_TOKEN`.

Inside the container the entrypoint configures a git credential helper that reads
`GH_TOKEN` from the environment, rather than writing it into a remote URL. That
matters because the clone's `.git/config` lives on the host, and a token baked
into it would outlive the container that was supposed to contain it. The runner's
own host-side fetch passes its tokenized URL per command for the same reason.

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
