# managed-agents

A durable-log orchestrator that runs Claude Code in disposable sandboxes on a
dedicated DigitalOcean droplet (`agents`, <agents-droplet-ip> — **not** the prod
`pic` droplet). This repo is **public**: never commit secrets, tokens, DSNs or
anything from `/srv/orchestrator.env`.

- `README.md` — the idea, the layout, development setup, configuration knobs.
- `docs/runbook.md` — how the host, sandbox image, GitHub App and log database
  were built, and the traps that cost time.
- Issues #1–#15 hold the original build log.

## Shipping

Merging happens only through GitHub auto-merge, which fires once the required
check **`test`** passes. Never merge by hand: no `--admin`, no merge API call.

**Before the PR** — mirror `.github/workflows/ci.yml` locally (Python 3.11;
tests need a Docker daemon for testcontainers):

```bash
pip install -r requirements-dev.txt
ruff check . && ruff format --check .
pytest -q --cov          # enforces fail_under = 90 from .coveragerc
```

**What merging does** — nothing on the droplet. There is no deploy-on-merge:
`/srv/orchestrator` is a plain copy of `orchestrator/`, `migrations/`, `docs/`,
`scripts/` and `requirements.txt` (not a git checkout), run from
`/srv/orchestrator/.venv`. Cron there runs `~/bin/orch-loop-keepalive.sh` every 5 min
(starts `orchestrator.main` if it is not running; the cron copy runs from
`~/bin`, so a change to `scripts/orch-loop-keepalive.sh` is deployed by copying it
there) and `orch-run.sh
orchestrator.poll` / `orchestrator.dream` / `orchestrator.supervisor` as one-shots; all log to `~/logs/`.
The sandbox image is built from another plain copy, `/srv/managed-agents`: a
change to `docker/sandbox/` (e.g. `entrypoint.sh`) is deployed by copying it
there and running `docker build -t managed-agents/sandbox:latest docker/sandbox`
from that directory.

**Deploy / verify** — compare the droplet's copy with the repo, and check the loop:

```bash
ssh wanderindev@<agents-droplet-ip> 'cd /srv/orchestrator && md5sum orchestrator/*.py orchestrator/sources/*.py migrations/*.sql' | LC_ALL=C sort -k2
md5sum orchestrator/*.py orchestrator/sources/*.py migrations/*.sql | LC_ALL=C sort -k2   # compare
ssh wanderindev@<agents-droplet-ip> 'pgrep -af orchestrator.main; tail -n 30 ~/logs/orchestrator-main.log'
```

After a merge, do it yourself in this order: copy the changed files to the
droplet, apply any migrations the PR added (below), then restart the running
loop, because it keeps old code in memory until killed. Restarting before the
migration runs new code against the old schema. Keepalive starts the loop
again only if it is not running. SIGTERM lets the loop finish its current
tick first, so wait for it to exit before running keepalive, or keepalive sees
it still alive and starts nothing. Match the loop with keepalive's anchored
pattern: a bare `pkill -f orchestrator.main` also matches the remote shell
running this very command, whose text names the module (#72).

```bash
ssh wanderindev@<agents-droplet-ip> 'P="^([^ ]*/)?python[0-9.]*( -[^ ]+)* -m orchestrator\.main( |$)"; pkill -f "$P"; while pgrep -f "$P" >/dev/null; do sleep 1; done; ~/bin/orch-loop-keepalive.sh'
```

Say in the report which files were copied, which migrations ran, and that the
loop was restarted.

**Migrations** — numbered SQL in `migrations/`, applied by
`python -m orchestrator.migrate` (idempotent; "nothing to apply" on a re-run).
The DB is reachable only from the droplet (VPC + trusted-sources firewall), so
copy the merged files first, then apply the migrations from the droplet:

```bash
ssh wanderindev@<agents-droplet-ip> 'cd /srv/orchestrator && set -a && . /srv/orchestrator.env && set +a && .venv/bin/python -m orchestrator.migrate'
```

Never hand-write rows into `agent_events` — it is append-only by trigger.

**No operator gate on the agents droplet** (operator decision 2026-10-12).
Agents may copy code to and restart anything on the droplet, edit
`/srv/orchestrator.env` or the crontab, rebuild the sandbox image, re-run
`provision-droplet.sh`, install or rotate a GitHub App or Sentry token the
operator has minted, and run any migration. Say in the report what was done.
What an agent cannot do is the browser steps: `claude auth login` (an
interactive login) and minting a new GitHub App or Sentry token. Hand those to
the operator.
