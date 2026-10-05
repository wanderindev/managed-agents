# managed-agents

A durable-log orchestrator that runs Claude Code in disposable sandboxes on a
dedicated DigitalOcean droplet (`agents`, 159.223.174.185 — **not** the prod
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
orchestrator.poll` / `orchestrator.dream` as one-shots; all log to `~/logs/`.

**Deploy / verify** — read-only checks you may run yourself:

```bash
ssh wanderindev@159.223.174.185 'cd /srv/orchestrator && md5sum orchestrator/*.py orchestrator/sources/*.py migrations/*.sql' | LC_ALL=C sort -k2
md5sum orchestrator/*.py orchestrator/sources/*.py migrations/*.sql | LC_ALL=C sort -k2   # compare
ssh wanderindev@159.223.174.185 'pgrep -af orchestrator.main; tail -n 30 ~/logs/orchestrator-main.log'
```

Copying the merged code to the droplet and restarting the running loop (it
keeps old code in memory until killed; keepalive restarts it) are state changes:
tell the operator which files changed and that the loop needs a restart.

**Migrations** — numbered SQL in `migrations/`, applied by
`python -m orchestrator.migrate` (idempotent; "nothing to apply" on a re-run).
The DB is reachable only from the droplet (VPC + trusted-sources firewall), so
once the operator has copied the merged files you may apply the migrations the
just-merged PR added, unattended:

```bash
ssh wanderindev@159.223.174.185 'cd /srv/orchestrator && set -a && . /srv/orchestrator.env && set +a && .venv/bin/python -m orchestrator.migrate'
```

Never hand-write rows into `agent_events` — it is append-only by trigger.

**Ask the operator first** — copying code to or restarting anything on the
droplet, editing `/srv/orchestrator.env` or the crontab, rebuilding the sandbox
image, re-running `provision-droplet.sh`, `claude auth login`, any GitHub App or
Sentry token change, and any migration that is not from the PR just merged.
