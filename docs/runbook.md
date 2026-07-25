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
