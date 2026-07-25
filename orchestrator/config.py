"""Configuration, read from the environment.

No settings framework. There are four values.
"""

import os

#: DSN for the log database. Its own database on the managed Postgres instance,
#: never inside feliudev or PIC's database.
DATABASE_URL = os.environ.get(
    "ORCHESTRATOR_DATABASE_URL",
    "postgresql://orchestrator:localdev@localhost:5433/orchestrator",
)

#: The managed instance is 2GB / 50 connections, and feliu + PIC pools were
#: already trimmed to 2/3 to fit. Stay small so this does not eat the headroom.
DB_POOL_MAX = int(os.environ.get("ORCHESTRATOR_DB_POOL_MAX", "2"))

#: How many runs may be in flight. Deliberately 1 to start: the point is the
#: loop, not throughput.
MAX_CONCURRENT_RUNS = int(os.environ.get("ORCHESTRATOR_MAX_CONCURRENT_RUNS", "1"))

#: Identifies this orchestrator process in ``agent_runs.worker_id``, so a
#: reconcile pass can tell its own abandoned leases from someone else's.
WORKER_ID = os.environ.get("ORCHESTRATOR_WORKER_ID", "orchestrator-1")

#: Seconds a lease is valid for. The heartbeat extends it every tick while the
#: sandbox is alive, so this only has to outlast one tick plus slack. Its real
#: job is bounding how long a run stays stuck after the *orchestrator* dies,
#: because then nobody is heartbeating and nothing can ask the container.
LEASE_SECONDS = int(os.environ.get("ORCHESTRATOR_LEASE_SECONDS", "300"))

#: How long between ticks.
TICK_SECONDS = int(os.environ.get("ORCHESTRATOR_TICK_SECONDS", "15"))

#: How many times a run may be leased before it is given up on. A run that dies
#: three times is an infrastructure problem, and retrying it a fourth time just
#: burns tokens on the same failure.
MAX_ATTEMPTS = int(os.environ.get("ORCHESTRATOR_MAX_ATTEMPTS", "3"))
