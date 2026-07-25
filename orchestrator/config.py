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
