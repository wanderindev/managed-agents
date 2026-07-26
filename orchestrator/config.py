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

# --- sandbox -----------------------------------------------------------------

SANDBOX_IMAGE = os.environ.get(
    "ORCHESTRATOR_SANDBOX_IMAGE", "managed-agents/sandbox:latest"
)

#: Host clones live here; each job gets a git worktree off one of them.
REPOS_ROOT = os.environ.get("ORCHESTRATOR_REPOS_ROOT", "/srv/repos")
WORKTREES_ROOT = os.environ.get("ORCHESTRATOR_WORKTREES_ROOT", "/srv/worktrees")

#: Per-job scratch, mounted at /work. The prompt goes here rather than into the
#: repo, so a job cannot accidentally commit its own instructions.
JOBS_ROOT = os.environ.get("ORCHESTRATOR_JOBS_ROOT", "/srv/jobs")

CLAUDE_CREDENTIALS = os.environ.get(
    "ORCHESTRATOR_CLAUDE_CREDENTIALS", "/home/wanderindev/.claude/.credentials.json"
)

#: Wall clock, enforced inside the container. Never an event count: a healthy
#: long research run emits few events, and a stuck one can emit thousands.
SANDBOX_TIMEOUT_SECONDS = int(
    os.environ.get("ORCHESTRATOR_SANDBOX_TIMEOUT_SECONDS", "1800")
)

SANDBOX_MEMORY = os.environ.get("ORCHESTRATOR_SANDBOX_MEMORY", "3g")
SANDBOX_MEMORY_SWAP = os.environ.get("ORCHESTRATOR_SANDBOX_MEMORY_SWAP", "4g")

#: Hands the job the host's Docker daemon so it can run testcontainers suites.
#: Off by default: with it on the container stops being a security boundary.
SANDBOX_WITH_DOCKER = os.environ.get("ORCHESTRATOR_SANDBOX_WITH_DOCKER", "0") == "1"

#: Ceiling on one event's payload. One runaway tool result must not be able to
#: bloat the log, and nothing downstream reads a 2MB payload usefully anyway.
MAX_PAYLOAD_BYTES = int(os.environ.get("ORCHESTRATOR_MAX_PAYLOAD_BYTES", "16384"))

# --- sentry work source (#6) -------------------------------------------------

#: A Sentry auth token with event:read and org:read. Created by hand in the
#: Sentry UI; there is no API for minting one.
SENTRY_TOKEN = os.environ.get("ORCHESTRATOR_SENTRY_TOKEN", "")
SENTRY_ORG = os.environ.get("ORCHESTRATOR_SENTRY_ORG", "javier-feliu")

#: Region URL, not sentry.io. The org lives in the US region and the API rejects
#: a request sent to the wrong one.
SENTRY_BASE_URL = os.environ.get("ORCHESTRATOR_SENTRY_BASE_URL", "https://us.sentry.io")

#: 1, not 2: these are low-traffic personal sites where a single occurrence
#: is already signal. See Filters.min_events.
SENTRY_MIN_EVENTS = int(os.environ.get("ORCHESTRATOR_SENTRY_MIN_EVENTS", "1"))
SENTRY_MAX_PER_POLL = int(os.environ.get("ORCHESTRATOR_SENTRY_MAX_PER_POLL", "5"))
SENTRY_COOLDOWN_DAYS = int(os.environ.get("ORCHESTRATOR_SENTRY_COOLDOWN_DAYS", "7"))

#: Lookback for the issue query. An issue whose last event is older than this is
#: invisible to the poll, however real it is, so this is a scope decision rather
#: than a tuning knob.
SENTRY_STATS_PERIOD = os.environ.get("ORCHESTRATOR_SENTRY_STATS_PERIOD", "14d")

# --- github app (#11) --------------------------------------------------------

#: Numeric App ID from the App's settings page.
GITHUB_APP_ID = os.environ.get("ORCHESTRATOR_GITHUB_APP_ID", "")

#: PEM private key, on disk rather than in the environment: it is multi-line, and
#: a file can be chmod 600 while an env var is visible to anything that can read
#: /proc for the process.
GITHUB_APP_PRIVATE_KEY_PATH = os.environ.get(
    "ORCHESTRATOR_GITHUB_APP_PRIVATE_KEY_PATH", "/srv/secrets/github-app.pem"
)

#: Which installation to mint tokens for. Discover it with
#: `python -m orchestrator.github`.
GITHUB_APP_INSTALLATION_ID = int(
    os.environ.get("ORCHESTRATOR_GITHUB_APP_INSTALLATION_ID", "0")
)

#: What the App is supposed to be able to reach. Used only to warn when setup
#: granted more than intended; nothing enforces it here, GitHub does.
GITHUB_EXPECTED_REPOS = (
    "wanderindev/feliu-dev",
    "wanderindev/panama-in-context",
)
