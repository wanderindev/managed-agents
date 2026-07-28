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

#: Base delay before a requeued run may be claimed again, doubling per attempt.
#: Without it, three 15-second ticks exhaust every attempt in ~45 seconds, which
#: turns transient problems (a daemon mid-restart, a Postgres failover) into
#: permanent give-ups. See ``loop.backoff_delay``.
BACKOFF_BASE_SECONDS = int(os.environ.get("ORCHESTRATOR_BACKOFF_BASE_SECONDS", "60"))

#: Ceiling on the computed backoff. A known-better wait (a rate limit's reset
#: time) is passed explicitly and is not subject to this.
BACKOFF_CEILING_SECONDS = int(
    os.environ.get("ORCHESTRATOR_BACKOFF_CEILING_SECONDS", "3600")
)

# --- sandbox -----------------------------------------------------------------

SANDBOX_IMAGE = os.environ.get(
    "ORCHESTRATOR_SANDBOX_IMAGE", "managed-agents/sandbox:latest"
)

#: Host clones live here; each job gets a standalone local clone off one of
#: them (`git clone --local`, so the objects are hardlinked and cheap). Not a
#: worktree: a worktree's .git links into the parent's .git on the *host*, a
#: path never mounted into the sandbox, which left git dead in /workspace (#33).
REPOS_ROOT = os.environ.get("ORCHESTRATOR_REPOS_ROOT", "/srv/repos")
#: Named for the worktree era (#33 replaced worktrees with clones); the env var
#: and the /srv/worktrees directory keep their names so nothing redeploys.
WORKTREES_ROOT = os.environ.get("ORCHESTRATOR_WORKTREES_ROOT", "/srv/worktrees")

#: Where each job clone's `origin` points, so commits push straight to GitHub
#: rather than at the parent clone on the host.
GITHUB_REMOTE_BASE = os.environ.get(
    "ORCHESTRATOR_GITHUB_REMOTE_BASE", "https://github.com/wanderindev"
)

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

# --- outcome email (#9) ------------------------------------------------------

#: Where outcome emails go. Empty disables the notifier entirely (with one
#: warning), which is the right dev default: a test loop must not email anyone.
NOTIFY_TO = os.environ.get("ORCHESTRATOR_NOTIFY_TO", "")

#: The From address. Must be a sender the SMTP relay accepts for its domain.
NOTIFY_FROM = os.environ.get("ORCHESTRATOR_NOTIFY_FROM", "")

#: Same transport PIC's email_sender.py uses: Google Workspace SMTP relay,
#: STARTTLS, no auth — the relay trusts allowlisted droplet IPs. Reused per #9
#: rather than adding a provider; the agents droplet's IP must be in the
#: Workspace admin allowlist or the relay refuses.
NOTIFY_SMTP_HOST = os.environ.get(
    "ORCHESTRATOR_NOTIFY_SMTP_HOST", "smtp-relay.gmail.com"
)
NOTIFY_SMTP_PORT = int(os.environ.get("ORCHESTRATOR_NOTIFY_SMTP_PORT", "587"))
NOTIFY_SMTP_TIMEOUT = int(os.environ.get("ORCHESTRATOR_NOTIFY_SMTP_TIMEOUT", "10"))

#: Individual emails per day before the notifier switches to a single digest.
#: Digest, not firehose — and the digest means the cap never suppresses
#: silently.
NOTIFY_DAILY_CAP = int(os.environ.get("ORCHESTRATOR_NOTIFY_DAILY_CAP", "10"))

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

# --- github change-request source (#10) --------------------------------------

#: The login GitHub shows for PRs the App opens. How #10 tells this
#: orchestrator's pull requests apart from a human's.
GITHUB_BOT_LOGIN = os.environ.get(
    "ORCHESTRATOR_GITHUB_BOT_LOGIN", "wanderindev-managed-agents[bot]"
)

#: Humans whose commits on an agent branch mean "hands off". Checked against
#: commit author/committer logins, because the *positive* identity of sandbox
#: commits is unreliable (PR #431's commit resolves to login `claude`).
GITHUB_HUMAN_LOGINS = tuple(
    login.strip()
    for login in os.environ.get(
        "ORCHESTRATOR_GITHUB_HUMAN_LOGINS", "wanderindev"
    ).split(",")
    if login.strip()
)

#: Revision runs one poll may enqueue. A review spree must not spawn a fleet.
GITHUB_PR_MAX_PER_POLL = int(os.environ.get("ORCHESTRATOR_GITHUB_PR_MAX_PER_POLL", "3"))
