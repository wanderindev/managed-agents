"""Sentry triage for the atelier projects (#68).

The storefront theme's Sentry project is named after a repo it does not live
in, the theme repo is on `master`, and a jQuery storefront throws a different
class of noise than the React apps. These pin all three.
"""

import pytest

from orchestrator import config, jobs
from orchestrator import poll as poll_module
from orchestrator.enums import Outcome
from orchestrator.log import create_run, load_events
from orchestrator.queue import get_run
from orchestrator.sandbox import JobSpec
from orchestrator.sources import github_prs, sentry
from orchestrator.sources.sentry import Filters, classify, poll
from tests.fakes import FakeCommands
from tests.test_adversary import CHAIN_PAYLOAD, fake_run
from tests.test_sandbox import make_run, make_runner
from tests.test_sentry_source import FakeSentry, issue
from tests.test_triage import PAYLOAD, FakeDetailClient

THEME_PAYLOAD = {
    **PAYLOAD,
    "short_id": "ATELIER-THEME-1F",
    "project": "atelier-theme",
    "repo": "atelier-new-cli",
    "base_branch": "master",
}

LOYALTY_PAYLOAD = {
    **PAYLOAD,
    "short_id": "ATELIER-LOYALTY-APP-1",
    "project": "atelier-loyalty-app",
    "repo": "atelier-loyalty-app",
    "base_branch": "main",
}


@pytest.fixture()
def detail(monkeypatch):
    client = FakeDetailClient()
    monkeypatch.setattr(jobs, "_sentry_client", lambda: client)
    return client


def theme_issue(title, culprit=""):
    return sentry._to_issue(
        issue(
            shortId="ATELIER-THEME-1F",
            title=title,
            culprit=culprit,
            project={"slug": "atelier-theme"},
        )
    )


def triage(conn, payload, subject="sentry:AT-1"):
    return get_run(conn, create_run(conn, sentry.RUN_KIND, subject, payload))


# --- the mapping ---------------------------------------------------------------


def test_the_storefront_theme_lands_in_the_repo_its_code_lives_in():
    """The `atelier-theme` *repo* is the unreleased replacement theme."""
    assert sentry.PROJECT_REPOS["atelier-theme"] == "atelier-new-cli"
    assert sentry.PROJECT_REPOS["atelier-loyalty-app"] == "atelier-loyalty-app"
    assert "pic-cert-watcher" not in sentry.PROJECT_REPOS


def test_every_triage_repo_is_polled_for_change_requests():
    """The PR poll walks GITHUB_EXPECTED_REPOS: a triage PR outside it would
    never have its review comments answered."""
    for repo in set(sentry.PROJECT_REPOS.values()):
        assert f"wanderindev/{repo}" in config.GITHUB_EXPECTED_REPOS


# --- the filters ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("title", "culprit", "expected"),
    [
        # Every one of these is a live atelier-theme issue on 2026-10-05.
        (
            (
                "TypeError: undefined is not an object"
                " (evaluating 't.settings.style.customized.faq_yn')"
            ),
            "<anonymous>(s/files/1/0033/3538/9233/files/pushdaddy_v56_test)",
            "PushDaddy",
        ),
        (
            "TypeError: B.charCodeAt is not a function.",
            "?(recaptcha/releases/BnqMGSY_YP4cCmbNINHpJPkd/recaptcha__en)",
            "reCAPTCHA",
        ),
        (
            "TypeError: Failed to fetch (otlp-http-production.shopifysvc.com)",
            "<anonymous>(core/build/esm/instrument/fetch)",
            "Shopify's own telemetry",
        ),
        (
            (
                "Error: Uncaught NetworkError: Failed to execute 'importScripts' on"
                " 'WorkerGlobalScope': The script at 'https://atelierm.store/web-pixels/"
                "strict/app/web-pixel-2279211229@5a8c.js' failed to load."
            ),
            "",
            "web-pixel",
        ),
        ("Error: NetworkError: Load failed", "", "browser-side network failure"),
        (
            "TypeError: Importing a module script failed.",
            "https://atelierm.store/",
            "module that failed to load",
        ),
        (
            (
                "UnhandledRejection: Non-Error promise rejection captured with value:"
                " Object Not Found Matching Id:2, MethodName:update, ParamCount:4"
            ),
            "https://atelierm.store/collections/cases",
            "link-scanner",
        ),
        (
            "TypeError: Cannot read properties of null (reading 'offsetTop')",
            "HTMLDocument.<anonymous>(services/assets/150804955357/editor_asset/theme)",
            "theme editor",
        ),
        (
            "TypeError: $ is not a function",
            "HTMLLIElement.<anonymous>(cdn/shop/t/75/assets/theme)",
            "jQuery missing",
        ),
        (
            "ReferenceError: jQuery is not defined",
            "<unknown>(cdn/shop/t/75/assets/theme)",
            "jQuery missing",
        ),
        (
            "Theme failed to boot (jQuery/theme missing after load)",
            "https://atelierm.store/products/charming-round-keychain",
            "superseded boot check",
        ),
        (
            "RangeError: Maximum call stack size exceeded.",
            "a(products/mens-toiletry)",
            "clobbered jQuery",
        ),
        ("Error: [object XMLHttpRequest]", "", "raw XHR"),
        ("ReferenceError: require is not defined", "?(options)", "CommonJS"),
    ],
)
def test_storefront_noise_is_dropped_with_a_reason(title, culprit, expected):
    reason = classify(theme_issue(title, culprit), Filters())
    assert reason is not None
    assert expected in reason


@pytest.mark.parametrize(
    ("title", "culprit"),
    [
        # The three live theme issues that point at the theme's own code.
        (
            "TypeError: undefined is not an object (evaluating 'evt.keyCode')",
            "Drawer.prototype.close(cdn/shop/t/75/assets/theme)",
        ),
        (
            "ReferenceError: Modernizr is not defined",
            "Collection(cdn/shop/t/75/assets/theme)",
        ),
        (
            (
                "TypeError: null is not an object"
                " (evaluating 'document.body.scrollHeight')"
            ),
            "global code(en/products/large-cosmetic-bag-1)",
        ),
    ],
)
def test_a_real_theme_bug_is_kept(title, culprit):
    assert classify(theme_issue(title, culprit), Filters()) is None


def test_a_theme_only_judgement_does_not_filter_other_projects():
    """`Maximum call stack size exceeded` is jQuery noise on the storefront and
    a genuine recursion bug anywhere else."""
    other = sentry._to_issue(
        issue(
            title="RangeError: Maximum call stack size exceeded.",
            culprit="renderTree(src/components/Tree.tsx)",
            project={"slug": "trd-javascript-react"},
        )
    )
    assert classify(other, Filters()) is None


# --- the base branch -------------------------------------------------------------


def _payload_of(conn, subject):
    run_id = conn.execute(
        "SELECT id FROM agent_runs WHERE subject = %s", (subject,)
    ).fetchone()["id"]
    return load_events(conn, run_id)[0].payload


def test_the_payload_carries_the_resolved_base_branch(conn):
    client = FakeSentry(
        {"atelier-theme": [issue(project={"slug": "atelier-theme"}, shortId="AT-1")]}
    )
    asked = []

    def base_branch(repo):
        asked.append(repo)
        return "master"

    poll(conn, client, projects=["atelier-theme"], base_branch=base_branch)

    payload = _payload_of(conn, "sentry:AT-1")
    assert payload["repo"] == "atelier-new-cli"
    assert payload["base_branch"] == "master"
    assert asked == ["atelier-new-cli"]


def test_without_a_resolver_the_base_is_main(conn):
    poll(conn, FakeSentry({"pic-python-fastapi": [issue()]}))
    payload = _payload_of(conn, "sentry:PIC-PYTHON-FASTAPI-1Q")
    assert payload["base_branch"] == "main"


class FakeRepoPulls:
    def __init__(self, branches):
        self.branches = branches
        self.asked = []

    def repository(self, full_repo):
        self.asked.append(full_repo)
        return {"default_branch": self.branches[full_repo]}


def test_the_poll_reads_each_default_branch_from_github_once(monkeypatch):
    pulls = FakeRepoPulls({"wanderindev/atelier-new-cli": "master"})
    resolve = poll_module._base_branch_resolver(pulls)

    assert resolve("atelier-new-cli") == "master"
    assert resolve("atelier-new-cli") == "master"
    assert pulls.asked == ["wanderindev/atelier-new-cli"], "one read per poll"


def test_without_the_app_no_base_resolves():
    assert poll_module._base_branch_resolver(None)("atelier-new-cli") is None


class FlakyRepoPulls:
    def repository(self, full_repo):
        raise OSError("connection reset")


def test_a_failed_lookup_resolves_to_nothing_not_main(caplog):
    with caplog.at_level("WARNING", logger="orchestrator.poll"):
        resolve = poll_module._base_branch_resolver(FlakyRepoPulls())
        assert resolve("atelier-new-cli") is None
    assert "default branch" in caplog.text


def test_an_unresolved_base_defers_that_repos_issues_without_a_cooldown(conn):
    """A guessed main would fail the sandbox fetch and cost a week (#68)."""
    client = FakeSentry(
        {
            "atelier-theme": [issue(project={"slug": "atelier-theme"}, shortId="AT-9")],
            "pic-python-fastapi": [issue()],
        }
    )

    def base_branch(repo):
        return None if repo == "atelier-new-cli" else "main"

    report = poll(
        conn,
        client,
        projects=["atelier-theme", "pic-python-fastapi"],
        base_branch=base_branch,
    )

    assert report.enqueued == ["sentry:PIC-PYTHON-FASTAPI-1Q"]
    assert report.dropped == {"base branch unresolved; retried next poll": 1}
    assert not sentry.queue.has_recent_run(
        conn, sentry.RUN_KIND, "sentry:AT-9", sentry.timedelta(days=7)
    ), "no run, so no cooldown"

    # The next poll, with GitHub answering, picks it up.
    report = poll(
        conn, client, projects=["atelier-theme"], base_branch=lambda r: "master"
    )
    assert report.enqueued == ["sentry:AT-9"]
    assert _payload_of(conn, "sentry:AT-9")["base_branch"] == "master"


def test_the_poll_wires_the_resolver_and_the_pr_poll(monkeypatch, migrated_dsn):
    monkeypatch.setattr(poll_module.config, "SENTRY_TOKEN", "tok")
    monkeypatch.setattr(poll_module.config, "DATABASE_URL", migrated_dsn)
    pulls = FakeRepoPulls({"wanderindev/atelier-new-cli": "master"})
    monkeypatch.setattr(poll_module, "_pulls_client", lambda: pulls)
    seen = {}

    def fake_poll(conn, client, *, base_branch=None, **kwargs):
        seen["base"] = base_branch("atelier-new-cli")
        return sentry.PollReport()

    def fake_pr_poll(conn, client, *, dry_run=False):
        seen["pr_client"] = client

    monkeypatch.setattr(poll_module.sentry, "poll", fake_poll)
    monkeypatch.setattr(poll_module.github_prs, "poll", fake_pr_poll)

    assert poll_module.main([]) == 0
    assert seen == {"base": "master", "pr_client": pulls}


@pytest.mark.parametrize(
    "error",
    [
        poll_module.github.GitHubAppError("no key"),
        poll_module.urllib.error.URLError("dns"),
        TimeoutError("timed out"),
    ],
)
def test_an_unusable_app_skips_the_pr_poll_out_loud(monkeypatch, caplog, error):
    class App:
        def installation_token(self):
            raise error

    monkeypatch.setattr(poll_module.github, "from_config", lambda: App())
    with caplog.at_level("WARNING", logger="orchestrator.poll"):
        assert poll_module._pulls_client() is None
    assert "skipping the PR poll" in caplog.text
    assert "deferring Sentry issues" in caplog.text


def test_a_network_failure_minting_the_token_still_polls_sentry(
    monkeypatch, migrated_dsn
):
    """installation_token only wraps HTTP errors; an unwrapped one must not
    cost the hour's Sentry poll."""
    monkeypatch.setattr(poll_module.config, "SENTRY_TOKEN", "tok")
    monkeypatch.setattr(poll_module.config, "DATABASE_URL", migrated_dsn)

    class App:
        def installation_token(self):
            raise poll_module.urllib.error.URLError("connection refused")

    monkeypatch.setattr(poll_module.github, "from_config", lambda: App())
    seen = {}

    def fake_poll(conn, client, *, base_branch=None, **kwargs):
        seen["base"] = base_branch("atelier-new-cli")
        return sentry.PollReport()

    def no_pr_poll(*args, **kwargs):
        raise AssertionError("no App, no PR poll")

    monkeypatch.setattr(poll_module.sentry, "poll", fake_poll)
    monkeypatch.setattr(poll_module.github_prs, "poll", no_pr_poll)

    assert poll_module.main([]) == 0
    assert seen == {"base": None}, "polled, with every base deferred"


def test_the_pulls_client_carries_the_installation_token(monkeypatch):
    class App:
        def installation_token(self):
            return "inst-tok"

    monkeypatch.setattr(poll_module.github, "from_config", lambda: App())
    assert poll_module._pulls_client().token == "inst-tok"


# --- the triage prompt -----------------------------------------------------------


def test_the_theme_fix_targets_master(conn, detail):
    spec = jobs.build_spec(triage(conn, THEME_PAYLOAD))
    assert spec.repo == "atelier-new-cli"
    assert spec.base_branch == "master"
    assert "gh pr create --draft --base master" in spec.prompt
    assert "Never push to master." in spec.prompt
    assert "--base main" not in spec.prompt


def test_a_payload_from_before_68_still_targets_main(conn, detail):
    legacy = {k: v for k, v in PAYLOAD.items() if k != "base_branch"}
    spec = jobs.build_spec(triage(conn, legacy))
    assert spec.base_branch == "main"
    assert "gh pr create --draft --base main" in spec.prompt


def test_the_theme_says_what_verified_means_without_a_test_suite(conn, detail):
    prompt = jobs.build_spec(triage(conn, THEME_PAYLOAD)).prompt
    assert "no test suite" in prompt
    assert "git show master:<path>" in prompt
    assert "Add or extend a test that FAILS" not in prompt
    # The gate: CI's pinned theme-check diff, run through npx.
    assert "npx -y @shopify/cli@3.94.3 theme check" in prompt
    assert "worktree add /work/base master" in prompt
    assert "theme_check_diff.py" in prompt
    assert "shopify theme push" in prompt and "LIVE storefront" in prompt


def test_the_loyalty_app_keeps_the_test_rule_and_gets_a_reachable_postgres(
    conn, detail
):
    prompt = jobs.build_spec(triage(conn, LOYALTY_PAYLOAD)).prompt
    assert "Add or extend a test that FAILS before your fix" in prompt
    assert "npm run typecheck" in prompt and "npm run build" in prompt
    assert "postgres:17-alpine" in prompt
    assert "{{range .NetworkSettings.Networks}}" in prompt, "Go template intact"
    assert "Do NOT\nuse the repo's `docker compose up -d db`" in prompt
    assert "gitleaks is not installed" in prompt
    # The Postgres cannot outlive the sandbox (the runner reaps by label).
    assert '--rm --label "$AGENT_SIBLING_LABEL"' in prompt


# --- the chain -------------------------------------------------------------------


def test_the_base_rides_the_chain_into_review():
    run = fake_run(sentry.RUN_KIND, THEME_PAYLOAD)
    decision = jobs.followups(
        run, {"outcome": "FIX", "pr_url": "https://x/pr/1", "branch": "agent/run-3"}
    )
    (review,) = decision.enqueue
    assert review.payload["base_branch"] == "master"


def test_the_review_diffs_against_the_base_and_reruns_the_reproduction(detail):
    payload = {**CHAIN_PAYLOAD, **THEME_PAYLOAD}
    spec = jobs.build_spec(fake_run(jobs.REVIEW_KIND, payload))
    assert spec.base_branch == "master"
    assert "git diff master...HEAD" in spec.prompt
    assert "reproduction script" in spec.prompt
    assert f"gh pr view {CHAIN_PAYLOAD['pr_url']}" in spec.prompt
    assert "git checkout master -- <impl files>" in spec.prompt


def test_a_review_elsewhere_keeps_the_test_attack(detail):
    spec = jobs.build_spec(fake_run(jobs.REVIEW_KIND, CHAIN_PAYLOAD))
    assert spec.base_branch == "main"
    assert "git diff main...HEAD" in spec.prompt
    assert "Is the new test asserting the bug is fixed" in spec.prompt
    assert "Mirror the repo's CI" in spec.prompt, "the repo's gate, not a generic"


def test_the_loyalty_review_can_reach_postgres_and_cannot_deploy(detail):
    """Attack line 2 re-runs the test on unpatched code, which needs the
    sandbox-safe Postgres recipe, not the repo's compose one."""
    payload = {**CHAIN_PAYLOAD, **LOYALTY_PAYLOAD}
    prompt = jobs.build_spec(fake_run(jobs.REVIEW_KIND, payload)).prompt
    assert "# Running the repository's checks" in prompt
    assert '--rm --label "$AGENT_SIBLING_LABEL"' in prompt
    assert "{{range .NetworkSettings.Networks}}" in prompt
    assert "./scripts/deploy.sh" in prompt and "shopify app deploy" in prompt


def test_the_theme_review_runs_the_check_and_cannot_publish(detail):
    payload = {**CHAIN_PAYLOAD, **THEME_PAYLOAD}
    prompt = jobs.build_spec(fake_run(jobs.REVIEW_KIND, payload)).prompt
    assert "theme_check_diff.py" in prompt
    assert "worktree add /work/base master" in prompt
    assert "`shopify theme push`, `pull`, `dev` or `publish`" in prompt


def test_a_revision_keeps_the_base(detail):
    payload = {**CHAIN_PAYLOAD, **THEME_PAYLOAD, "refutation": "wrong", "round": 2}
    spec = jobs.build_spec(fake_run(jobs.REVISION_KIND, payload))
    assert spec.base_branch == "master"
    assert "Never push to master." in spec.prompt
    assert "theme_check_diff.py" in spec.prompt


def test_a_pr_revision_targets_the_prs_own_base(conn):
    origin = create_run(conn, sentry.RUN_KIND, "sentry:AT-origin", THEME_PAYLOAD)

    class Pulls:
        def open_pulls(self, full_repo):
            return [
                {
                    "number": 9,
                    "user": {"login": "bot"},
                    "head": {"ref": f"agent/run-{origin}"},
                    "base": {"ref": "master"},
                    "html_url": "https://github.com/wanderindev/atelier-new-cli/pull/9",
                }
            ]

        def reviews(self, full_repo, number):
            return [
                {
                    "state": "CHANGES_REQUESTED",
                    "user": {"login": "human"},
                    "submitted_at": "2026-10-05T10:00:00Z",
                    "body": "tighten this",
                }
            ]

        def review_comments(self, full_repo, number):
            return []

        def commits(self, full_repo, number):
            return []

    report = github_prs.poll(
        conn,
        Pulls(),
        repos=("wanderindev/atelier-new-cli",),
        bot_login="bot",
        human_logins=("human",),
        max_per_poll=3,
    )
    assert report.enqueued == ["pr:atelier-new-cli#9"]
    run = conn.execute(
        "SELECT id FROM agent_runs WHERE kind = %s", (github_prs.RUN_KIND,)
    ).fetchone()["id"]
    run = get_run(conn, run)
    assert run.payload["base_branch"] == "master"

    spec = jobs.build_spec(run)
    assert spec.base_branch == "master"
    assert "Never push to master." in spec.prompt


# --- containers a sandbox starts --------------------------------------------------


@pytest.fixture()
def roots(tmp_path):
    """Same shape as test_sandbox's fixture of the same name."""
    creds = tmp_path / "credentials.json"
    creds.write_text("{}")
    return {
        "repos_root": tmp_path / "repos",
        "worktrees_root": tmp_path / "worktrees",
        "jobs_root": tmp_path / "jobs",
        "credentials": creds,
    }


def _finish(roots, commands, run_id=4, attempts=2):
    return make_runner(roots, commands).finish(
        make_run(run_id, attempts=attempts), f"ma-run-{run_id}-{attempts}"
    )


def test_a_docker_sandbox_is_told_its_sibling_label(roots):
    commands = FakeCommands()
    runner = make_runner(roots, commands, spec=JobSpec(prompt="x", needs_docker=True))
    runner.start(make_run(4, attempts=2))
    argv = commands.commands("run")[0]
    assert "AGENT_SIBLING_LABEL=managed-agents.run=ma-run-4-2" in argv


def test_finish_reaps_only_this_attempts_siblings(roots):
    """A timed-out agent never got to `docker rm` its Postgres."""
    commands = FakeCommands(
        {
            ("inspect",): (0, "124\n", ""),
            ("ps",): (0, "abc123\ndef456\n", ""),
        }
    )
    _finish(roots, commands)

    (listing,) = commands.commands("ps")
    assert listing[-2:] == ["--filter", "label=managed-agents.run=ma-run-4-2"]
    assert ["docker", "rm", "-f", "abc123", "def456"] in commands.calls


def test_finish_reaps_siblings_of_a_vanished_sandbox(roots):
    commands = FakeCommands(
        {("inspect",): (1, "", "No such object"), ("ps",): (0, "abc123\n", "")}
    )
    assert _finish(roots, commands).outcome is Outcome.GONE
    assert ["docker", "rm", "-f", "abc123"] in commands.calls


@pytest.mark.parametrize("listing", [(0, "", ""), (1, "abc123\n", "daemon down")])
def test_no_siblings_or_a_failed_listing_removes_nothing_else(roots, listing):
    commands = FakeCommands({("inspect",): (0, "0\n", ""), ("ps",): listing})
    _finish(roots, commands)
    assert commands.commands("rm") == [["docker", "rm", "-f", "ma-run-4-2"]]
