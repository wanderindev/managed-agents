"""The poll entry point."""

from orchestrator import poll as poll_module


def test_it_refuses_clearly_without_a_token(monkeypatch, caplog):
    monkeypatch.setattr(poll_module.config, "SENTRY_TOKEN", "")
    with caplog.at_level("ERROR", logger="orchestrator.poll"):
        assert poll_module.main([]) == 2
    assert "ORCHESTRATOR_SENTRY_TOKEN" in caplog.text
    assert "event:read" in caplog.text, "say what scopes the token needs"


def test_it_polls_and_exits_zero_when_nothing_is_found(monkeypatch, migrated_dsn):
    """A quiet poll is a normal outcome, not a failure for the timer to alert on."""
    monkeypatch.setattr(poll_module.config, "SENTRY_TOKEN", "tok")
    monkeypatch.setattr(poll_module.config, "DATABASE_URL", migrated_dsn)
    seen = {}

    def fake_poll(conn, client, *, filters=None, dry_run=False):
        seen["client"] = client
        seen["filters"] = filters
        return poll_module.sentry.PollReport()

    monkeypatch.setattr(poll_module.sentry, "poll", fake_poll)
    assert poll_module.main([]) == 0
    assert seen["client"].org == "javier-feliu"
    assert seen["filters"].min_events >= 1


def test_dry_run_is_passed_through(monkeypatch, migrated_dsn):
    monkeypatch.setattr(poll_module.config, "SENTRY_TOKEN", "tok")
    monkeypatch.setattr(poll_module.config, "DATABASE_URL", migrated_dsn)
    seen = {}

    def fake_poll(conn, client, *, filters=None, dry_run=False):
        seen["dry_run"] = dry_run
        return poll_module.sentry.PollReport()

    monkeypatch.setattr(poll_module.sentry, "poll", fake_poll)
    poll_module.main(["--dry-run"])
    assert seen["dry_run"] is True
