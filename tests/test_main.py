"""The entry point. Small, but it is the thing that actually runs in production,
so it should not be the one module nothing ever executes.
"""

import signal

from orchestrator import main as main_module


def test_main_wires_a_docker_runner_into_the_loop(monkeypatch):
    built = {}

    class FakeOrchestrator:
        worker_id = "agents-1"
        max_concurrent = 1
        tick_seconds = 15

        def __init__(self, runner, **kwargs):
            built["runner"] = runner
            built["kwargs"] = kwargs

        def run_forever(self, **kwargs):
            built["should_stop"] = kwargs["should_stop"]

    monkeypatch.setattr(main_module, "Orchestrator", FakeOrchestrator)
    assert main_module.main() == 0

    from orchestrator.sandbox import DockerRunner

    assert isinstance(built["runner"], DockerRunner)
    # The registry is the seam #6 and #7 extend; main must not hardcode a kind.
    assert built["runner"].job_spec is main_module.jobs.build_spec


def test_a_signal_asks_for_a_clean_stop_rather_than_killing_a_tick(monkeypatch):
    """Killing the process outright is safe too; this just makes it tidy."""
    monkeypatch.setattr(main_module, "_stopping", False)
    stops = []

    class FakeOrchestrator:
        worker_id = max_concurrent = tick_seconds = "x"

        def __init__(self, runner, **kwargs):
            pass

        def run_forever(self, **kwargs):
            should_stop = kwargs["should_stop"]
            stops.append(should_stop())
            main_module._request_stop(signal.SIGTERM, None)
            stops.append(should_stop())

    monkeypatch.setattr(main_module, "Orchestrator", FakeOrchestrator)
    main_module.main()

    assert stops == [False, True]


def test_an_unconfigured_github_app_warns_rather_than_stopping_the_loop(
    monkeypatch, caplog
):
    """Kinds that never touch GitHub must still run."""
    from orchestrator import github

    def boom():
        raise github.GitHubAppError("ORCHESTRATOR_GITHUB_APP_ID is not set")

    monkeypatch.setattr(main_module.github, "from_config", boom)
    with caplog.at_level("WARNING", logger="orchestrator.main"):
        assert main_module._github_token() is None
    assert "github jobs will fail" in caplog.text
