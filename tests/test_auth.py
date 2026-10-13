"""orchestrator.auth: recognising a dead login and versioning the credential."""

from orchestrator import auth


def test_the_stream_error_marks_a_dead_login():
    assert auth.failed_auth([{"type": "assistant", "error": "authentication_failed"}])


def test_the_result_text_alone_is_enough():
    assert auth.failed_auth(
        [{"type": "result", "is_error": True, "result": "Failed to authenticate: x"}]
    )


def test_other_errors_and_clean_runs_are_not_auth():
    assert not auth.failed_auth([])
    assert not auth.failed_auth(
        [{"type": "result", "is_error": True, "result": "boom"}]
    )
    assert not auth.failed_auth(
        [{"type": "result", "is_error": False, "result": "Failed to authenticate"}]
    )


def test_fingerprint_tracks_rewrites_and_absence(tmp_path):
    path = tmp_path / "c.json"
    assert auth.fingerprint(path) == "missing"
    path.write_text("a")
    first = auth.fingerprint(path)
    path.write_text("bb")
    assert auth.fingerprint(path) != first
    # Never the content: the token is a secret.
    assert "bb" not in auth.fingerprint(path)
