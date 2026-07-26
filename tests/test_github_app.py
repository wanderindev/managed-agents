"""GitHub App authentication, and how its token reaches a sandbox."""

import json
import time
import urllib.error

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from orchestrator.github import GitHubAppAuth, GitHubAppError

APP_ID = "123456"


@pytest.fixture(scope="module")
def keypair():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    public_pem = (
        key.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return private_pem, public_pem


class FakeGitHub:
    """Records requests and replays canned JSON."""

    def __init__(self, responses=None, error=None):
        self.responses = responses or {}
        self.error = error
        self.calls = []

    def __call__(self, request, timeout=None):
        self.calls.append(request)
        if self.error:
            raise self.error
        path = request.full_url.replace("https://api.github.com", "")
        body = self.responses.get(path, {})
        return _Response(json.dumps(body).encode())


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return self.payload


def make_auth(keypair, github, installation_id=42, now=None):
    private_pem, _ = keypair
    return GitHubAppAuth(
        APP_ID,
        private_pem,
        installation_id,
        opener=github,
        now=now or time.time,
    )


# --- the app JWT -------------------------------------------------------------


def test_the_jwt_is_signed_with_the_app_key_and_verifies(keypair):
    _, public_pem = keypair
    auth = make_auth(keypair, FakeGitHub())

    decoded = jwt.decode(auth.app_jwt(), public_pem, algorithms=["RS256"])

    assert decoded["iss"] == APP_ID


def test_the_jwt_is_backdated_and_short_lived(keypair):
    """GitHub rejects a JWT issued in the future or living beyond 10 minutes."""
    _, public_pem = keypair
    fixed = 1_800_000_000
    auth = make_auth(keypair, FakeGitHub(), now=lambda: fixed)

    # Time claims are the thing under test, so they are asserted rather than
    # validated: a fixed clock is by definition not "now".
    decoded = jwt.decode(
        auth.app_jwt(),
        public_pem,
        algorithms=["RS256"],
        options={"verify_exp": False, "verify_iat": False},
    )

    assert decoded["iat"] < fixed, "backdated to absorb clock skew"
    assert decoded["exp"] - decoded["iat"] <= 600


# --- installation tokens -----------------------------------------------------


def test_minting_returns_the_token(keypair):
    github = FakeGitHub(
        {
            "/app/installations/42/access_tokens": {
                "token": "ghs_secret",
                "expires_at": "2026-07-25T18:00:00Z",
            }
        }
    )
    assert make_auth(keypair, github).installation_token() == "ghs_secret"


def test_minting_posts_with_the_app_jwt(keypair):
    github = FakeGitHub({"/app/installations/42/access_tokens": {"token": "t"}})
    auth = make_auth(keypair, github)
    _, public_pem = keypair

    auth.installation_token()

    request = github.calls[0]
    assert request.method == "POST"
    bearer = request.headers["Authorization"].removeprefix("Bearer ")
    # The app JWT, not a token: proving key possession is the whole point.
    assert jwt.decode(bearer, public_pem, algorithms=["RS256"])["iss"] == APP_ID


def test_a_fresh_token_is_minted_every_time(keypair):
    """Not cached: a cache only risks handing a sandbox a token that expires
    mid-run, and per-job tokens make a leak traceable to one job."""
    github = FakeGitHub({"/app/installations/42/access_tokens": {"token": "t"}})
    auth = make_auth(keypair, github)

    auth.installation_token()
    auth.installation_token()

    assert len(github.calls) == 2


def test_minting_without_an_installation_id_says_how_to_find_one(keypair):
    auth = make_auth(keypair, FakeGitHub(), installation_id=None)
    with pytest.raises(GitHubAppError, match="orchestrator.github"):
        auth.installation_token()


def test_a_missing_token_in_the_response_is_an_error(keypair):
    github = FakeGitHub({"/app/installations/42/access_tokens": {}})
    with pytest.raises(GitHubAppError, match="no token"):
        make_auth(keypair, github).installation_token()


def test_an_http_error_carries_githubs_explanation(keypair):
    error = urllib.error.HTTPError(
        "https://api.github.com/app/installations",
        401,
        "Unauthorized",
        {},
        None,
    )
    error.read = lambda: b'{"message":"A JSON web token could not be decoded"}'
    with pytest.raises(GitHubAppError, match="could not be decoded"):
        make_auth(keypair, FakeGitHub(error=error)).installations()


# --- discovery ---------------------------------------------------------------


def test_installations_are_listed_for_setup(keypair):
    github = FakeGitHub(
        {
            "/app/installations": [
                {
                    "id": 42,
                    "account": {"login": "wanderindev"},
                    "repository_selection": "selected",
                }
            ]
        }
    )
    installations = make_auth(keypair, github).installations()

    assert installations[0].id == 42
    assert installations[0].account == "wanderindev"
    # "selected" rather than "all" is the whole point of #11.
    assert installations[0].repository_selection == "selected"


def test_reachable_repositories_can_be_checked(keypair):
    """An App installed on everything would give each sandbox far too much."""
    github = FakeGitHub(
        {
            "/app/installations/42/access_tokens": {"token": "t"},
            "/installation/repositories": {
                "repositories": [
                    {"full_name": "wanderindev/feliu-dev"},
                    {"full_name": "wanderindev/panama-in-context"},
                ]
            },
        }
    )
    assert make_auth(keypair, github).repositories() == [
        "wanderindev/feliu-dev",
        "wanderindev/panama-in-context",
    ]


# --- configuration -----------------------------------------------------------


def test_from_config_reads_the_key_off_disk(keypair, tmp_path, monkeypatch):
    from orchestrator import config, github

    private_pem, _ = keypair
    key = tmp_path / "app.pem"
    key.write_text(private_pem)
    monkeypatch.setattr(config, "GITHUB_APP_ID", APP_ID)
    monkeypatch.setattr(config, "GITHUB_APP_PRIVATE_KEY_PATH", str(key))
    monkeypatch.setattr(config, "GITHUB_APP_INSTALLATION_ID", 42)

    auth = github.from_config()

    assert auth.app_id == APP_ID
    assert auth.installation_id == 42


def test_from_config_complains_about_a_missing_app_id(monkeypatch):
    from orchestrator import config, github

    monkeypatch.setattr(config, "GITHUB_APP_ID", "")
    with pytest.raises(GitHubAppError, match="GITHUB_APP_ID"):
        github.from_config()


def test_from_config_complains_about_a_missing_key_file(tmp_path, monkeypatch):
    from orchestrator import config, github

    monkeypatch.setattr(config, "GITHUB_APP_ID", APP_ID)
    monkeypatch.setattr(config, "GITHUB_APP_PRIVATE_KEY_PATH", str(tmp_path / "nope"))
    with pytest.raises(GitHubAppError, match="no App private key"):
        github.from_config()


def test_an_unset_installation_id_reads_as_none(keypair, tmp_path, monkeypatch):
    """0 is the "unset" value from the environment, not a real installation."""
    from orchestrator import config, github

    private_pem, _ = keypair
    key = tmp_path / "app.pem"
    key.write_text(private_pem)
    monkeypatch.setattr(config, "GITHUB_APP_ID", APP_ID)
    monkeypatch.setattr(config, "GITHUB_APP_PRIVATE_KEY_PATH", str(key))
    monkeypatch.setattr(config, "GITHUB_APP_INSTALLATION_ID", 0)

    assert github.from_config().installation_id is None


# --- the setup CLI -----------------------------------------------------------


def test_the_cli_reports_a_missing_configuration(monkeypatch, caplog):
    from orchestrator import config, github

    monkeypatch.setattr(config, "GITHUB_APP_ID", "")
    with caplog.at_level("ERROR", logger="orchestrator.github"):
        assert github.main() == 2
    assert "GITHUB_APP_ID" in caplog.text


def test_the_cli_lists_installations_when_none_is_chosen(
    keypair, tmp_path, monkeypatch, caplog
):
    from orchestrator import config, github

    private_pem, _ = keypair
    key = tmp_path / "app.pem"
    key.write_text(private_pem)
    monkeypatch.setattr(config, "GITHUB_APP_ID", APP_ID)
    monkeypatch.setattr(config, "GITHUB_APP_PRIVATE_KEY_PATH", str(key))
    monkeypatch.setattr(config, "GITHUB_APP_INSTALLATION_ID", 0)
    monkeypatch.setattr(
        github.urllib.request,
        "urlopen",
        FakeGitHub(
            {
                "/app/installations": [
                    {
                        "id": 42,
                        "account": {"login": "wanderindev"},
                        "repository_selection": "selected",
                    }
                ]
            }
        ),
    )

    with caplog.at_level("INFO", logger="orchestrator.github"):
        assert github.main() == 0

    assert "installation 42" in caplog.text
    assert "INSTALLATION_ID" in caplog.text


def test_the_cli_warns_when_the_app_can_reach_more_than_intended(
    keypair, tmp_path, monkeypatch, caplog
):
    """Installing on "all repositories" is one careless click during setup."""
    from orchestrator import config, github

    private_pem, _ = keypair
    key = tmp_path / "app.pem"
    key.write_text(private_pem)
    monkeypatch.setattr(config, "GITHUB_APP_ID", APP_ID)
    monkeypatch.setattr(config, "GITHUB_APP_PRIVATE_KEY_PATH", str(key))
    monkeypatch.setattr(config, "GITHUB_APP_INSTALLATION_ID", 42)
    monkeypatch.setattr(
        github.urllib.request,
        "urlopen",
        FakeGitHub(
            {
                "/app/installations": [
                    {
                        "id": 42,
                        "account": {"login": "wanderindev"},
                        "repository_selection": "all",
                    },
                ],
                "/app/installations/42/access_tokens": {"token": "t"},
                "/installation/repositories": {
                    "repositories": [
                        {"full_name": "wanderindev/feliu-dev"},
                        {"full_name": "wanderindev/panama-in-context"},
                        {"full_name": "wanderindev/pic-extension"},
                    ]
                },
            }
        ),
    )

    with caplog.at_level("INFO", logger="orchestrator.github"):
        github.main()

    assert "beyond the intended two" in caplog.text
    assert "pic-extension" in caplog.text
