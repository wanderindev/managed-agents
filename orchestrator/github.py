"""GitHub App authentication. Issue #11.

The orchestrator holds an App private key. Per job it mints an **installation
token**: valid one hour, scoped to the two repos the App is installed on, and
limited to the two permissions the App was granted. That token is what goes into
the sandbox.

Why an App rather than a personal access token, or a Claude connector:

* A PAT carries the whole account and does not expire. An installation token
  expires in an hour and can only touch what the App was installed on, so the
  blast radius of a leak is a pull request on a personal repo.
* A connector operates at the API level. ``git clone`` and ``git push`` need a
  credential in the git transport, which no connector provides.

This is the 90/5 version of the egress-proxy pattern from the trends item that
prompted #11: that post keeps the real credential outside the sandbox entirely by
rewriting headers at a proxy, which needs MITM TLS and only pays off when the
sandbox is untrusted. Ours is trusted, so a short-lived scoped token gets most of
the benefit for almost none of the work.
"""

import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import jwt

from orchestrator import config

logger = logging.getLogger(__name__)

#: GitHub rejects an app JWT whose lifetime exceeds 10 minutes. 9 leaves room
#: for clock skew without arguing about it.
_JWT_LIFETIME_SECONDS = 540

#: Backdated to absorb a slow clock on the droplet. GitHub rejects a JWT issued
#: in the future, and a host drifting by a few seconds is ordinary.
_JWT_BACKDATE_SECONDS = 60


class GitHubAppError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Installation:
    id: int
    account: str
    repository_selection: str


class GitHubAppAuth:
    def __init__(
        self,
        app_id: str,
        private_key: str,
        installation_id: int | None = None,
        *,
        api_url: str = "https://api.github.com",
        opener=None,
        now=time.time,
    ) -> None:
        self.app_id = app_id
        self.private_key = private_key
        self.installation_id = installation_id
        self.api_url = api_url.rstrip("/")
        # Resolved here rather than as a default argument. A module attribute
        # captured at class-definition time cannot be patched afterwards, which
        # is how a test ended up making a real request to api.github.com.
        self._open = opener or urllib.request.urlopen
        self._now = now

    # --- app-level ------------------------------------------------------------

    def app_jwt(self) -> str:
        """Short-lived JWT proving we hold the App's private key."""
        issued = int(self._now()) - _JWT_BACKDATE_SECONDS
        return jwt.encode(
            {
                "iat": issued,
                "exp": issued + _JWT_LIFETIME_SECONDS,
                "iss": self.app_id,
            },
            self.private_key,
            algorithm="RS256",
        )

    def installations(self) -> list[Installation]:
        """Every installation of this App. How you find the installation id."""
        payload = self._request("GET", "/app/installations", self.app_jwt())
        return [
            Installation(
                id=raw["id"],
                account=(raw.get("account") or {}).get("login", "?"),
                repository_selection=raw.get("repository_selection", "?"),
            )
            for raw in payload
        ]

    # --- installation-level ---------------------------------------------------

    def installation_token(self) -> str:
        """Mint a fresh one-hour token for this installation.

        Deliberately not cached. Minting is one request, jobs are minutes apart at
        a concurrency of 1, and a cache only buys the chance of handing a sandbox
        a token that expires mid-run. Each sandbox getting its own token is also
        what makes a leaked one traceable to a single job.
        """
        if self.installation_id is None:
            raise GitHubAppError(
                "no installation id; run `python -m orchestrator.github` to list them"
            )
        payload = self._request(
            "POST",
            f"/app/installations/{self.installation_id}/access_tokens",
            self.app_jwt(),
        )
        token = payload.get("token")
        if not token:
            raise GitHubAppError("GitHub returned no token")
        logger.info(
            "minted an installation token, expires %s", payload.get("expires_at")
        )
        return token

    def repositories(self) -> list[str]:
        """Full names of the repos this installation can reach.

        Worth checking after setup: an App installed on "all repositories" would
        hand every sandbox far more reach than #11 intends.
        """
        payload = self._request(
            "GET", "/installation/repositories", self.installation_token()
        )
        return [repo["full_name"] for repo in payload.get("repositories", [])]

    # --- transport ------------------------------------------------------------

    def _request(self, method: str, path: str, bearer: str):
        request = urllib.request.Request(
            f"{self.api_url}{path}",
            method=method,
            headers={
                "Authorization": f"Bearer {bearer}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "wanderindev-managed-agents",
            },
        )
        try:
            with self._open(request, timeout=30) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")[:400]
            raise GitHubAppError(f"{method} {path} failed: {exc.code} {body}") from exc


def from_config() -> GitHubAppAuth:
    """Build the auth from the environment, reading the key off disk."""
    if not config.GITHUB_APP_ID:
        raise GitHubAppError("ORCHESTRATOR_GITHUB_APP_ID is not set")
    key_path = Path(config.GITHUB_APP_PRIVATE_KEY_PATH)
    if not key_path.is_file():
        raise GitHubAppError(f"no App private key at {key_path}")
    return GitHubAppAuth(
        config.GITHUB_APP_ID,
        key_path.read_text(),
        config.GITHUB_APP_INSTALLATION_ID or None,
    )


def main() -> int:
    """Report what the App can see. Setup aid, not part of the loop.

    python -m orchestrator.github
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        auth = from_config()
    except GitHubAppError as exc:
        logger.error("%s", exc)
        return 2

    for installation in auth.installations():
        marker = " <- configured" if installation.id == auth.installation_id else ""
        logger.info(
            "installation %s  account=%s  repos=%s%s",
            installation.id,
            installation.account,
            installation.repository_selection,
            marker,
        )

    if auth.installation_id is None:
        logger.info(
            "\nSet ORCHESTRATOR_GITHUB_APP_INSTALLATION_ID to one of the ids above."
        )
        return 0

    repos = auth.repositories()
    logger.info("\nreachable repositories (%s):", len(repos))
    for name in repos:
        logger.info("  %s", name)
    # An App installed on every repository would hand each sandbox far more reach
    # than #11 intends, and it is an easy box to tick by accident during setup.
    unexpected = set(repos) - set(config.GITHUB_EXPECTED_REPOS)
    if unexpected:
        logger.warning(
            "\nWARNING: reachable beyond the intended two: %s", sorted(unexpected)
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
