"""Keep the orchestrator's own secrets out of the log and out of email. #80.

Some secrets are handed *into* sandboxes (the PIC MCP read token, as an env
var), and an agent can echo its environment into the stream as easily as any
other text. The PIC MCP pipeline token (#84) never enters a sandbox, but the
host's own MCP calls are logged, so it is scrubbed the same way. Redaction therefore happens where things leave the process for
good: :func:`orchestrator.log.append` (the append-only ``agent_events``, which
cannot be scrubbed after the fact) and the notifier's transport.

Exact-value replacement, not pattern matching: the values are known, and a
pattern broad enough to catch an opaque token would mangle ordinary text.
"""

from typing import Any

from orchestrator import config

REDACTED = "[REDACTED]"

#: Shorter values are ignored: an empty or near-empty setting would otherwise
#: "redact" every string in the log. Real tokens are far longer.
_MIN_SECRET_LEN = 8


def secrets() -> tuple[str, ...]:
    """The values to redact, read at call time so tests can patch config."""
    values = (config.PIC_MCP_READ_TOKEN, config.PIC_MCP_PIPELINE_TOKEN)
    return tuple(v for v in values if v and len(v) >= _MIN_SECRET_LEN)


def redact_text(text: str, values: tuple[str, ...] | None = None) -> str:
    for value in secrets() if values is None else values:
        text = text.replace(value, REDACTED)
    return text


def redact(value: Any, values: tuple[str, ...] | None = None) -> Any:
    """``value`` with every secret replaced, walking dicts, lists and tuples.

    Keys are redacted too: a stream event can carry arbitrary JSON. Anything
    that is not a string or a container is returned as is.
    """
    values = secrets() if values is None else values
    if not values:
        return value
    if isinstance(value, str):
        return redact_text(value, values)
    if isinstance(value, dict):
        return {redact(k, values): redact(v, values) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return type(value)(redact(v, values) for v in value)
    return value
