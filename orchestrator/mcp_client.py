"""A minimal host-side MCP client: JSON-RPC over Streamable HTTP. #84.

The orchestrator itself calls one PIC MCP tool (``retry_agent_task``, see
orchestrator/auto_retry.py), so it needs ``initialize`` and ``tools/call``
and nothing else. No MCP SDK: the runtime dependencies stay short, and this
is a few dozen lines of stdlib.

PIC serves the endpoint stateless with ``json_response=True`` (its
app/mcp/server.py), so a POST is answered with one JSON body. An SSE body
(``text/event-stream``) is read too, as is an ``Mcp-Session-Id``, so a server
that changes either setting still works.

Every client is built with the tools it may call (``allowed_tools``): a call
to any other is refused before anything leaves the process. The token is sent
as a bearer header and never appears in an error message.
"""

import json
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable
from typing import Any

PROTOCOL_VERSION = "2025-06-18"

CLIENT_INFO = {"name": "managed-agents-orchestrator", "version": "1"}


class McpError(Exception):
    """The request failed in transport or protocol: whether a tool ran is unknown."""


class ToolError(Exception):
    """The tool ran and answered with an error (``isError``); its text is the message."""


class Client:
    def __init__(
        self,
        url: str,
        token: str,
        *,
        allowed_tools: Iterable[str],
        timeout: float = 30,
        opener: Callable[..., Any] = urllib.request.urlopen,
    ) -> None:
        self.url = url
        self._token = token
        self.allowed_tools = frozenset(allowed_tools)
        self.timeout = timeout
        self._opener = opener
        self._ids = 0
        self._session: str | None = None
        self._protocol: str | None = None
        self._initialized = False

    def _post(self, message: dict[str, Any]) -> dict[str, Any] | None:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": f"Bearer {self._token}",
        }
        if self._session:
            headers["Mcp-Session-Id"] = self._session
        if self._protocol:
            headers["MCP-Protocol-Version"] = self._protocol
        request = urllib.request.Request(
            self.url, data=json.dumps(message).encode(), headers=headers, method="POST"
        )
        try:
            with self._opener(request, timeout=self.timeout) as response:
                self._session = response.headers.get("Mcp-Session-Id") or self._session
                content_type = response.headers.get("Content-Type") or ""
                raw = response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as err:
            raise McpError(f"HTTP {err.code} from the MCP server") from None
        except (urllib.error.URLError, OSError, ValueError) as err:
            raise McpError(f"MCP server unreachable ({type(err).__name__})") from None
        if "id" not in message:
            return None  # a notification: the server answers 202, no body
        reply = _reply(raw, content_type, message["id"])
        if "error" in reply:
            error = reply["error"] if isinstance(reply["error"], dict) else {}
            raise McpError(
                f"JSON-RPC error {error.get('code')}: {error.get('message')}"
            )
        result = reply.get("result")
        if not isinstance(result, dict):
            raise McpError("MCP reply carries no result")
        return result

    def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self._ids += 1
        result = self._post(
            {"jsonrpc": "2.0", "id": self._ids, "method": method, "params": params}
        )
        assert result is not None
        return result

    def initialize(self) -> None:
        result = self._request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": CLIENT_INFO,
            },
        )
        self._protocol = str(result.get("protocolVersion") or PROTOCOL_VERSION)
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        self._initialized = True

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """The tool's structured result. Raises :class:`ToolError` when the tool
        answered with an error, :class:`McpError` when the call failed."""
        if name not in self.allowed_tools:
            raise PermissionError(f"tool {name!r} is not allowed for this client")
        if not self._initialized:
            self.initialize()
        result = self._request("tools/call", {"name": name, "arguments": arguments})
        text = "\n".join(
            str(block.get("text") or "")
            for block in result.get("content") or []
            if isinstance(block, dict) and block.get("type") == "text"
        )
        if result.get("isError"):
            raise ToolError(text or "tool error")
        structured = result.get("structuredContent")
        if isinstance(structured, dict):
            return structured
        try:
            parsed = json.loads(text)
        except ValueError:
            return {"text": text}
        return parsed if isinstance(parsed, dict) else {"result": parsed}


def _reply(raw: str, content_type: str, request_id: int) -> dict[str, Any]:
    """The JSON-RPC response to ``request_id`` in a JSON or SSE body."""
    if content_type.startswith("text/event-stream"):
        candidates = []
        for line in raw.splitlines():
            if line.startswith("data:"):
                try:
                    candidates.append(json.loads(line[5:].strip()))
                except ValueError:
                    continue
    else:
        try:
            candidates = [json.loads(raw)]
        except ValueError:
            raise McpError("MCP reply is not JSON") from None
    for item in candidates:
        if isinstance(item, list):
            item = next(
                (m for m in item if isinstance(m, dict) and m.get("id") == request_id),
                None,
            )
        if isinstance(item, dict) and item.get("id") == request_id:
            return item
    raise McpError("MCP reply does not answer the request")
