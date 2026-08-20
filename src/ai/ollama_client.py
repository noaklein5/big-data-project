"""Ollama HTTP client for chat completions."""

from __future__ import annotations

import httpx

from src.config import OLLAMA_MODEL, OLLAMA_URL

DEFAULT_TIMEOUT = httpx.Timeout(120.0, connect=10.0)


class OllamaError(RuntimeError):
    """Raised when the Ollama API returns an error."""


def chat(
    system_prompt: str,
    user_message: str,
    *,
    model: str | None = None,
    base_url: str | None = None,
    timeout: httpx.Timeout | None = None,
) -> str:
    """Send a chat request and return the assistant message content."""
    url = f"{(base_url or OLLAMA_URL).rstrip('/')}/api/chat"
    payload = {
        "model": model or OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "stream": False,
        "format": "json",
        "options": {"temperature": 0},
    }

    try:
        response = httpx.post(url, json=payload, timeout=timeout or DEFAULT_TIMEOUT)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise OllamaError(f"Ollama request failed: {exc}") from exc

    data = response.json()
    message = data.get("message") or {}
    content = message.get("content", "").strip()
    if not content:
        raise OllamaError("Ollama returned an empty response")
    return content
