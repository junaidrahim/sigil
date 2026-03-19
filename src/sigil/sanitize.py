"""Sanitization and redaction engine.

Philosophy: aggressive by default. Better to lose data than leak proprietary context.
"""

from __future__ import annotations

import re

from sigil.config import SanitizeConfig

# Matches fenced code blocks (```...```) and inline code (`...`)
_FENCED_CODE_RE = re.compile(r"```[\s\S]*?```", re.MULTILINE)
_INLINE_CODE_RE = re.compile(r"`[^`\n]+`")


def sanitize_text(text: str, config: SanitizeConfig) -> str:
    """Apply all sanitization rules to a text string."""
    if not text:
        return text

    # Strip code blocks first (before path/pattern redaction so we don't
    # waste time redacting inside code we're about to remove)
    if config.strip_code_blocks:
        text = strip_code_blocks(text)

    # Strip/redact file paths
    text = strip_paths(text, config.strip_paths)

    # Redact patterns (API keys, internal URLs, etc.)
    text = redact_patterns(text, config.redact_patterns)

    return text


def strip_code_blocks(text: str) -> str:
    """Remove fenced code blocks and inline code from text."""
    text = _FENCED_CODE_RE.sub("[code block removed]", text)
    text = _INLINE_CODE_RE.sub("[code removed]", text)
    return text


def strip_paths(text: str, paths: list[str]) -> str:
    """Replace full file paths that start with sensitive prefixes.

    Replaces the entire path (prefix + any non-whitespace continuation)
    so that nothing after the prefix leaks.
    """
    for path in paths:
        if path:
            # Escape the path for regex, then match any non-whitespace after it
            pattern = re.escape(path) + r"\S*"
            text = re.sub(pattern, "[path redacted]", text)
    return text


def redact_patterns(text: str, patterns: list[str]) -> str:
    """Redact strings matching configured regex patterns."""
    for pattern in patterns:
        try:
            text = re.sub(pattern, "[redacted]", text)
        except re.error:
            # Skip invalid patterns rather than crash
            continue
    return text


def sanitize_project_path(path: str | None, strip_paths: list[str]) -> str | None:
    """Sanitize a project path, redacting sensitive prefixes."""
    if not path:
        return None
    for p in strip_paths:
        if path.startswith(p):
            return "[path redacted]"
    return path
