"""Tests for the sanitization engine. This is the critical path — must not leak data."""

from sigil.config import SanitizeConfig
from sigil.sanitize import (
    redact_patterns,
    sanitize_project_path,
    sanitize_text,
    strip_code_blocks,
    strip_paths,
)


class TestStripCodeBlocks:
    def test_fenced_code_block(self):
        text = "before\n```python\ndef secret():\n    pass\n```\nafter"
        result = strip_code_blocks(text)
        assert "secret" not in result
        assert "before" in result
        assert "after" in result
        assert "[code block removed]" in result

    def test_inline_code(self):
        text = "Run `rm -rf /secret/path` to clean up"
        result = strip_code_blocks(text)
        assert "rm -rf" not in result
        assert "[code removed]" in result

    def test_multiple_code_blocks(self):
        text = "```\nblock1\n```\nmiddle\n```\nblock2\n```"
        result = strip_code_blocks(text)
        assert "block1" not in result
        assert "block2" not in result
        assert "middle" in result

    def test_no_code_blocks(self):
        text = "Just plain text with no code"
        assert strip_code_blocks(text) == text

    def test_empty_string(self):
        assert strip_code_blocks("") == ""

    def test_backtick_without_closure_preserved(self):
        text = "Use the ` character for quoting"
        result = strip_code_blocks(text)
        assert result == text

    def test_nested_backticks_in_fenced(self):
        text = "before\n```\nsome `inline` inside\n```\nafter"
        result = strip_code_blocks(text)
        assert "inline" not in result
        assert "before" in result


class TestStripPaths:
    def test_basic_path_stripping(self):
        text = "Found file at /Users/junaid/work/secret-project/main.py"
        result = strip_paths(text, ["/Users/junaid/work/"])
        assert "secret-project" not in result
        assert "[path redacted]" in result

    def test_multiple_paths(self):
        text = "Paths: /home/junaid/atlan/foo and /Users/junaid/work/bar"
        result = strip_paths(text, ["/home/junaid/atlan/", "/Users/junaid/work/"])
        assert "foo" not in result
        assert "bar" not in result

    def test_no_matching_paths(self):
        text = "File at /tmp/safe/file.txt"
        result = strip_paths(text, ["/Users/junaid/work/"])
        assert result == text

    def test_empty_path_list(self):
        text = "Some text"
        assert strip_paths(text, []) == text

    def test_path_appears_multiple_times(self):
        text = "/Users/junaid/work/a and /Users/junaid/work/b"
        result = strip_paths(text, ["/Users/junaid/work/"])
        assert result.count("[path redacted]") == 2


class TestRedactPatterns:
    def test_api_key_redaction(self):
        text = "Using key sk-abc123XYZ for auth"
        result = redact_patterns(text, [r"sk-[a-zA-Z0-9]+"])
        assert "sk-abc123XYZ" not in result
        assert "[redacted]" in result

    def test_domain_redaction(self):
        text = "Check https://internal.atlan.com/dashboard"
        result = redact_patterns(text, [r"atlan\.com"])
        assert "atlan.com" not in result
        assert "[redacted]" in result

    def test_multiple_patterns(self):
        text = "Key: sk-test123 URL: foo.atlan.com"
        result = redact_patterns(text, [r"sk-[a-zA-Z0-9]+", r"atlan\.com"])
        assert "sk-test123" not in result
        assert "atlan.com" not in result

    def test_invalid_regex_skipped(self):
        text = "Some text"
        result = redact_patterns(text, [r"[invalid"])
        assert result == text

    def test_no_patterns(self):
        text = "Some text"
        assert redact_patterns(text, []) == text

    def test_overlapping_matches(self):
        text = "sk-abcdef sk-xyz"
        result = redact_patterns(text, [r"sk-[a-zA-Z0-9]+"])
        assert "sk-" not in result


class TestSanitizeText:
    def test_full_pipeline(self):
        config = SanitizeConfig(
            strip_paths=["/Users/junaid/work/"],
            redact_patterns=[r"sk-[a-zA-Z0-9]+", r"atlan\.com"],
            strip_code_blocks=True,
        )
        text = (
            "Working on /Users/junaid/work/secret/file.py\n"
            "API key is sk-secret123\n"
            "Dashboard at internal.atlan.com\n"
            "```python\ndef hack():\n    pass\n```\n"
            "Done"
        )
        result = sanitize_text(text, config)
        assert "secret" not in result
        assert "sk-secret123" not in result
        assert "atlan.com" not in result
        assert "def hack" not in result
        assert "Done" in result

    def test_code_blocks_stripped_before_patterns(self):
        """Code blocks are stripped first, so patterns inside them don't matter."""
        config = SanitizeConfig(
            strip_paths=[],
            redact_patterns=[r"SENSITIVE"],
            strip_code_blocks=True,
        )
        text = "```\nSENSITIVE data\n```"
        result = sanitize_text(text, config)
        assert "SENSITIVE" not in result

    def test_disabled_code_block_stripping(self):
        config = SanitizeConfig(strip_code_blocks=False)
        text = "```\ncode here\n```"
        result = sanitize_text(text, config)
        assert "code here" in result

    def test_empty_text(self):
        config = SanitizeConfig()
        assert sanitize_text("", config) == ""

    def test_none_text(self):
        config = SanitizeConfig()
        assert sanitize_text(None, config) is None


class TestSanitizeProjectPath:
    def test_sensitive_path_redacted(self):
        result = sanitize_project_path(
            "/Users/junaid/work/secret-project",
            ["/Users/junaid/work/"],
        )
        assert result == "[path redacted]"

    def test_safe_path_preserved(self):
        result = sanitize_project_path(
            "/Users/junaid/personal/blog",
            ["/Users/junaid/work/"],
        )
        assert result == "/Users/junaid/personal/blog"

    def test_none_path(self):
        assert sanitize_project_path(None, ["/foo/"]) is None

    def test_empty_strip_list(self):
        result = sanitize_project_path("/some/path", [])
        assert result == "/some/path"
