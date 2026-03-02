from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.secret_scanner import (
    SecretFinding,
    SecretPattern,
    redact_content,
    scan_and_redact,
    scan_content,
)


class TestSecretPatternDataclass:
    def test_frozen_immutability(self) -> None:
        pattern = SecretPattern(
            name="test", pattern=r"secret", severity="high",
        )
        with pytest.raises(AttributeError):
            pattern.name = "changed"

    def test_fields_present(self) -> None:
        pattern = SecretPattern(
            name="aws_access_key", pattern=r"AKIA[A-Z0-9]{16}", severity="critical",
        )
        assert pattern.name == "aws_access_key"
        assert pattern.severity == "critical"


class TestSecretFindingDataclass:
    def test_frozen_immutability(self) -> None:
        finding = SecretFinding(
            pattern_name="aws_access_key",
            line=5,
            column=10,
            matched_text_preview="AKIA****",
        )
        with pytest.raises(AttributeError):
            finding.line = 99

    def test_fields_present(self) -> None:
        finding = SecretFinding(
            pattern_name="github_token",
            line=12,
            column=3,
            matched_text_preview="ghp_****",
        )
        assert finding.pattern_name == "github_token"
        assert finding.line == 12
        assert finding.column == 3


class TestScanContentAWSKeys:
    def test_detects_aws_access_key(self) -> None:
        content = 'aws_key = "AKIAIOSFODNN7EXAMPLE"'
        findings = scan_content(content)
        assert len(findings) >= 1
        aws_findings = [f for f in findings if f.pattern_name == "aws_access_key"]
        assert len(aws_findings) == 1
        assert aws_findings[0].line == 1

    def test_detects_aws_key_multiline(self) -> None:
        content = "line one\nline two\nAKIAIOSFODNN7EXAMPLE\nline four"
        findings = scan_content(content)
        aws_findings = [f for f in findings if f.pattern_name == "aws_access_key"]
        assert len(aws_findings) == 1
        assert aws_findings[0].line == 3


class TestScanContentGitHubTokens:
    def test_detects_ghp_token(self) -> None:
        content = 'token = "ghp_ABCDEFghijklmnopqrstuvwxyz0123456789"'
        findings = scan_content(content)
        gh_findings = [f for f in findings if f.pattern_name == "github_token"]
        assert len(gh_findings) == 1

    def test_detects_ghs_token(self) -> None:
        content = 'GH_TOKEN=ghs_ABCDEFghijklmnopqrstuvwxyz0123456789'
        findings = scan_content(content)
        gh_findings = [f for f in findings if f.pattern_name == "github_token"]
        assert len(gh_findings) == 1

    def test_detects_github_pat(self) -> None:
        content = 'export GITHUB_TOKEN="github_pat_11ABCDEF0123456789_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRST"'
        findings = scan_content(content)
        gh_findings = [f for f in findings if f.pattern_name == "github_pat"]
        assert len(gh_findings) == 1


class TestScanContentOpenAIKeys:
    def test_detects_sk_proj_key(self) -> None:
        content = 'OPENAI_API_KEY="sk-proj-abcdefghijklmnopqrstuvwxyz123456"'
        findings = scan_content(content)
        oai_findings = [f for f in findings if f.pattern_name == "openai_key"]
        assert len(oai_findings) == 1

    def test_detects_sk_live_key(self) -> None:
        content = 'api_key = "sk-live-abcdefghijklmnopqrstuvwxyz123456"'
        findings = scan_content(content)
        oai_findings = [f for f in findings if f.pattern_name == "openai_key"]
        assert len(oai_findings) == 1


class TestScanContentPrivateKeys:
    def test_detects_rsa_private_key(self) -> None:
        content = (
            "-----BEGIN RSA PRIVATE KEY-----\n"
            "MIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn/ygWyF8PbnGy5AHB\n"
            "-----END RSA PRIVATE KEY-----"
        )
        findings = scan_content(content)
        pk_findings = [f for f in findings if f.pattern_name == "private_key"]
        assert len(pk_findings) == 1
        assert pk_findings[0].line == 1

    def test_detects_ec_private_key(self) -> None:
        content = (
            "-----BEGIN EC PRIVATE KEY-----\n"
            "MHQCAQEEIFakeKeyDataHere\n"
            "-----END EC PRIVATE KEY-----"
        )
        findings = scan_content(content)
        pk_findings = [f for f in findings if f.pattern_name == "private_key"]
        assert len(pk_findings) == 1


class TestScanContentPasswordAssignments:
    def test_detects_password_equals_string(self) -> None:
        content = 'password = "super_secret_123"'
        findings = scan_content(content)
        pw_findings = [f for f in findings if f.pattern_name == "password_assignment"]
        assert len(pw_findings) == 1

    def test_detects_PASSWORD_colon(self) -> None:
        content = 'PASSWORD: "my-database-pass"'
        findings = scan_content(content)
        pw_findings = [f for f in findings if f.pattern_name == "password_assignment"]
        assert len(pw_findings) == 1

    def test_detects_db_password_env(self) -> None:
        content = 'DB_PASSWORD="prod_db_pass_123"'
        findings = scan_content(content)
        pw_findings = [f for f in findings if f.pattern_name == "password_assignment"]
        assert len(pw_findings) == 1


class TestScanContentGenericAPIKeys:
    def test_detects_api_key_equals(self) -> None:
        content = 'api_key = "abcdef123456789012345678"'
        findings = scan_content(content)
        api_findings = [f for f in findings if f.pattern_name == "generic_api_key"]
        assert len(api_findings) == 1

    def test_detects_apikey_colon(self) -> None:
        content = 'apikey: "abcdef123456789012345678"'
        findings = scan_content(content)
        api_findings = [f for f in findings if f.pattern_name == "generic_api_key"]
        assert len(api_findings) == 1


class TestScanContentJWTTokens:
    def test_detects_jwt_token(self) -> None:
        content = 'token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"'
        findings = scan_content(content)
        jwt_findings = [f for f in findings if f.pattern_name == "jwt_token"]
        assert len(jwt_findings) == 1


class TestFalsePositiveAvoidance:
    def test_word_password_in_comment_not_flagged(self) -> None:
        content = "# This function validates the password field"
        findings = scan_content(content)
        pw_findings = [f for f in findings if f.pattern_name == "password_assignment"]
        assert len(pw_findings) == 0

    def test_password_variable_declaration_not_flagged(self) -> None:
        content = "password = get_password_from_vault()"
        findings = scan_content(content)
        pw_findings = [f for f in findings if f.pattern_name == "password_assignment"]
        assert len(pw_findings) == 0

    def test_short_strings_not_flagged_as_api_keys(self) -> None:
        content = 'api_key = "short"'
        findings = scan_content(content)
        api_findings = [f for f in findings if f.pattern_name == "generic_api_key"]
        assert len(api_findings) == 0

    def test_import_statements_not_flagged(self) -> None:
        content = "from password_utils import validate_password"
        findings = scan_content(content)
        pw_findings = [f for f in findings if f.pattern_name == "password_assignment"]
        assert len(pw_findings) == 0


class TestRedactContent:
    def test_redacts_aws_key(self) -> None:
        content = 'aws_key = "AKIAIOSFODNN7EXAMPLE"'
        redacted = redact_content(content)
        assert "AKIAIOSFODNN7EXAMPLE" not in redacted
        assert "[REDACTED-aws_access_key]" in redacted

    def test_redacts_github_token(self) -> None:
        content = 'token = "ghp_ABCDEFghijklmnopqrstuvwxyz0123456789"'
        redacted = redact_content(content)
        assert "ghp_ABCDEF" not in redacted
        assert "[REDACTED-github_token]" in redacted

    def test_redacts_private_key_block(self) -> None:
        content = (
            "some code\n"
            "-----BEGIN RSA PRIVATE KEY-----\n"
            "MIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn\n"
            "-----END RSA PRIVATE KEY-----\n"
            "more code"
        )
        redacted = redact_content(content)
        assert "BEGIN RSA PRIVATE KEY" not in redacted
        assert "[REDACTED-private_key]" in redacted
        assert "some code" in redacted
        assert "more code" in redacted

    def test_preserves_line_count(self) -> None:
        content = "line1\nAKIAIOSFODNN7EXAMPLE\nline3"
        redacted = redact_content(content)
        assert redacted.count("\n") == content.count("\n")

    def test_redacts_multiple_secrets_same_content(self) -> None:
        content = (
            'aws_key = "AKIAIOSFODNN7EXAMPLE"\n'
            'token = "ghp_ABCDEFghijklmnopqrstuvwxyz0123456789"'
        )
        redacted = redact_content(content)
        assert "AKIAIOSFODNN7EXAMPLE" not in redacted
        assert "ghp_ABCDEF" not in redacted
        assert "[REDACTED-aws_access_key]" in redacted
        assert "[REDACTED-github_token]" in redacted


class TestScanAndRedact:
    def test_returns_tuple_of_redacted_and_findings(self) -> None:
        content = 'aws_key = "AKIAIOSFODNN7EXAMPLE"'
        redacted, findings = scan_and_redact(content)
        assert isinstance(redacted, str)
        assert isinstance(findings, list)
        assert "AKIAIOSFODNN7EXAMPLE" not in redacted
        assert len(findings) >= 1

    def test_empty_content_returns_empty(self) -> None:
        redacted, findings = scan_and_redact("")
        assert redacted == ""
        assert findings == []

    def test_clean_content_returns_unchanged(self) -> None:
        content = "def hello():\n    return 42"
        redacted, findings = scan_and_redact(content)
        assert redacted == content
        assert findings == []


class TestScanContentLineAndColumn:
    def test_finding_has_correct_line(self) -> None:
        content = "safe line\nAKIAIOSFODNN7EXAMPLE\nanother safe line"
        findings = scan_content(content)
        aws_findings = [f for f in findings if f.pattern_name == "aws_access_key"]
        assert aws_findings[0].line == 2

    def test_finding_has_correct_column(self) -> None:
        content = "prefix AKIAIOSFODNN7EXAMPLE suffix"
        findings = scan_content(content)
        aws_findings = [f for f in findings if f.pattern_name == "aws_access_key"]
        assert aws_findings[0].column == 8

    def test_matched_text_preview_is_truncated(self) -> None:
        content = 'ghp_ABCDEFghijklmnopqrstuvwxyz0123456789'
        findings = scan_content(content)
        gh_findings = [f for f in findings if f.pattern_name == "github_token"]
        assert len(gh_findings[0].matched_text_preview) <= 12


class TestPipelineIntegration:
    @pytest.mark.asyncio
    async def test_enrich_with_llm_calls_scanner(self) -> None:
        from orchestrator.app.graph_builder import enrich_with_llm

        mock_state = {
            "raw_files": [
                {
                    "path": "main.py",
                    "content": 'AWS_KEY = "AKIAIOSFODNN7EXAMPLE"\ndef main(): pass',
                },
            ],
            "extracted_nodes": [],
        }

        with patch(
            "orchestrator.app.graph_builder.scan_and_redact",
        ) as mock_scan, patch(
            "orchestrator.app.graph_builder._build_extractor",
        ) as mock_extractor_factory:
            mock_scan.return_value = ("[REDACTED]", [])
            mock_result = MagicMock()
            mock_result.services = []
            mock_result.calls = []
            extractor_instance = MagicMock()
            extractor_instance.extract_all = AsyncMock(return_value=mock_result)
            mock_extractor_factory.return_value = extractor_instance
            await enrich_with_llm(mock_state)
            mock_scan.assert_called()
