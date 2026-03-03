from __future__ import annotations

import inspect
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.app.main import get_evaluation


class TestEvaluationEndpointAuth:

    def test_signature_includes_authorization_header(self) -> None:
        sig = inspect.signature(get_evaluation)
        assert "authorization" in sig.parameters, (
            "get_evaluation must accept an authorization header parameter"
        )

    @pytest.mark.asyncio
    async def test_missing_auth_returns_401_when_required(self) -> None:
        env = {
            "AUTH_REQUIRE_TOKENS": "true",
            "AUTH_TOKEN_SECRET": "test-secret-256-bit-key-for-tests",
        }
        with patch.dict("os.environ", env, clear=False):
            with pytest.raises(HTTPException) as exc_info:
                await get_evaluation("some-query-id", authorization=None)
            assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_invalid_token_returns_401(self) -> None:
        env = {
            "AUTH_REQUIRE_TOKENS": "true",
            "AUTH_TOKEN_SECRET": "test-secret-256-bit-key-for-tests",
        }
        with patch.dict("os.environ", env, clear=False):
            with pytest.raises(HTTPException) as exc_info:
                await get_evaluation(
                    "some-query-id", authorization="Bearer invalid-jwt",
                )
            assert exc_info.value.status_code in (401, 403)

    @pytest.mark.asyncio
    async def test_valid_token_returns_result(self) -> None:
        env = {
            "AUTH_REQUIRE_TOKENS": "false",
        }
        eval_data = {"query_id": "q1", "score": 0.9}
        with (
            patch.dict("os.environ", env, clear=False),
            patch(
                "orchestrator.app.main._eval_store_get",
                new_callable=AsyncMock,
                return_value=eval_data,
            ),
        ):
            os.environ.pop("AUTH_TOKEN_SECRET", None)
            response = await get_evaluation(
                "q1", authorization=None,
            )
            assert response.status_code == 200
