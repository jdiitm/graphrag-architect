from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.config import RAGEvalConfig


class TestLLMEvaluatorWiring:

    def test_rag_eval_config_has_use_llm_judge_field(self) -> None:
        config = RAGEvalConfig()
        assert hasattr(config, "use_llm_judge"), (
            "RAGEvalConfig must have a 'use_llm_judge' field"
        )
        assert config.use_llm_judge is True, (
            "use_llm_judge must default to True"
        )

    def test_rag_eval_config_from_env_reads_llm_judge(self) -> None:
        with patch.dict("os.environ", {"RAG_USE_LLM_JUDGE": "false"}):
            config = RAGEvalConfig.from_env()
        assert config.use_llm_judge is False

    def test_rag_eval_config_from_env_defaults_true(self) -> None:
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("RAG_USE_LLM_JUDGE", None)
            config = RAGEvalConfig.from_env()
        assert config.use_llm_judge is True
