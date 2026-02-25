from unittest.mock import patch

from orchestrator.app.rag_evaluator import (
    EvaluationStore,
    RedisEvaluationStore,
    create_evaluation_store,
)


def test_create_evaluation_store_returns_redis_when_url_set():
    with patch.dict("os.environ", {"REDIS_URL": "redis://localhost:6379"}):
        store = create_evaluation_store()
    assert isinstance(store, RedisEvaluationStore)


def test_create_evaluation_store_returns_local_when_no_url():
    with patch.dict("os.environ", {"REDIS_URL": ""}, clear=False):
        store = create_evaluation_store()
    assert isinstance(store, EvaluationStore)
    assert not isinstance(store, RedisEvaluationStore)


def test_query_engine_imports_factory():
    import orchestrator.app.query_engine as qe_mod
    assert hasattr(qe_mod, "create_evaluation_store")
