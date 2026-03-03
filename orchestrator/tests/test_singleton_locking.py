"""Tests for singleton holder thread-safety guards."""
import threading

from orchestrator.app.graph_builder import (
    _ast_pool_lock,
    _container_lock,
    _redis_lock,
    _vector_store_lock,
)


class TestSingletonHolderLocking:
    def test_ast_pool_lock_exists(self) -> None:
        assert isinstance(_ast_pool_lock, type(threading.Lock()))

    def test_container_lock_exists(self) -> None:
        assert isinstance(_container_lock, type(threading.Lock()))

    def test_vector_store_lock_exists(self) -> None:
        assert isinstance(_vector_store_lock, type(threading.Lock()))

    def test_redis_lock_exists(self) -> None:
        assert isinstance(_redis_lock, type(threading.Lock()))
