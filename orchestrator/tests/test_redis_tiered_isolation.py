from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from orchestrator.app.config import RedisCacheConfig, RedisOutboxConfig


class TestRedisCacheConfig:
    def test_reads_dedicated_cache_url(self) -> None:
        env = {"REDIS_CACHE_URL": "redis://cache:6379", "REDIS_URL": "redis://shared:6379"}
        with patch.dict(os.environ, env, clear=True):
            cfg = RedisCacheConfig.from_env()
        assert cfg.url == "redis://cache:6379"

    def test_falls_back_to_redis_url(self) -> None:
        env = {"REDIS_URL": "redis://shared:6379"}
        with patch.dict(os.environ, env, clear=True):
            cfg = RedisCacheConfig.from_env()
        assert cfg.url == "redis://shared:6379"

    def test_empty_when_nothing_set(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            cfg = RedisCacheConfig.from_env()
        assert cfg.url == ""


class TestRedisOutboxConfig:
    def test_reads_dedicated_outbox_url(self) -> None:
        env = {"REDIS_OUTBOX_URL": "redis://outbox:6379", "REDIS_URL": "redis://shared:6379"}
        with patch.dict(os.environ, env, clear=True):
            cfg = RedisOutboxConfig.from_env()
        assert cfg.url == "redis://outbox:6379"

    def test_falls_back_to_redis_url(self) -> None:
        env = {"REDIS_URL": "redis://shared:6379"}
        with patch.dict(os.environ, env, clear=True):
            cfg = RedisOutboxConfig.from_env()
        assert cfg.url == "redis://shared:6379"

    def test_empty_when_nothing_set(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            cfg = RedisOutboxConfig.from_env()
        assert cfg.url == ""


class TestTieredIsolation:
    def test_cache_and_outbox_can_differ(self) -> None:
        env = {
            "REDIS_CACHE_URL": "redis://cache:6379",
            "REDIS_OUTBOX_URL": "redis://outbox:6379",
        }
        with patch.dict(os.environ, env, clear=True):
            cache_cfg = RedisCacheConfig.from_env()
            outbox_cfg = RedisOutboxConfig.from_env()
        assert cache_cfg.url != outbox_cfg.url
        assert cache_cfg.url == "redis://cache:6379"
        assert outbox_cfg.url == "redis://outbox:6379"
