from __future__ import annotations

from unittest.mock import patch

import pytest

from orchestrator.app.query_models import QueryComplexity
from orchestrator.app.token_bucket import (
    QueryCostModel,
    TenantCostBudget,
    create_cost_budget,
)


class TestQueryCostModelDefaults:
    def test_default_entity_lookup_cost(self) -> None:
        model = QueryCostModel()
        assert model.entity_lookup == 1

    def test_default_single_hop_cost(self) -> None:
        model = QueryCostModel()
        assert model.single_hop == 3

    def test_default_multi_hop_cost(self) -> None:
        model = QueryCostModel()
        assert model.multi_hop == 10

    def test_default_aggregate_cost(self) -> None:
        model = QueryCostModel()
        assert model.aggregate == 8


class TestQueryCostModelCostFor:
    def test_cost_for_entity_lookup(self) -> None:
        model = QueryCostModel()
        assert model.cost_for(QueryComplexity.ENTITY_LOOKUP) == 1

    def test_cost_for_single_hop(self) -> None:
        model = QueryCostModel()
        assert model.cost_for(QueryComplexity.SINGLE_HOP) == 3

    def test_cost_for_multi_hop(self) -> None:
        model = QueryCostModel()
        assert model.cost_for(QueryComplexity.MULTI_HOP) == 10

    def test_cost_for_aggregate(self) -> None:
        model = QueryCostModel()
        assert model.cost_for(QueryComplexity.AGGREGATE) == 8

    def test_cost_for_custom_values(self) -> None:
        model = QueryCostModel(entity_lookup=2, single_hop=5, multi_hop=20, aggregate=15)
        assert model.cost_for(QueryComplexity.ENTITY_LOOKUP) == 2
        assert model.cost_for(QueryComplexity.SINGLE_HOP) == 5
        assert model.cost_for(QueryComplexity.MULTI_HOP) == 20
        assert model.cost_for(QueryComplexity.AGGREGATE) == 15


class TestQueryCostModelFromEnv:
    def test_from_env_reads_all_vars(self) -> None:
        env = {
            "QUERY_COST_ENTITY_LOOKUP": "4",
            "QUERY_COST_SINGLE_HOP": "6",
            "QUERY_COST_MULTI_HOP": "25",
            "QUERY_COST_AGGREGATE": "12",
        }
        with patch.dict("os.environ", env, clear=False):
            model = QueryCostModel.from_env()
        assert model.entity_lookup == 4
        assert model.single_hop == 6
        assert model.multi_hop == 25
        assert model.aggregate == 12

    def test_from_env_uses_defaults_when_unset(self) -> None:
        with patch.dict("os.environ", {}, clear=False):
            for key in ("QUERY_COST_ENTITY_LOOKUP", "QUERY_COST_SINGLE_HOP",
                        "QUERY_COST_MULTI_HOP", "QUERY_COST_AGGREGATE"):
                import os
                os.environ.pop(key, None)
            model = QueryCostModel.from_env()
        assert model.entity_lookup == 1
        assert model.single_hop == 3
        assert model.multi_hop == 10
        assert model.aggregate == 8


class TestTenantCostBudgetCapacity:
    @pytest.mark.asyncio
    async def test_entity_lookups_exhaust_at_capacity(self) -> None:
        budget = TenantCostBudget(capacity=100, window_seconds=60.0)
        for _ in range(100):
            assert await budget.try_acquire("t1", QueryComplexity.ENTITY_LOOKUP) is True
        assert await budget.try_acquire("t1", QueryComplexity.ENTITY_LOOKUP) is False

    @pytest.mark.asyncio
    async def test_multi_hop_exhausts_faster(self) -> None:
        budget = TenantCostBudget(capacity=100, window_seconds=60.0)
        for _ in range(10):
            assert await budget.try_acquire("t1", QueryComplexity.MULTI_HOP) is True
        assert await budget.try_acquire("t1", QueryComplexity.MULTI_HOP) is False

    @pytest.mark.asyncio
    async def test_budget_exhausted_returns_false(self) -> None:
        budget = TenantCostBudget(capacity=10, window_seconds=60.0)
        assert await budget.try_acquire("t1", QueryComplexity.MULTI_HOP) is True
        assert await budget.try_acquire("t1", QueryComplexity.ENTITY_LOOKUP) is False


class TestTenantCostBudgetIsolation:
    @pytest.mark.asyncio
    async def test_independent_budgets_per_tenant(self) -> None:
        budget = TenantCostBudget(capacity=10, window_seconds=60.0)
        assert await budget.try_acquire("t1", QueryComplexity.MULTI_HOP) is True
        assert await budget.try_acquire("t1", QueryComplexity.MULTI_HOP) is False
        assert await budget.try_acquire("t2", QueryComplexity.MULTI_HOP) is True


class TestTenantCostBudgetSlidingWindow:
    @pytest.mark.asyncio
    async def test_budget_refills_after_window_expires(self) -> None:
        budget = TenantCostBudget(capacity=10, window_seconds=0.1)
        clock = [1000.0]
        with patch("time.monotonic", side_effect=lambda: clock[0]):
            assert await budget.try_acquire("t1", QueryComplexity.MULTI_HOP) is True
            assert await budget.try_acquire("t1", QueryComplexity.MULTI_HOP) is False
            clock[0] += 0.2
            assert await budget.try_acquire("t1", QueryComplexity.MULTI_HOP) is True


class TestTenantCostBudgetMixedComplexity:
    @pytest.mark.asyncio
    async def test_mixed_complexity_costs_accumulate(self) -> None:
        budget = TenantCostBudget(capacity=15, window_seconds=60.0)
        assert await budget.try_acquire("t1", QueryComplexity.MULTI_HOP) is True
        assert await budget.try_acquire("t1", QueryComplexity.SINGLE_HOP) is True
        assert await budget.try_acquire("t1", QueryComplexity.ENTITY_LOOKUP) is True
        assert await budget.try_acquire("t1", QueryComplexity.ENTITY_LOOKUP) is True
        assert await budget.try_acquire("t1", QueryComplexity.SINGLE_HOP) is False


class TestCreateCostBudget:
    def test_returns_local_budget_without_redis(self) -> None:
        with patch.dict("os.environ", {"REDIS_URL": ""}, clear=False):
            result = create_cost_budget()
        assert isinstance(result, TenantCostBudget)

    def test_returns_budget_with_custom_capacity(self) -> None:
        with patch.dict("os.environ", {"REDIS_URL": ""}, clear=False):
            result = create_cost_budget(capacity=200)
        assert isinstance(result, TenantCostBudget)
