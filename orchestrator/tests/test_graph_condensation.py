from __future__ import annotations

from orchestrator.app.graph_embeddings import GraphTopology


class TestMacroNodeModel:

    def test_macro_node_fields(self) -> None:
        from orchestrator.app.macro_node import MacroNode
        node = MacroNode(
            node_id="macro-1",
            community_id="community-0",
            tenant_id="t1",
            member_ids=frozenset({"svc-a", "svc-b", "svc-c"}),
            summary_text="Backend microservices handling auth and payments.",
            member_count=3,
        )
        assert node.node_id == "macro-1"
        assert node.member_count == 3
        assert "svc-a" in node.member_ids

    def test_macro_node_label_property(self) -> None:
        from orchestrator.app.macro_node import MacroNode
        node = MacroNode(
            node_id="macro-1",
            community_id="community-0",
            tenant_id="t1",
            member_ids=frozenset({"svc-a"}),
            summary_text="summary",
            member_count=1,
        )
        assert node.neo4j_label == "MacroNode"


class TestGraphCondensationPipeline:

    def test_condense_returns_macro_nodes(self) -> None:
        from orchestrator.app.graph_condensation import condense_graph
        topology = GraphTopology(
            nodes=["a", "b", "c", "d", "e", "f"],
            adjacency={
                "a": ["b", "c"],
                "b": ["a", "c"],
                "c": ["a", "b"],
                "d": ["e", "f"],
                "e": ["d", "f"],
                "f": ["d", "e"],
            },
        )
        result = condense_graph(
            topology=topology,
            tenant_id="t1",
            min_community_size=2,
        )
        assert len(result.macro_nodes) >= 1
        for mn in result.macro_nodes:
            assert mn.tenant_id == "t1"
            assert mn.member_count >= 2

    def test_small_communities_excluded(self) -> None:
        from orchestrator.app.graph_condensation import condense_graph
        topology = GraphTopology(
            nodes=["a", "b"],
            adjacency={"a": ["b"], "b": ["a"]},
        )
        result = condense_graph(
            topology=topology,
            tenant_id="t1",
            min_community_size=5,
        )
        assert len(result.macro_nodes) == 0

    def test_inter_community_edges_created(self) -> None:
        from orchestrator.app.graph_condensation import condense_graph
        topology = GraphTopology(
            nodes=["a", "b", "c", "d", "e", "f", "bridge"],
            adjacency={
                "a": ["b", "c", "bridge"],
                "b": ["a", "c"],
                "c": ["a", "b"],
                "d": ["e", "f", "bridge"],
                "e": ["d", "f"],
                "f": ["d", "e"],
                "bridge": ["a", "d"],
            },
        )
        result = condense_graph(
            topology=topology,
            tenant_id="t1",
            min_community_size=2,
        )
        assert len(result.macro_nodes) >= 2, (
            "Expected at least 2 macro-nodes from two dense clusters"
        )
        assert len(result.inter_community_edges) >= 1

    def test_condensation_result_has_node_to_macro_map(self) -> None:
        from orchestrator.app.graph_condensation import condense_graph
        topology = GraphTopology(
            nodes=["a", "b", "c"],
            adjacency={"a": ["b", "c"], "b": ["a", "c"], "c": ["a", "b"]},
        )
        result = condense_graph(
            topology=topology,
            tenant_id="t1",
            min_community_size=2,
        )
        assert len(result.macro_nodes) >= 1, (
            "Expected at least 1 macro-node from a 3-node clique"
        )
        assert len(result.node_to_macro) >= 1
        for node_id, macro_id in result.node_to_macro.items():
            assert any(
                mn.node_id == macro_id for mn in result.macro_nodes
            )


class TestSummaryGeneration:

    def test_generate_summary_returns_text(self) -> None:
        from orchestrator.app.community_summary import generate_summary
        member_names = ["auth-service", "user-service", "payment-service"]
        member_types = ["Service", "Service", "Service"]
        summary = generate_summary(
            member_names=member_names,
            member_types=member_types,
        )
        assert isinstance(summary, str)
        assert len(summary) > 0

    def test_generate_summary_includes_member_info(self) -> None:
        from orchestrator.app.community_summary import generate_summary
        member_names = ["auth-service", "payment-service"]
        member_types = ["Service", "Service"]
        summary = generate_summary(
            member_names=member_names,
            member_types=member_types,
        )
        assert "auth-service" in summary or "payment" in summary


class TestTraversalMacroNodeHalting:

    def test_macro_node_detected_in_context(self) -> None:
        from orchestrator.app.context_manager import format_macro_node_context
        from orchestrator.app.macro_node import MacroNode
        macro = MacroNode(
            node_id="macro-1",
            community_id="c-0",
            tenant_id="t1",
            member_ids=frozenset({"svc-a", "svc-b"}),
            summary_text="A group of backend services.",
            member_count=2,
        )
        context_block = format_macro_node_context(macro)
        assert "macro-1" in context_block
        assert "backend services" in context_block.lower() or "summary" in context_block.lower()

    def test_is_macro_node_check(self) -> None:
        from orchestrator.app.macro_node import is_macro_node
        result_with_macro_label = {
            "target_label": "MacroNode",
            "target_id": "macro-1",
        }
        result_without_macro = {
            "target_label": "Service",
            "target_id": "svc-a",
        }
        assert is_macro_node(result_with_macro_label) is True
        assert is_macro_node(result_without_macro) is False
