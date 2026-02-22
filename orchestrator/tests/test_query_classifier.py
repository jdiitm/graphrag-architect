from orchestrator.app.query_classifier import classify_query
from orchestrator.app.query_models import QueryComplexity


class TestEntityLookupClassification:
    def test_named_entity_question(self):
        result = classify_query("What language is the auth-service written in?")
        assert result == QueryComplexity.ENTITY_LOOKUP

    def test_simple_property_question(self):
        result = classify_query("What framework does order-service use?")
        assert result == QueryComplexity.ENTITY_LOOKUP

    def test_ambiguous_defaults_to_entity(self):
        result = classify_query("Tell me about the payment-service")
        assert result == QueryComplexity.ENTITY_LOOKUP


class TestSingleHopClassification:
    def test_produces_keyword(self):
        result = classify_query("What topics does order-service produce to?")
        assert result == QueryComplexity.SINGLE_HOP

    def test_consumes_keyword(self):
        result = classify_query("Which services consume from the events topic?")
        assert result == QueryComplexity.SINGLE_HOP

    def test_calls_keyword(self):
        result = classify_query("What services does auth-service call?")
        assert result == QueryComplexity.SINGLE_HOP

    def test_deployed_in_keyword(self):
        result = classify_query("Where is user-service deployed in?")
        assert result == QueryComplexity.SINGLE_HOP


class TestMultiHopClassification:
    def test_blast_radius(self):
        result = classify_query(
            "If auth-service fails, what is the full downstream blast radius?"
        )
        assert result == QueryComplexity.MULTI_HOP

    def test_downstream_keyword(self):
        result = classify_query("What are the downstream dependencies of order-service?")
        assert result == QueryComplexity.MULTI_HOP

    def test_dependency_chain(self):
        result = classify_query("Show me the dependency chain from gateway to database")
        assert result == QueryComplexity.MULTI_HOP

    def test_cascade_failure(self):
        result = classify_query("What would cascade if kafka broker goes down?")
        assert result == QueryComplexity.MULTI_HOP

    def test_impact_keyword(self):
        result = classify_query("What is the impact of disabling the auth-service?")
        assert result == QueryComplexity.MULTI_HOP


class TestAggregateClassification:
    def test_most_critical(self):
        result = classify_query(
            "What are the most critical services by transitive dependency count?"
        )
        assert result == QueryComplexity.AGGREGATE

    def test_top_n(self):
        result = classify_query("What are the top 5 most connected services?")
        assert result == QueryComplexity.AGGREGATE

    def test_count_keyword(self):
        result = classify_query("How many services depend on the auth-service?")
        assert result == QueryComplexity.AGGREGATE

    def test_ranking(self):
        result = classify_query("Rank services by number of downstream consumers")
        assert result == QueryComplexity.AGGREGATE


class TestCaseInsensitivity:
    def test_uppercase_keywords(self):
        result = classify_query("What is the BLAST RADIUS of gateway failure?")
        assert result == QueryComplexity.MULTI_HOP

    def test_mixed_case(self):
        result = classify_query("What are the Most Critical services?")
        assert result == QueryComplexity.AGGREGATE
