import pathlib

import yaml
import pytest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
ROLLOUT = REPO_ROOT / "infrastructure" / "k8s" / "orchestrator-rollout.yaml"
ANALYSIS_TEMPLATE = REPO_ROOT / "infrastructure" / "k8s" / "analysis-template.yaml"
INGRESS = REPO_ROOT / "infrastructure" / "k8s" / "ingress.yaml"

EXPECTED_CANARY_WEIGHTS = [10, 50, 100]


def _load_yaml_docs(path: pathlib.Path) -> list[dict]:
    text = path.read_text()
    return [doc for doc in yaml.safe_load_all(text) if doc is not None]


class TestArgoRolloutExists:
    def test_rollout_file_exists(self):
        assert ROLLOUT.exists(), "orchestrator-rollout.yaml must exist"

    def test_rollout_kind_is_rollout(self):
        docs = _load_yaml_docs(ROLLOUT)
        rollout = next(
            (d for d in docs if d.get("kind") == "Rollout"),
            None,
        )
        assert rollout is not None, "Rollout resource must be present"

    def test_rollout_targets_orchestrator(self):
        docs = _load_yaml_docs(ROLLOUT)
        rollout = next(d for d in docs if d.get("kind") == "Rollout")
        labels = rollout["spec"]["selector"]["matchLabels"]
        assert labels.get("app") == "orchestrator"


class TestCanarySteps:
    def test_canary_strategy_exists(self):
        docs = _load_yaml_docs(ROLLOUT)
        rollout = next(d for d in docs if d.get("kind") == "Rollout")
        assert "canary" in rollout["spec"]["strategy"]

    def test_canary_has_correct_weight_steps(self):
        docs = _load_yaml_docs(ROLLOUT)
        rollout = next(d for d in docs if d.get("kind") == "Rollout")
        steps = rollout["spec"]["strategy"]["canary"]["steps"]
        weights = [
            s["setWeight"] for s in steps if "setWeight" in s
        ]
        assert weights == EXPECTED_CANARY_WEIGHTS

    def test_canary_includes_pause_between_steps(self):
        docs = _load_yaml_docs(ROLLOUT)
        rollout = next(d for d in docs if d.get("kind") == "Rollout")
        steps = rollout["spec"]["strategy"]["canary"]["steps"]
        pause_steps = [s for s in steps if "pause" in s]
        assert len(pause_steps) >= 2, "Must have pauses between weight steps"

    def test_canary_has_analysis_step(self):
        docs = _load_yaml_docs(ROLLOUT)
        rollout = next(d for d in docs if d.get("kind") == "Rollout")
        steps = rollout["spec"]["strategy"]["canary"]["steps"]
        analysis_steps = [s for s in steps if "analysis" in s]
        assert len(analysis_steps) >= 1, "Must include at least one analysis step"


class TestAnalysisTemplate:
    def test_analysis_template_file_exists(self):
        assert ANALYSIS_TEMPLATE.exists(), "analysis-template.yaml must exist"

    def test_analysis_template_kind(self):
        docs = _load_yaml_docs(ANALYSIS_TEMPLATE)
        tmpl = next(
            (d for d in docs if d.get("kind") == "AnalysisTemplate"),
            None,
        )
        assert tmpl is not None, "AnalysisTemplate resource must be present"

    def test_analysis_template_has_success_rate_metric(self):
        docs = _load_yaml_docs(ANALYSIS_TEMPLATE)
        tmpl = next(d for d in docs if d.get("kind") == "AnalysisTemplate")
        metrics = tmpl["spec"]["metrics"]
        metric_names = [m["name"] for m in metrics]
        assert "success-rate" in metric_names

    def test_success_rate_uses_prometheus_provider(self):
        docs = _load_yaml_docs(ANALYSIS_TEMPLATE)
        tmpl = next(d for d in docs if d.get("kind") == "AnalysisTemplate")
        metrics = tmpl["spec"]["metrics"]
        sr = next(m for m in metrics if m["name"] == "success-rate")
        assert "prometheus" in sr["provider"]


class TestWAFAnnotations:
    def test_ingress_has_modsecurity_enabled(self):
        docs = _load_yaml_docs(INGRESS)
        ingress = next(d for d in docs if d.get("kind") == "Ingress")
        annotations = ingress["metadata"]["annotations"]
        assert annotations.get(
            "nginx.ingress.kubernetes.io/enable-modsecurity"
        ) == "true"

    def test_ingress_has_owasp_core_ruleset(self):
        docs = _load_yaml_docs(INGRESS)
        ingress = next(d for d in docs if d.get("kind") == "Ingress")
        annotations = ingress["metadata"]["annotations"]
        assert annotations.get(
            "nginx.ingress.kubernetes.io/enable-owasp-modsecurity-crs"
        ) == "true"

    def test_ingress_has_modsecurity_snippet(self):
        docs = _load_yaml_docs(INGRESS)
        ingress = next(d for d in docs if d.get("kind") == "Ingress")
        annotations = ingress["metadata"]["annotations"]
        snippet = annotations.get(
            "nginx.ingress.kubernetes.io/modsecurity-snippet"
        )
        assert snippet is not None, "ModSecurity snippet must be configured"
        assert "SecRuleEngine On" in snippet
