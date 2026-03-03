from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = REPO_ROOT / "docs"
INFRA_DIR = REPO_ROOT / "infrastructure"
MULTI_REGION_DIR = INFRA_DIR / "k8s" / "multi-region"
APP_DIR = REPO_ROOT / "orchestrator" / "app"

REQUIRED_ARCH_SECTIONS = [
    "replication",
    "failover",
    "data routing",
]


class TestMultiRegionArchitectureDoc:

    def test_multi_region_doc_exists(self) -> None:
        path = DOCS_DIR / "multi-region-architecture.md"
        assert path.is_file(), (
            "Expected docs/multi-region-architecture.md to exist"
        )

    def test_doc_covers_replication_strategy(self) -> None:
        content = (DOCS_DIR / "multi-region-architecture.md").read_text(
            encoding="utf-8",
        ).lower()
        assert "replication" in content, (
            "Architecture doc must cover replication strategy"
        )

    def test_doc_covers_failover(self) -> None:
        content = (DOCS_DIR / "multi-region-architecture.md").read_text(
            encoding="utf-8",
        ).lower()
        assert "failover" in content, (
            "Architecture doc must cover failover mechanisms"
        )

    def test_doc_covers_data_routing(self) -> None:
        content = (DOCS_DIR / "multi-region-architecture.md").read_text(
            encoding="utf-8",
        ).lower()
        assert "data routing" in content or "routing" in content, (
            "Architecture doc must cover data routing"
        )

    def test_doc_has_minimum_depth(self) -> None:
        content = (DOCS_DIR / "multi-region-architecture.md").read_text(
            encoding="utf-8",
        )
        assert len(content) >= 500, (
            "Architecture doc should be substantive (>= 500 chars)"
        )


class TestMultiRegionK8sInfrastructure:

    def test_multi_region_directory_exists(self) -> None:
        assert MULTI_REGION_DIR.is_dir(), (
            f"Expected infrastructure/k8s/multi-region/ at {MULTI_REGION_DIR}"
        )

    def test_mirrormaker_config_exists(self) -> None:
        yaml_files = list(MULTI_REGION_DIR.glob("*.yaml"))
        names = [f.stem for f in yaml_files]
        assert any("mirrormaker" in n.lower() for n in names), (
            "Expected a MirrorMaker YAML config in multi-region directory"
        )

    def test_cross_region_service_exists(self) -> None:
        yaml_files = list(MULTI_REGION_DIR.glob("*.yaml"))
        names = [f.stem for f in yaml_files]
        assert any("cross-region" in n.lower() for n in names), (
            "Expected a cross-region service YAML in multi-region directory"
        )

    def test_mirrormaker_is_valid_yaml(self) -> None:
        mm_files = [
            f for f in MULTI_REGION_DIR.glob("*.yaml")
            if "mirrormaker" in f.stem.lower()
        ]
        assert mm_files, "No MirrorMaker YAML found"
        content = yaml.safe_load(mm_files[0].read_text(encoding="utf-8"))
        assert isinstance(content, dict)

    def test_cross_region_service_has_kind(self) -> None:
        cr_files = [
            f for f in MULTI_REGION_DIR.glob("*.yaml")
            if "cross-region" in f.stem.lower()
        ]
        assert cr_files, "No cross-region service YAML found"
        docs = list(yaml.safe_load_all(cr_files[0].read_text(encoding="utf-8")))
        assert docs, "Cross-region YAML is empty"
        assert all("kind" in d for d in docs), (
            "Every document in cross-region manifest must define kind"
        )


class TestDataResidencyConfig:

    def test_data_residency_module_exists(self) -> None:
        path = APP_DIR / "data_residency.py"
        assert path.is_file(), (
            "Expected orchestrator/app/data_residency.py to exist"
        )

    def test_data_residency_config_importable(self) -> None:
        from orchestrator.app.data_residency import DataResidencyConfig
        assert DataResidencyConfig is not None

    def test_config_has_home_region(self) -> None:
        from orchestrator.app.data_residency import DataResidencyConfig
        cfg = DataResidencyConfig(
            tenant_id="t-1",
            home_region="us-east-1",
        )
        assert cfg.home_region == "us-east-1"

    def test_config_has_allowed_regions(self) -> None:
        from orchestrator.app.data_residency import DataResidencyConfig
        cfg = DataResidencyConfig(
            tenant_id="t-1",
            home_region="us-east-1",
            allowed_regions=["us-east-1", "us-west-2"],
        )
        assert "us-west-2" in cfg.allowed_regions

    def test_config_has_consistency_level(self) -> None:
        from orchestrator.app.data_residency import DataResidencyConfig
        cfg = DataResidencyConfig(
            tenant_id="t-1",
            home_region="eu-west-1",
            consistency="strong",
        )
        assert cfg.consistency == "strong"

    def test_config_defaults(self) -> None:
        from orchestrator.app.data_residency import DataResidencyConfig
        cfg = DataResidencyConfig(
            tenant_id="t-2",
            home_region="ap-southeast-1",
        )
        assert cfg.consistency == "eventual"
        assert cfg.allowed_regions == []


class TestTenantRegionRouter:

    def test_router_importable(self) -> None:
        from orchestrator.app.data_residency import TenantRegionRouter
        assert TenantRegionRouter is not None

    def test_router_returns_home_region(self) -> None:
        from orchestrator.app.data_residency import (
            DataResidencyConfig,
            TenantRegionRouter,
        )
        configs = {
            "t-1": DataResidencyConfig(
                tenant_id="t-1",
                home_region="eu-central-1",
            ),
        }
        router = TenantRegionRouter(configs)
        assert router.route("t-1") == "eu-central-1"

    def test_router_raises_for_unknown_tenant(self) -> None:
        from orchestrator.app.data_residency import TenantRegionRouter
        router = TenantRegionRouter({})
        with pytest.raises(KeyError):
            router.route("unknown-tenant")

    def test_router_rejects_request_outside_allowed_regions(self) -> None:
        from orchestrator.app.data_residency import (
            DataResidencyConfig,
            TenantRegionRouter,
        )
        configs = {
            "t-eu": DataResidencyConfig(
                tenant_id="t-eu",
                home_region="eu-west-1",
                allowed_regions=["eu-west-1", "eu-central-1"],
            ),
        }
        router = TenantRegionRouter(configs)
        assert router.is_region_allowed("t-eu", "eu-central-1") is True
        assert router.is_region_allowed("t-eu", "us-east-1") is False

    def test_router_empty_allowed_means_home_only(self) -> None:
        from orchestrator.app.data_residency import (
            DataResidencyConfig,
            TenantRegionRouter,
        )
        configs = {
            "t-locked": DataResidencyConfig(
                tenant_id="t-locked",
                home_region="ap-northeast-1",
            ),
        }
        router = TenantRegionRouter(configs)
        assert router.is_region_allowed("t-locked", "ap-northeast-1") is True
        assert router.is_region_allowed("t-locked", "us-east-1") is False
