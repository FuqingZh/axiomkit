from pathlib import Path

import pytest

from axiomkit.workspace import WorkspaceLayoutSpec, WorkspacePlan


def test_workspace_plan_apply_success(tmp_path: Path) -> None:
    cls_root = tmp_path / "workspace-root"
    cls_plan = WorkspacePlan(dir_root=cls_root)

    assert cls_plan.report_check.ok
    cls_report = cls_plan.apply()
    assert cls_report.ok

    for cls_path in cls_plan.paths.iter_all():
        assert cls_path.exists()
        assert cls_path.is_dir()

    assert cls_plan.paths["out"].exists()
    assert cls_plan.paths["meta"].exists()


def test_workspace_plan_apply_rejects_invalid_spec(tmp_path: Path) -> None:
    cls_plan = WorkspacePlan(
        dir_root=tmp_path,
        spec=WorkspaceLayoutSpec(dirs={"out": "../escape"}),
    )
    assert not cls_plan.report_check.ok
    with pytest.raises(ValueError):
        cls_plan.apply()


def test_workspace_plan_exposes_paths_after_apply(tmp_path: Path) -> None:
    cls_plan = WorkspacePlan(dir_root=tmp_path)
    cls_report = cls_plan.apply()
    assert cls_report.ok
    assert cls_plan.paths.dir_root == tmp_path
    assert cls_plan.paths["out"].exists()
    assert cls_report.paths.dir_root == cls_plan.paths.dir_root
    assert cls_report.paths["out"] == cls_plan.paths["out"]


def test_workspace_plan_supports_sequence_dirs(tmp_path: Path) -> None:
    cls_spec = WorkspaceLayoutSpec(dirs=("canonical", "derived", "meta", "tmp"))
    cls_plan = WorkspacePlan(dir_root=tmp_path / "workspace", spec=cls_spec)
    assert cls_plan.report_check.ok

    cls_report = cls_plan.apply()
    assert cls_report.ok
    assert cls_plan.paths["canonical"].exists()
    assert cls_plan.paths["derived"].exists()


def test_workspace_plan_supports_mapping_dirs(tmp_path: Path) -> None:
    cls_spec = WorkspaceLayoutSpec(
        dirs={
            "canonical": "tables/canonical",
            "derived": "tables/derived",
            "meta": "meta",
            "tmp": "tmp",
        }
    )
    cls_plan = WorkspacePlan(dir_root=tmp_path / "workspace", spec=cls_spec)
    assert cls_plan.report_check.ok

    cls_report = cls_plan.apply()
    assert cls_report.ok
    assert cls_plan.paths["canonical"].exists()
    assert cls_plan.paths["derived"].exists()


def test_workspace_plan_rejects_plain_string_dirs(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="must not be a plain string"):
        WorkspacePlan(dir_root=tmp_path, spec=WorkspaceLayoutSpec(dirs="meta"))
