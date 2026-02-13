from pathlib import Path

import pytest

from axiomkit.workspace import SpecWorkspaceLayout, WorkspacePlan


def test_workspace_plan_apply_success(tmp_path: Path) -> None:
    cls_root = tmp_path / "workspace-root"
    cls_plan = WorkspacePlan(dir_root=cls_root)

    assert cls_plan.report_check.ok
    cls_report = cls_plan.apply()
    assert cls_report.ok

    for cls_path in cls_plan.paths.iter_all():
        assert cls_path.exists()
        assert cls_path.is_dir()


def test_workspace_plan_apply_rejects_invalid_spec(tmp_path: Path) -> None:
    cls_plan = WorkspacePlan(
        dir_root=tmp_path,
        spec=SpecWorkspaceLayout(name_dir_out="../escape"),
    )
    assert not cls_plan.report_check.ok
    with pytest.raises(ValueError):
        cls_plan.apply()


def test_workspace_plan_exposes_paths_after_apply(tmp_path: Path) -> None:
    cls_plan = WorkspacePlan(dir_root=tmp_path)
    cls_report = cls_plan.apply()
    assert cls_report.ok
    assert cls_plan.paths.dir_root == tmp_path
    assert cls_plan.paths.dir_out.exists()
    assert cls_report.paths.dir_root == cls_plan.paths.dir_root
    assert cls_report.paths.dir_out == cls_plan.paths.dir_out


def test_workspace_plan_supports_extra_dirs(tmp_path: Path) -> None:
    cls_spec = SpecWorkspaceLayout(
        name_dir_in="input",
        name_dir_out="output",
        extra_dirs={"cache": "cache", "artifacts": "artifacts/v1"},
    )
    cls_plan = WorkspacePlan(dir_root=tmp_path / "workspace", spec=cls_spec)
    assert cls_plan.report_check.ok

    cls_report = cls_plan.apply()
    assert cls_report.ok
    assert cls_plan.paths.dirs_extra["cache"].exists()
    assert cls_plan.paths.dirs_extra["artifacts"].exists()
