# Schema 文档集索引（v1.1）

- 更新日期：`2026-03-04`
- 当前有效版本：`v1.1`

## 1. 文档集评估结论

现有 `v1.0` 文档总体框架可用，但存在以下问题：

1. 配置真源仍写为 `YAML`，与当前工程实践不一致（现统一为 `TOML`）。
2. 目录布局与 `WorkspacePlan` 现行结构不一致（应包含 `meta/` 目录，而非 `out/meta.json`）。
3. 文件命名规范未明确统一前缀和分隔语义（`data- / info- / graph-` 等）。
4. `identifer` 目录拼写错误，已新增 `identifier` 作为规范路径。
5. 个别文档绑定生信/蛋白组学语义，泛化不足。

## 2. v1.1 生效文档

1. `data_architecture_file_format_specification/20260304-v1.1.md`
2. `pipeline_directory_layout_specification/20260304-v1.1.md`
3. `step_to_step_artifact_flow_specification/20260304-v1.1.md`
4. `identifier/20260304-v1.1.md`

## 3. 版本管理规则

1. 旧版保留，不覆盖；新版递增版本号并更新时间。
2. 文件命名：`YYYYMMDD-vX.Y.md`。
3. 目录命名建议无空格、无拼写错误；历史目录保留兼容，新规范优先用新目录。
