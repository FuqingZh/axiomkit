# Contributing to axiomkit

This document defines contributor-facing conventions for public API naming, method verbs, and repository workflow.

## Scope

- This file is normative for new public APIs in `py/`, `r/`, and `rs/`.
- Existing APIs can migrate incrementally; do not break external contracts without deprecation.
- Tooling (`ruff`, `pyright`, tests, CI) is the final gate for merge, but naming and architecture review still applies.

## Function Prefixes

### 计算/推断

- `calculate_`: 确定性数值计算
- `derive_`: 派生结构/字段
- `estimate_`: 近似数值
- `infer_`: 离散标签/属性

### 构建

- `create_`: 创建/实例化对象

### 解析/编解码

- `decode_`: 编码 -> 原始
- `encode_`: 原始 -> 编码
- `parse_`: 文本/头信息 -> 结构

### 验证

- `is_`: 事实判断，返回 `bool`
- `should_`: 策略判断，返回 `bool`
- `validate_`: 强校验，失败抛异常

### 变换/规范化

- `convert_`: 类型/格式等价转换，尽量可逆
- `normalize_`: 数值尺度/分布标准化
- `sanitize_`: 文本/字段名/非法字符清洗

### 选择

- `filter_`: 按谓词过滤（不用于选列）
- `select_`: 字段/列投影与重排

### 抽取（可选）

- `extract_`: 从嵌套/复合结构抽取子结构（不用于表格投影）

### 规划/应用

- `plan_`: 生成方案
- `apply_`: 将方案应用到目标

### IO

- `copy_`: 复制/迁移对象或文件系统资源
- `read_`: 读取并解析为对象
- `scan_`: 惰性/轻量读取
- `sink_`: 流式/管道式写出
- `write_`: 序列化并写出对象

### 生成

- `generate_`: 批量/序列生成
- `sample_`: 随机采样

### 流程

- `prepare_`
- `run_`
- `finalize_`

### 呈现

- `render_`
- `report_`

## Naming Boundaries

- `infer_` 返回 `bool | Enum | Literal[...]`。如需多字段结果，返回命名明确的 `Spec*`，字段必须是离散属性，不返回连续数值。
- `calculate_` vs `derive_`: 前者输出数值/统计量，后者输出结构/字段。
- `read_` vs `write_`: 前者读取并解析为对象，后者序列化并写出对象。
- `sink_` vs `write_`: `sink_` 仅用于真正流式写出（无需全量物化），否则用 `write_`。
- `validate_`: 允许轻量 IO（如 exists）；重 IO 放到 `read_`/`write_`。
- `scan_`: 返回 lazy/iterator/metadata，不默认物化主体数据。
- `select_` vs `filter_`: `select_` 只做投影/重排；`filter_` 只做谓词过滤。
- `create_` vs `generate_`: 单对象用 `create_`，序列/批量用 `generate_`。

## Method Verbs (Lifecycle and Protocol)

Object methods should only keep protocol-required verbs. Domain behaviors remain module functions with prefix rules above.

- `close()`: 唯一资源终止/提交动词。实现 context manager 时，`__exit__` 必须调用 `close()`。
- `build()`: 仅允许在 `*Builder` 类型作为终止方法。
- `add_*`: 仅用于可变累积器（errors/warnings/counters），不用于普通业务对象。
- `run()`: 可运行对象的主执行入口。
- `render()` / `report()`: 生成展示对象，不隐式落盘。

### Disallowed Public Method Verbs

- `save/load/export/dump`
- `execute/start/stop/finish/shutdown/dispose`
- `make/process/do/get/show`

### Allowed Public Methods

- `close()`
- `build()` (builder only)
- `run()`
- `render()`
- `report()`
- Python protocol essentials: `__init__`, `__enter__`, `__exit__`

## Workflow

1. Keep API changes minimal and explicit.
2. Add or update tests with each behavior change.
3. Keep docs and examples in sync with public API.
4. Prefer additive evolution; deprecate before removal.
