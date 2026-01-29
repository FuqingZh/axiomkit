# axiomkit

Personal, portable engineering toolkit (Python/R/Rust).


## Function prefixes (concise)

- 计算/推断：calculate_（确定性数值）、derive_（派生结构/字段）、infer_（离散标签/属性）、estimate_（近似数值）
- 构建：create_（创建/实例化对象）
- 解析/编解码：parse_（文本/头信息→结构）、decode_（编码→原始）、encode_（原始→编码）
- 验证：validate_（抛异常）、check_（返回 bool）
- 变换/规范化：convert_（类型/格式等价转换；尽量可逆）、normalize_（数值尺度/分布标准化）、sanitize_（文本/字段名/非法字符清洗与标准化）
- 选择：select_（字段/列投影/重排）、filter_（谓词过滤；不用于选列）
- 抽取（可选）：extract_（从嵌套/复合结构抽取子结构；不用于表格投影）
- 规划/应用：plan_（生成方案）、apply_（应用方案到目标）
- IO：scan_（惰性/轻量读取）、read_（读取并解析为对象）、write_（序列化并写出对象）、sink_（可选；流式/管道式写出）
- 生成：generate_（批量/序列）、sample_（随机）
- 流程：prepare_、run_、finalize_
- 呈现：render_、report_

### Naming boundaries

- infer_：返回 bool | Enum | Literal[...]；如需多字段结果，返回命名明确的 Spec*，且字段均为离散属性；不得返回连续数值。
- calculate_ vs derive_：calculate_ 输出数值/统计量；derive_ 输出结构/字段。
- read_ vs write_：read_ 读取并解析为对象；write_ 序列化并写出对象。
- sink_ vs write_：sink_ 仅用于真正流式/管道式写出（不要求全量物化）；否则用 write_。
- validate_：强契约校验（失败抛异常）；允许轻量 IO（如 exists），重 IO 放到 read_/write_。
- scan_：惰性/轻量读取，返回 lazy/iterator/metadata；不得默认物化主体数据（需物化用 read_）。
- select_ vs filter_：select_ 只做投影/重排（列/字段）；filter_ 仅按谓词丢行/元素，不用于选列。
- create_ vs generate_：单个对象→create_；序列/批量→generate_。

## Method verbs (lifecycle & protocol)

> 目标：对象方法只保留协议必需动词；模块函数继续用前缀体系。

- 生命周期：`close()` 作为唯一资源终止/提交动词（若实现 context manager，`__exit__` 必须调用 `close()`）。
- Builder：`build()` 仅允许出现在 `*Builder` 类型作为终止方法；不使用 `build_` 前缀函数。
- 执行：`run()` 作为可运行对象的主执行入口；对外写盘由 `write_`/`sink_` 负责。
- 呈现：`render()` / `report()` 生成展示/报告对象；不隐式落盘。
- 构造：类型构造优先 `__init__`；若需工厂方法，用 `create_*`（函数或 `@classmethod`）。

### 禁止清单（public methods）

- 禁止：`save/load/export/dump`（与 `read/write/sink` 体系冲突）。
- 禁止：`execute/start/stop/finish/shutdown/dispose`（与 `run/close` 冲突）。
- 禁止：`make/process/do/get/show`（泛词，降低信息量；`render` + `write` 代替 `show`）。

### Method 白名单（public）

- `close()`
- `build()`（仅 Builder）
- `run()`
- `render()`
- `report()`
- 以及 Python 必需的：`__init__`, `__enter__`, `__exit__`
