# axiomkit

Personal, portable engineering toolkit (Python/R/Rust).

## Function prefixes (concise)

- **计算/推断**
    - calculate\_（确定性数值）
    - derive\_（派生结构/字段）
    - estimate\_（近似数值）
    - infer\_（离散标签/属性）
- **构建**
    - create\_（创建/实例化对象）
- **解析/编解码**
    - decode\_（编码→原始）
    - encode\_（原始→编码）
    - parse\_（文本/头信息→结构）
- **验证**
    - is\_（事实判断，返回 bool）
    - should\_（策略/规则判断，返回 bool）
    - validate\_（抛异常）
- **变换/规范化**
    - convert\_（类型/格式等价转换；尽量可逆）
    - normalize\_（数值尺度/分布标准化）
    - sanitize\_（文本/字段名/非法字符清洗与标准化）
- **选择**
    - filter*（谓词过滤；不用于选列）
    - select*（字段/列投影/重排）
- **抽取（可选）**
    - extract\_（从嵌套/复合结构抽取子结构；不用于表格投影）
- **规划/应用**
    - apply*（应用方案到目标）
    - plan*（生成方案）
- **IO**
    - copy\_（复制/迁移对象或文件系统资源）
    - read*（读取并解析为对象）
    - scan*（惰性/轻量读取）
    - sink*（可选；流式/管道式写出）
    - write*（序列化并写出对象）
- **生成**
    - generate*（批量/序列）
    - sample*（随机）
- **流程**
    - finalize\_
    - prepare*
    - run*
- **呈现**
    - render*
    - report*

### Naming boundaries

- **infer\_**: 返回 bool | Enum | Literal[...]；如需多字段结果，返回命名明确的 Spec\*，且字段均为离散属性；不得返回连续数值。
- **calculate* vs derive***: calculate* 输出数值/统计量；derive* 输出结构/字段。
- **read* vs write***: read* 读取并解析为对象；write* 序列化并写出对象。
- **sink* vs write***: sink* 仅用于真正流式/管道式写出（不要求全量物化）；否则用 write*。
- **validate***: 强契约校验（失败抛异常）；允许轻量 IO（如 exists），重 IO 放到 read*/write\_。
- **scan***: 惰性/轻量读取，返回 lazy/iterator/metadata；不得默认物化主体数据（需物化用 read*）。
- **select* vs filter***: select* 只做投影/重排（列/字段）；filter* 仅按谓词丢行/元素，不用于选列。
- **create* vs generate***: 单个对象→create*；序列/批量→generate*。

## Method verbs (lifecycle & protocol)

> 目标：对象方法只保留协议必需动词；模块函数继续用前缀体系。

- **生命周期**: `close()` 作为唯一资源终止/提交动词（若实现 context manager，`__exit__` 必须调用 `close()`）。
- **Builder**: `build()` 仅允许出现在 `*Builder` 类型作为终止方法；不使用 `build_` 前缀函数。
- **Accumulator/Collector**: 允许 `add_*` 作为可变累积器的追加/计数入口（如 errors/warnings/counters），不用于普通业务对象。
- **执行**: `run()` 作为可运行对象的主执行入口；对外写盘由 `write_`/`sink_` 负责。
- **呈现**: `render()` / `report()` 生成展示/报告对象；不隐式落盘。
- **构造**: 类型构造优先 `__init__`；若需工厂方法，用 `create_*`（函数或 `@classmethod`）。

### 禁止清单（public methods）

- `save/load/export/dump`（与 `read/write/sink` 体系冲突）
- `execute/start/stop/finish/shutdown/dispose`（与 `run/close` 冲突）
- `make/process/do/get/show`（泛词，降低信息量；`render` + `write` 代替 `show`）

### Method 白名单（public）

- `close()`
- `build()`（仅 Builder）
- `run()`
- `render()`
- `report()`
- 以及 Python 必需的：`__init__`, `__enter__`, `__exit__`
