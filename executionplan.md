# Trading Analysis 执行计划

## 1. 项目目标
- 基于 Market Profile 数据，构建一个交易分析工具。
- 支持多周期分析（例如 90 天、60 天、30 天、20 天、10 天、5 天）。
- 输出可执行的交易信号与清晰的可视化/决策结果，服务交易员实战流程。

## 2. 当前范围（草案）
- 打通 SierraChart CSV -> 本地分析流程（首要）。
- 定义各周期所需的 Market Profile 输入数据（基于 SierraChart 导出字段）。
- 设计多周期聚合的数据处理管道。
- 定义每个周期的核心指标/特征。
- 设计跨周期综合判断逻辑。
- 定义输出形式（先以结构化报告为主，后续扩展看板/信号对象）。
- 定义回测与评估方法（放在首轮试验后）。

## 3. 已确认（第一轮）
- 数据来源（当前阶段）：Yahoo Finance（`ES=F`）数据，用于先跑通流程与推导版 Profile。
- 数据来源（后续阶段）：SierraChart 导出的 CSV（用于更细粒度与正式版本）。
- 首个测试标的：ES 期货（Yahoo 对应 `ES=F`）。
- 工作方式：先做单品种、单数据源试验，跑通后再扩展。
- 项目目标优先级：先验证“可导入、可计算、可解释”的分析闭环。
- 数据分析实现偏好：优先使用 `NumPy + Pandas`（向量化/聚合）。

## 4. 待确认问题（收敛版）
- 你准备先导出哪几类 CSV（仅 TPO 汇总，还是含分时/逐笔明细）？
- 时间范围先做多久（建议先 20~30 个交易日样本）？
- 一个交易日是否区分 RTH/ETH（是否分 session）？
- 首轮输出你更想先看哪种结果：
  - A. 纯指标表（POC/VAH/VAL/IB/Range 等）
  - B. 指标 + 简单方向判断
  - C. 指标 + 交易触发条件草案

## 5. 固定窗口分析规则（已落地）
- 窗口类型：`full-weeks`，按“最近完整 N 周”处理。
- 周定义：每周 `Mon-Fri`，按周聚合，不使用“执行时点往回倒推天数”的半周窗口。
- 当前默认验证窗口：最近完整 `3` 周。
- 分段抓取：按 7 天分段，分段间 sleep，减少限流风险。
- 当前粒度：`30m`（yfinance 日内数据），用于推导 TPO/Volume Profile 近似指标。
- 当前 session 规则：全时段聚合（尚未做 RTH/ETH 分离）。

## 6. 程序已实现逻辑（当前代码状态）
- 抓取方式：使用 `yfinance` Python 库（不再手写 HTTP 请求）。
- 分段策略：超过 7 天窗口按 7 天分段抓取，分段间 sleep（当前默认 2 秒）。
- 重试策略：默认单次请求流程，不做激进重试，避免被数据源限流。
- 缓存策略：使用 SQLite 缓存分段抓取状态与行情数据，避免重复抓取。
- 数据粒度：当前推导版使用日内 `30m` 数据进行 Profile 推导（标的默认 `ES=F`）。
- 分析实现：核心计算优先使用 `NumPy + Pandas`（向量化/聚合）而非手写循环。
- 已落库数据表（SQLite）：
  - `intraday_bars`：日内行情缓存（OHLCV + 时间戳）。
  - `fetched_chunks_v2`：分段抓取状态。
  - `profile_analysis`：推导后的 Profile 指标结果。
- 当前已落库的推导指标（核心）：
  - `TPO POC/VAH/VAL`、`Volume POC/VAH/VAL`
  - `IB High/Low/Width`
  - `Rotation Factor`
  - `Average Subperiod Range`
  - 区间高低、区间范围、总量、TPO 上下分布等。
- 输出约束：不再生成 CSV/JSON 文件，分析结果仅入 SQLite。
- 周级入库（新增）：
  - `weekly_profile_analysis`：每周一条聚合结果（Mon-Fri）。
  - `weekly_profile_comparison`：每周一条相对前一周的对比摘要（如 `poc_change/vah_change/val_change/direction_bias`）。
- 当前限制：未做 RTH/ETH 分离，当前为全时段聚合推导。

## 7. Watchlist（已落库）
- 表名：`watchlist_stocks`（SQLite）。
- 当前规模：`60` 个标的。
- 分类与范围：
  - `magnificent7`：AAPL/MSFT/NVDA/AMZN/GOOGL/META/TSLA
  - `sp500_large`：BRK-B/AVGO/LLY/JPM/XOM/UNH/V/MA/COST/WMT 等
  - `quantum`：IONQ/RGTI/QBTS/QUBT/ARQQ/QTUM
  - `etf_*`：常用 ETF（含 VOO/VTI/QQQ/TQQQ/VXUS 等）
  - `crypto`：BTC-USD/ETH-USD/SOL-USD/DOGE-USD 等
  - `custom`：TTAN（ServiceTitan）

## 8. 最近三周结果（ES=F，30m，Mon-Fri）
- 窗口：`2026-02-09` 到 `2026-02-27`（完整三周）。
- 周条目（`weekly_profile_analysis`）：
  - 2026-02-09 ~ 2026-02-13
  - 2026-02-16 ~ 2026-02-20
  - 2026-02-23 ~ 2026-02-27
- 周对比摘要（`weekly_profile_comparison`）：
  - Week2 vs Week1：`down_shift`
  - Week3 vs Week2：`up_shift`

## 9. 下一步
- 对齐业务需求与技术约束。
- 确认是否对 watchlist 全量批跑最近完整三周。
- 增加 RTH/ETH 可选 session 分析。
- 冻结 V1 范围并拆解为里程碑。
