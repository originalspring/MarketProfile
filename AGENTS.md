# 协作偏好

## 规划方式
- 以渐进方式讨论和完善计划。
- 除非用户明确要求，否则不要一次性给出完整定稿方案。

## Markdown 文件规则
- 当用户明确开启设计/规划会话并要求本地文档时，在仓库中创建 `.md` 文件。
- 如果用户没有明确要求创建 `.md` 文件，不要新建任何 `.md` 文件。
- 在起草 `.md` 文件时，默认尽量使用中文。

## 本地 Skills
- `yahoo-finance-8day-fetch`: Yahoo Finance 数据抓取统一采用 7 天分段请求；超过 7 天的回看窗口必须分段；周末区间跳过；并使用本地 SQLite 缓存已抓取数据与分段状态，避免重复请求；无数据日仅输出提示，不中断流程。路径：`skills/yahoo-finance-8day-fetch/SKILL.md`
- `quant-numpy-pandas`: 数据分析与推导计算默认优先使用 NumPy + Pandas；尽量向量化与 DataFrame 聚合，不随意手写纯 Python 分析循环。路径：`skills/quant-numpy-pandas/SKILL.md`
