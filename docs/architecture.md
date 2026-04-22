# 资产看板方案说明

## 1. 目标

这套系统解决的是“多链 + 多交易所”的资产归集问题，不直接做交易，也不处理资金划转。它的职责只有两件事：

- 定时采集资产状态
- 用统一结构存储，方便后续展示、分析和告警

适合你的场景：

- 链上同时有 EVM 地址和 Sol 地址
- CEX 使用 Binance 和 OKX
- 全部通过只读 API 获取
- 部署在一台 VPS 上，追求简单、稳定、易扩展

## 2. 架构分层

### 2.1 采集层

采集器按 source 拆分：

- `DeBankCollector`
- `MoralisCollector`
- `BinanceCollector`
- `OKXCollector`

每个 collector 只负责两件事：

- 调官方接口
- 把结果转换成统一结构

### 2.2 标准化层

标准化后统一写入四类表：

- `snapshot_runs`: 一次完整采集批次
- `raw_ingestions`: 每次 API 原始返回
- `positions`: 持仓级明细
- `prices`: 价格快照
- `source_summaries`: 来源级汇总值

这样设计的好处是：

- 原始数据保留，后面修口径不用回溯 API
- 不同 source 的字段差异不会污染核心分析表
- 可以同时兼顾“逐仓位分析”和“按账户总览”

### 2.3 展示层

Grafana 直接连 PostgreSQL，建议先做 4 个面板：

- 总资产趋势
- 按来源占比
- 按账户类型占比
- 最新一次大额持仓明细

## 3. 为什么不只用一张快照表

如果只用一张 `asset_snapshots`，短期可以跑起来，但很快会遇到这些问题：

- 同名资产无法区分链和场所，例如 `USDT`
- CEX 有账户级估值，但未必总能拆到资产级
- DeFi 头寸和普通 token balance 不是一种结构
- 原始接口变化后，不容易排查是采集错了还是归一化错了

所以 MVP 也建议一开始就至少拆成：

- 原始层
- 明细层
- 汇总层

## 4. 数据口径

### 4.1 链上

EVM：

- 一个 EVM 地址配置可自动展开成多条链采集，适合同地址体系下的 `ETH / Base / Arbitrum / Optimism / Polygon / BSC`
- 总资产、Token 明细、复杂 DeFi 仓位优先取 DeBank
- 这样对 Pendle、LP、收益凭证类资产的估值更稳
- Moralis 不再作为 EVM 主估值来源

Solana：

- token balance 从 portfolio 读取
- 价格通过 Solana price API 单独补
- 原生 SOL 使用包装 SOL 的 mint 做价格代理

### 4.2 Binance

Binance 资产拆两类看：

- 资产级明细：`Spot / Funding / 子账户 Spot / 子账户 Futures`
- 账户级估值：`wallet balance` summary

也就是说，主账户 Futures 先保留 summary，不强行伪造资产拆分。

### 4.3 OKX

OKX 资产拆三层：

- Trading account 明细
- Funding account 明细
- `asset-valuation` 作为总览汇总

子账户同理。

## 5. 部署建议

这版 MVP 默认用 Docker Compose：

- `postgres`: 存储
- `grafana`: 展示
- `collector`: Python 定时采集

调度方式用单进程循环，环境变量控制：

- `COLLECTOR_INTERVAL_SECONDS`
- `COLLECTOR_RUN_ONCE`

这样比额外加一个 cron 容器更直接，也更容易看日志。

## 6. 后续建议的下一步

如果这个 MVP 跑通，下一阶段建议优先做下面三件事：

1. 加 `asset_registry` 映射表，解决多 source 资产统一问题。
2. 加 `portfolio_views` 物化视图，减少 Grafana 查询复杂度。
3. 加日报和异常波动告警，例如 Telegram 或飞书。
