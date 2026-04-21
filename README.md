# Portfolio Collector MVP

这是一个适合部署在 VPS 上的资产汇总骨架，目标是把以下来源统一进 PostgreSQL，并直接给 Grafana 使用：

- Moralis: EVM 地址、Solana 地址
- Binance: 主账户、Funding、可选子账户
- OKX: Trading、Funding、可选子账户

当前版本是一个可运行的 MVP，重点放在三层数据：

- `raw_ingestions`: 每次 API 原始返回，便于排错和以后补算
- `positions`: 标准化后的资产持仓明细
- `source_summaries`: 账户级汇总值，例如 wallet valuation、账户总权益

## 目录

- `docker-compose.yml`: 启动 PostgreSQL、Grafana、Collector
- `sql/init/001_schema.sql`: 初始化数据库结构
- `collector/`: Python 采集服务
- `grafana/provisioning/`: Grafana 数据源自动配置
- `docs/architecture.md`: 中文架构说明

## 快速启动

1. 复制环境变量文件并填写：

```bash
cp .env.example .env
```

2. 启动服务：

```bash
docker compose up -d --build
```

3. 访问 Grafana：

- 地址: `http://<your-vps-ip>:3000`
- 用户名: `.env` 里的 `GRAFANA_ADMIN_USER`
- 密码: `.env` 里的 `GRAFANA_ADMIN_PASSWORD`

4. 手动查看 collector 日志：

```bash
docker compose logs -f collector
```

## 推荐 Grafana 查询

最新一次采集的总资产按来源汇总：

```sql
select
  source,
  account_type,
  sum(metric_value) as total_usd
from source_summaries
where snapshot_run_id = (select max(id) from snapshot_runs where status in ('success', 'partial_success'))
  and metric_unit = 'USD'
group by 1, 2
order by total_usd desc;
```

最新一次采集的链上 token 明细：

```sql
select
  source,
  chain,
  account_label,
  asset_symbol,
  amount,
  price_usd,
  usd_value
from positions
where snapshot_run_id = (select max(id) from snapshot_runs where status in ('success', 'partial_success'))
  and source = 'moralis'
  and position_kind in ('token', 'native')
order by usd_value desc nulls last, amount desc nulls last;
```

## 当前取舍

- Binance 现阶段优先抓 `Spot / Funding / 子账户 Spot / 子账户 Futures`，主账户 Futures 只先保留账户级 summary，不做更深的持仓拆解。
- OKX 现阶段优先抓 `Trading / Funding / 子账户 Trading / 子账户 Funding`，并用官方 `asset-valuation` 做账户汇总。
- Solana 资产估值单独补价，不假设 portfolio 接口一定返回稳定的 USD 字段。

## 后续扩展

- 增加资产映射表，把 `CEX asset code / EVM token address / Sol mint` 统一到内部资产主键
- 增加告警任务，例如稳定币占比、单平台敞口、日变动阈值
- 增加物化视图或 TimescaleDB 做时序加速

