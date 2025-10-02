Polyrust - Polymarket HFT Engine (Rust skeleton)

目录结构
- engine: 核心库，包含
  - model: 基本数据模型（订单、行情、用户事件、状态）
  - ports: 端口/接口（ExchangeClient、MarketStream、UserStream、OrderStore）
  - oms: 订单状态管理（内存态，后续可扩展持久化）
  - runner: 引擎主循环，聚合行情/用户流并管理订单全生命周期
  - metrics: Prometheus 指标定义
  - config: 引擎配置（HTTP/WS/线程数）
- app: 二进制入口，默认使用 Mock 适配器

下一步建议
- 实现基于 reqwest 的 ExchangeClient（POST /orders, DELETE /orders 等）
- 实现基于 tokio-tungstenite 的行情/用户 WebSocket 适配器，以及多连接拆分
- 增加批量下单、撤单、订单同步接口；完善 OMS 延迟统计
- 暴露 Prometheus 指标与健康检查 HTTP 端口

## 运行套利策略

仓库现在提供了一个 `arbitrage` 启动器，用于加载配置并驱动 `ArbitrageStrategy`：

```bash
RUST_LOG=info cargo run --bin arbitrage            # 默认读取根目录 config.json
RUST_LOG=info cargo run --bin arbitrage -- my.cfg  # 指定自定义配置文件
```

运行前请确保 `config.json`（或你指定的文件）内填好私钥、API Key 以及订阅的 token 列表。
