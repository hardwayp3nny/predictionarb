# Prometheus Metrics

## 访问方式

启动程序后，metrics会在以下地址暴露：

```
http://0.0.0.0:9090/metrics
```

## 查看metrics

### 1. 使用curl

```bash
curl http://localhost:9090/metrics
```

### 2. 使用浏览器

直接访问：http://localhost:9090/metrics

### 3. 配置Prometheus采集

在prometheus.yml中添加：

```yaml
scrape_configs:
  - job_name: 'polymarket-arbitrage'
    static_configs:
      - targets: ['localhost:9090']
```

## 可用的Metrics

### 订单相关
- `orders_sent` - 发送的订单总数
- `orders_succeeded` - 成功的订单数
- `orders_failed` - 失败的订单数
- `inflight_orders` - 当前进行中的订单数

### HTTP请求相关
- `http_requests_total` - HTTP请求总数（按method和path分类）
- `http_failures_total` - HTTP请求失败总数
- `http_inflight` - 当前进行中的HTTP请求数
- `http_latency_seconds` - HTTP请求延迟（直方图，按method和path分类）

### WebSocket相关
- `ws_events` - WebSocket事件总数

## 查询示例

### 平均HTTP延迟（Prometheus PromQL）

```promql
# 平均延迟
rate(http_latency_seconds_sum[5m]) / rate(http_latency_seconds_count[5m])

# POST /orders的P99延迟
histogram_quantile(0.99, rate(http_latency_seconds_bucket{method="POST",path="/orders"}[5m]))

# 订单成功率
orders_succeeded / orders_sent

# 每秒订单数
rate(orders_sent[1m])
```

## 注意事项

1. Metrics server在程序启动时自动在后台启动
2. 端口9090需要确保没有被占用
3. Metrics数据会一直累积，直到程序重启

