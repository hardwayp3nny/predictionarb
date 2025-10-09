use crate::config::EngineConfig;
use anyhow::{Context, Result};
use flate2::read::{GzDecoder, ZlibDecoder};
use futures::future::join_all;
use prometheus::{HistogramVec, IntCounterVec, IntGaugeVec, Registry};
use reqwest::{
    header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING},
    Client, ClientBuilder,
};
use serde_json::Value;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use url::Url;

#[derive(Clone)]
pub struct HttpPool {
    client: Client,
    base: Url,
    metrics: Arc<HttpMetrics>,
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub json: Option<Value>,
    pub text: Option<String>,
    pub bytes: Vec<u8>,
}

impl HttpPool {
    pub fn new(cfg: &EngineConfig, registry: &Registry) -> Result<Self> {
        let http = &cfg.http;
        let client = ClientBuilder::new()
            .http1_only() // Polymarket CLOB is HTTP/1.1; 可按需放开 http2
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .pool_idle_timeout(None)
            .pool_max_idle_per_host(http.max_connections)
            .gzip(true)
            .brotli(true)
            .deflate(true)
            .connect_timeout(Duration::from_millis(http.timeout_ms))
            .timeout(Duration::from_millis(http.timeout_ms as u64))
            .build()
            .context("build reqwest client")?;
        let base = Url::parse(&http.base_url).context("parse base url")?;
        let metrics = Arc::new(HttpMetrics::new(registry));
        let pool = Self {
            client: client.clone(),
            base: base.clone(),
            metrics: Arc::clone(&metrics),
        };

        pool.spawn_health_monitor(
            http.health_path.clone(),
            http.health_interval_ms,
            http.max_connections,
        );

        Ok(Self {
            client,
            base,
            metrics,
        })
    }

    fn build_url(&self, path: &str) -> Result<Url> {
        if path.starts_with('h') {
            return Url::parse(path).context("parse absolute url");
        }
        self.base.join(path).context("join url")
    }

    fn default_headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/plain, */*"),
        );
        h.insert(
            ACCEPT_ENCODING,
            HeaderValue::from_static("gzip, deflate, br"),
        );
        // 模拟浏览器请求头（更贴近实际服务）
        h.insert("User-Agent", HeaderValue::from_static("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"));
        h.insert(
            "Accept-Language",
            HeaderValue::from_static("en-US,en;q=0.9"),
        );
        // Origin/Referer 需要与目标一致；这里使用 base 域
        if let Some(origin) = self
            .base
            .origin()
            .ascii_serialization()
            .strip_suffix('/')
            .map(|s| s.to_string())
        {
            if let Ok(val) = HeaderValue::from_str(&origin) {
                h.insert("Origin", val);
            }
            if let Ok(val) = HeaderValue::from_str(&(origin + "/")) {
                h.insert("Referer", val);
            }
        }
        h
    }

    fn spawn_health_monitor(&self, path: String, interval_ms: u64, max_connections: usize) {
        if interval_ms == 0 {
            return;
        }

        let resolved = if path.trim().is_empty() {
            self.base.clone()
        } else if path.starts_with('h') {
            match Url::parse(&path) {
                Ok(url) => url,
                Err(err) => {
                    tracing::warn!(target: "http", ?err, path=%path, "invalid health path");
                    return;
                }
            }
        } else {
            match self.base.join(&path) {
                Ok(url) => url,
                Err(err) => {
                    tracing::warn!(
                        target: "http",
                        ?err,
                        base=%self.base,
                        path=%path,
                        "failed to resolve health path"
                    );
                    return;
                }
            }
        };

        let client = self.client.clone();
        let url = Arc::new(resolved);
        tracing::info!(
            target: "http",
            path = %url.as_ref(),
            interval_ms = interval_ms,
            connections = max_connections,
            "starting HTTP health monitor"
        );
        let headers = Arc::new(self.default_headers());
        let interval = Duration::from_millis(interval_ms);
        let label_path = Arc::new(path.clone());

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;

                let futs = (0..max_connections.max(1)).map(|_| {
                    let client = client.clone();
                    let url = Arc::clone(&url);
                    let headers = headers.as_ref().clone();
                    let label_path = Arc::clone(&label_path);
                    async move {
                        let request = client.get(url.as_ref().clone()).headers(headers);
                        match request.send().await {
                            Ok(resp) => {
                                if let Err(err) = resp.bytes().await {
                                    tracing::debug!(
                                        target: "http",
                                        ?err,
                                        path = %label_path.as_ref(),
                                        "health probe read body failed"
                                    );
                                }
                            }
                            Err(err) => {
                                tracing::warn!(
                                    target: "http",
                                    ?err,
                                    path = %label_path.as_ref(),
                                    "health probe request failed"
                                );
                            }
                        }
                    }
                });

                join_all(futs).await;
            }
        });
    }

    pub async fn get(&self, path: &str, headers: Option<HeaderMap>) -> Result<HttpResponse> {
        let url = self.build_url(path)?;
        let mut h = self.default_headers();
        if let Some(extra) = headers {
            h.extend(extra);
        }
        self.do_req("GET", path, self.client.get(url).headers(h))
            .await
    }

    pub async fn delete(
        &self,
        path: &str,
        headers: Option<HeaderMap>,
        body: Option<&Value>,
    ) -> Result<HttpResponse> {
        let url = self.build_url(path)?;
        let mut h = self.default_headers();
        if let Some(extra) = headers {
            h.extend(extra);
        }
        let builder = self.client.delete(url).headers(h);
        self.do_req_body("DELETE", path, builder, body).await
    }

    pub async fn post(
        &self,
        path: &str,
        headers: Option<HeaderMap>,
        body: Option<&Value>,
    ) -> Result<HttpResponse> {
        let url = self.build_url(path)?;
        let mut h = self.default_headers();
        if let Some(extra) = headers {
            h.extend(extra);
        }
        let builder = self.client.post(url).headers(h);
        self.do_req_body("POST", path, builder, body).await
    }

    pub async fn put(
        &self,
        path: &str,
        headers: Option<HeaderMap>,
        body: Option<&Value>,
    ) -> Result<HttpResponse> {
        let url = self.build_url(path)?;
        let mut h = self.default_headers();
        if let Some(extra) = headers {
            h.extend(extra);
        }
        let builder = self.client.put(url).headers(h);
        self.do_req_body("PUT", path, builder, body).await
    }

    async fn do_req(
        &self,
        method: &str,
        path: &str,
        builder: reqwest::RequestBuilder,
    ) -> Result<HttpResponse> {
        let start = Instant::now();
        self.metrics.inflight.with_label_values(&[method]).inc();
        self.metrics
            .req_total
            .with_label_values(&[method, path])
            .inc();
        let resp = builder.send().await.context("http send")?;
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let body_bytes = resp.bytes().await.context("read body bytes")?;
        let dur = start.elapsed().as_secs_f64();
        self.metrics
            .latency
            .with_label_values(&[method, path])
            .observe(dur);
        self.metrics.inflight.with_label_values(&[method]).dec();
        if status >= 400 {
            self.metrics
                .fail_total
                .with_label_values(&[method, path])
                .inc();
        }
        let dur_ms = (dur * 1000.0) as u64;
        tracing::info!(target: "http", method=%method, path=%path, status=%status, latency_ms=%dur_ms, bytes=%body_bytes.len(), "HTTP request completed");
        Ok(Self::parse_body(status, &headers, &body_bytes))
    }

    async fn do_req_body(
        &self,
        method: &str,
        path: &str,
        builder: reqwest::RequestBuilder,
        body: Option<&Value>,
    ) -> Result<HttpResponse> {
        let start = Instant::now();
        self.metrics.inflight.with_label_values(&[method]).inc();
        self.metrics
            .req_total
            .with_label_values(&[method, path])
            .inc();
        let resp = match body {
            Some(v) => builder.json(v).send().await,
            None => builder.send().await,
        }
        .context("http send body")?;
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let body_bytes = resp.bytes().await.context("read body bytes")?;
        let dur = start.elapsed().as_secs_f64();
        self.metrics
            .latency
            .with_label_values(&[method, path])
            .observe(dur);
        self.metrics.inflight.with_label_values(&[method]).dec();
        if status >= 400 {
            self.metrics
                .fail_total
                .with_label_values(&[method, path])
                .inc();
        }
        let dur_ms = (dur * 1000.0) as u64;
        tracing::info!(target: "http", method=%method, path=%path, status=%status, latency_ms=%dur_ms, bytes=%body_bytes.len(), "HTTP request completed");
        Ok(Self::parse_body(status, &headers, &body_bytes))
    }

    fn parse_body(status: u16, headers: &HeaderMap, bytes: &bytes::Bytes) -> HttpResponse {
        let mut json = None;
        let mut text = None;
        let mut raw = bytes.to_vec();

        // 正常情况下 reqwest 已解压；若未解压，做一次兜底
        if json.is_none() {
            if let Ok(v) = serde_json::from_slice::<Value>(&raw) {
                json = Some(v);
            }
        }
        if json.is_none() {
            // 尝试 gzip/deflate 手动解压
            if let Some(enc) = headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok())
            {
                if enc.contains("gzip") {
                    let mut d = GzDecoder::new(raw.as_slice());
                    let mut out = Vec::new();
                    if d.read_to_end(&mut out).is_ok() {
                        if let Ok(v) = serde_json::from_slice::<Value>(&out) {
                            json = Some(v);
                        } else {
                            text = Some(String::from_utf8_lossy(&out).to_string());
                        }
                        return HttpResponse {
                            status,
                            json,
                            text,
                            bytes: out,
                        };
                    }
                } else if enc.contains("deflate") {
                    let mut d = ZlibDecoder::new(raw.as_slice());
                    let mut out = Vec::new();
                    if d.read_to_end(&mut out).is_ok() {
                        if let Ok(v) = serde_json::from_slice::<Value>(&out) {
                            json = Some(v);
                        } else {
                            text = Some(String::from_utf8_lossy(&out).to_string());
                        }
                        return HttpResponse {
                            status,
                            json,
                            text,
                            bytes: out,
                        };
                    }
                }
            }
        }
        if json.is_none() && text.is_none() {
            text = Some(String::from_utf8_lossy(&raw).to_string());
        }
        HttpResponse {
            status,
            json,
            text,
            bytes: raw,
        }
    }
}

#[derive(Clone)]
struct HttpMetrics {
    req_total: IntCounterVec,
    fail_total: IntCounterVec,
    inflight: IntGaugeVec,
    latency: HistogramVec,
}

impl HttpMetrics {
    fn new(registry: &Registry) -> Self {
        let req_total = IntCounterVec::new(
            prometheus::Opts::new("http_requests_total", "HTTP requests total"),
            &["method", "path"],
        )
        .unwrap();
        let fail_total = IntCounterVec::new(
            prometheus::Opts::new("http_failures_total", "HTTP failures total"),
            &["method", "path"],
        )
        .unwrap();
        let inflight = IntGaugeVec::new(
            prometheus::Opts::new("http_inflight", "HTTP inflight requests"),
            &["method"],
        )
        .unwrap();
        let latency = HistogramVec::new(
            prometheus::HistogramOpts::new("http_latency_seconds", "HTTP request latency seconds")
                .buckets(vec![
                    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                ]),
            &["method", "path"],
        )
        .unwrap();
        registry.register(Box::new(req_total.clone())).ok();
        registry.register(Box::new(fail_total.clone())).ok();
        registry.register(Box::new(inflight.clone())).ok();
        registry.register(Box::new(latency.clone())).ok();
        Self {
            req_total,
            fail_total,
            inflight,
            latency,
        }
    }
}
