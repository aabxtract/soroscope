use reqwest::Client;
use serde::Deserialize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

// ── Configuration constants ───────────────────────────────────────────────────

/// Number of consecutive health-check failures before a provider is tripped.
const CIRCUIT_BREAKER_THRESHOLD: u64 = 3;

/// How long a tripped provider is excluded from the pool.
const CIRCUIT_BREAKER_COOLDOWN: Duration = Duration::from_secs(5 * 60); // 5 minutes

/// Timeout for the lightweight `getLatestLedger` health probe.
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(10);

/// EMA smoothing window, measured in samples. α = 2 / (window + 1) so a
/// window of 20 weights the newest sample at ~9.5% — responsive enough
/// to notice a provider slowing down within a few requests, stable
/// enough that a single outlier doesn't dominate routing decisions.
const DEFAULT_EMA_WINDOW: u32 = 20;

/// Minimum samples a provider must have accumulated before its EMA is
/// trusted for latency-based routing. Before every provider clears this
/// threshold the registry falls back to round-robin so new providers
/// are not starved of traffic they need to build up statistics.
pub const MIN_SAMPLES_FOR_EMA: u64 = 10;

// ── Types ─────────────────────────────────────────────────────────────────────

/// A single Soroban RPC endpoint with optional authentication.
#[derive(Debug, Clone, Deserialize)]
pub struct RpcProvider {
    /// Human-readable label (e.g. "stellar-testnet", "blockdaemon-mainnet").
    pub name: String,
    /// Full JSON-RPC URL.
    pub url: String,
    /// Optional authentication header name (e.g. "Authorization", "X-API-Key").
    #[serde(default)]
    pub auth_header: Option<String>,
    /// Optional authentication header value (e.g. "Bearer <token>", "<api-key>").
    #[serde(default)]
    pub auth_value: Option<String>,
}

/// Per-provider latency statistics backed by lock-free atomics.
///
/// All values are microseconds, stored whole (not fixed-point) in an
/// `AtomicU64`. The EMA update is a read-modify-write with
/// `Ordering::Relaxed`: we do not need a total order across threads,
/// only eventual convergence, and the alternative (mutex or CAS loop)
/// would add contention on the request hot path for no measurable win.
///
/// The tiny race this allows — two threads racing to publish an EMA —
/// costs at most one sample of noise on the published value and
/// self-corrects on the next write.
#[derive(Debug)]
pub struct ProviderStats {
    /// EMA of RTT in whole microseconds.
    ema_rtt_us: AtomicU64,
    /// Number of samples recorded since the registry started.
    sample_count: AtomicU64,
    /// α·10⁶ — the EMA smoothing factor scaled so the update loop is
    /// integer-only. α = 2 / (window + 1).
    alpha_millionths: u64,
}

impl ProviderStats {
    /// Build a fresh stats tracker with `ema_window` samples of memory.
    /// Smaller windows react faster to latency changes; larger windows
    /// smooth over transient outliers.
    pub fn new(ema_window: u32) -> Self {
        // Guard against a zero window (would divide by zero / make the
        // EMA always-latest). Clamp to at least 1 sample of history.
        let w = ema_window.max(1) as u64;
        let alpha_millionths = 2_000_000 / (w + 1);
        Self {
            ema_rtt_us: AtomicU64::new(0),
            sample_count: AtomicU64::new(0),
            alpha_millionths,
        }
    }

    /// Record one RTT observation (microseconds) and fold it into the
    /// EMA. The first sample seeds the EMA directly so the series
    /// starts on the true value rather than slowly rising from zero.
    pub fn record(&self, rtt_us: u64) {
        let count = self.sample_count.fetch_add(1, Ordering::Relaxed);
        if count == 0 {
            self.ema_rtt_us.store(rtt_us, Ordering::Relaxed);
            return;
        }
        let prev = self.ema_rtt_us.load(Ordering::Relaxed);
        let alpha = self.alpha_millionths;
        // EMA = α·sample + (1−α)·prev, all scaled by 10⁶ then divided
        // back down at the end.
        let new_ema = (alpha * rtt_us + (1_000_000 - alpha) * prev) / 1_000_000;
        self.ema_rtt_us.store(new_ema, Ordering::Relaxed);
    }

    /// Current EMA in microseconds. Zero means no samples have been
    /// recorded yet.
    pub fn ema_rtt_us(&self) -> u64 {
        self.ema_rtt_us.load(Ordering::Relaxed)
    }

    /// Total samples recorded so far. Used to gate the switch from
    /// warmup (round-robin) to steady-state (least-EMA) routing.
    pub fn sample_count(&self) -> u64 {
        self.sample_count.load(Ordering::Relaxed)
    }

    /// True once the stats have enough samples to drive latency-based
    /// routing decisions. See [`MIN_SAMPLES_FOR_EMA`].
    pub fn is_warmed(&self) -> bool {
        self.sample_count() >= MIN_SAMPLES_FOR_EMA
    }
}

/// Runtime health state for a single provider.
#[derive(Debug)]
struct ProviderState {
    provider: RpcProvider,
    /// Rolling count of consecutive failures (reset on success).
    consecutive_failures: AtomicU64,
    /// When the circuit breaker was tripped (None = healthy).
    tripped_at: RwLock<Option<Instant>>,
    /// Latest ledger number returned by the last successful health check.
    latest_ledger: AtomicU64,
    /// RTT statistics recorded by every successful request against this
    /// provider (see [`ProviderRegistry::record_rtt`]).
    stats: ProviderStats,
}

/// Immutable snapshot of one provider's identity and current RTT
/// statistics. Returned by [`ProviderRegistry::stats_snapshot`] so
/// callers (metrics exporters, `/health`, tests) can inspect latency
/// data without holding any registry lock.
#[derive(Debug, Clone)]
pub struct ProviderStatsSnapshot {
    pub name: String,
    pub url: String,
    pub ema_rtt_us: u64,
    pub sample_count: u64,
}

/// Thread-safe registry that tracks provider health and drives failover.
pub struct ProviderRegistry {
    states: Vec<Arc<ProviderState>>,
    client: Client,
    /// Rotating cursor used by the round-robin fallback path during the
    /// warmup window, before every provider has produced the minimum
    /// number of RTT samples required to trust its EMA.
    round_robin_cursor: AtomicUsize,
}

impl ProviderRegistry {
    /// Build a registry from a prioritized list of providers.
    ///
    /// The order matters: the first provider is preferred when healthy.
    pub fn new(providers: Vec<RpcProvider>) -> Arc<Self> {
        let states = providers
            .into_iter()
            .map(|p| {
                Arc::new(ProviderState {
                    provider: p,
                    consecutive_failures: AtomicU64::new(0),
                    tripped_at: RwLock::new(None),
                    latest_ledger: AtomicU64::new(0),
                    stats: ProviderStats::new(DEFAULT_EMA_WINDOW),
                })
            })
            .collect();

        Arc::new(Self {
            states,
            client: Client::new(),
            round_robin_cursor: AtomicUsize::new(0),
        })
    }

    /// Return the list of providers that are currently available for requests,
    /// in priority order (skipping tripped providers whose cooldown hasn't elapsed).
    pub async fn healthy_providers(&self) -> Vec<&RpcProvider> {
        let mut available = Vec::new();
        for state in &self.states {
            if self.is_available(state).await {
                available.push(&state.provider);
            }
        }
        available
    }

    /// Return the list of healthy providers ordered by preference for the
    /// failover loop, honouring measured latency when available:
    ///
    /// - If **every** healthy provider has reached [`MIN_SAMPLES_FOR_EMA`]
    ///   samples, sort by EMA ascending — the fastest observed provider
    ///   is tried first.
    /// - Otherwise fall back to round-robin (starting index advances by
    ///   one on each call) to give new providers a chance to accumulate
    ///   statistics instead of being starved by warmer neighbours.
    ///
    /// The callers' existing fallback-on-error behaviour is preserved:
    /// this method only changes **ordering**. Even the slowest healthy
    /// provider remains in the returned list so the simulation engine
    /// can still fail over to it if the top pick errors.
    pub async fn providers_by_latency(&self) -> Vec<&RpcProvider> {
        let mut available: Vec<&Arc<ProviderState>> = Vec::new();
        for state in &self.states {
            if self.is_available(state).await {
                available.push(state);
            }
        }
        if available.is_empty() {
            return Vec::new();
        }

        let all_warmed = available.iter().all(|s| s.stats.is_warmed());

        if all_warmed {
            available.sort_by_key(|s| s.stats.ema_rtt_us());
        } else {
            // Round-robin bootstrap. We don't shuffle — we rotate by one
            // position per call so the cursor advances deterministically
            // and every provider gets its turn as the "first" pick.
            let cursor = self.round_robin_cursor.fetch_add(1, Ordering::Relaxed);
            available.rotate_left(cursor % available.len());
        }

        available.into_iter().map(|s| &s.provider).collect()
    }

    /// Record an RTT observation (microseconds) for the provider at
    /// `url`. No-op when the URL is unknown so the caller can record
    /// unconditionally without paying a lookup cost on a miss.
    pub fn record_rtt(&self, url: &str, rtt_us: u64) {
        if let Some(state) = self.find_by_url(url) {
            state.stats.record(rtt_us);
            tracing::debug!(
                provider = %state.provider.name,
                rtt_us,
                ema_rtt_us = state.stats.ema_rtt_us(),
                samples = state.stats.sample_count(),
                "RTT sample recorded"
            );
        }
    }

    /// Snapshot the current RTT statistics for every provider in the
    /// registry. Intended for metrics endpoints, `/health`, and tests —
    /// the returned `Vec` is a value type so callers don't hold any
    /// registry lock across inspection.
    pub fn stats_snapshot(&self) -> Vec<ProviderStatsSnapshot> {
        self.states
            .iter()
            .map(|s| ProviderStatsSnapshot {
                name: s.provider.name.clone(),
                url: s.provider.url.clone(),
                ema_rtt_us: s.stats.ema_rtt_us(),
                sample_count: s.stats.sample_count(),
            })
            .collect()
    }

    /// Report a successful request to `url`. Resets the failure counter and
    /// clears any active trip.
    pub async fn report_success(&self, url: &str) {
        if let Some(state) = self.find_by_url(url) {
            state.consecutive_failures.store(0, Ordering::Relaxed);
            let mut tripped = state.tripped_at.write().await;
            *tripped = None;
        }
    }

    /// Report a failed request to `url`. Increments the failure counter and
    /// trips the circuit breaker when the threshold is reached.
    pub async fn report_failure(&self, url: &str) {
        if let Some(state) = self.find_by_url(url) {
            let prev = state.consecutive_failures.fetch_add(1, Ordering::Relaxed);
            if prev + 1 >= CIRCUIT_BREAKER_THRESHOLD {
                let mut tripped = state.tripped_at.write().await;
                if tripped.is_none() {
                    tracing::warn!(
                        provider = %state.provider.name,
                        url = %state.provider.url,
                        failures = prev + 1,
                        "Circuit breaker TRIPPED — provider excluded for {:?}",
                        CIRCUIT_BREAKER_COOLDOWN
                    );
                }
                *tripped = Some(Instant::now());
            }
        }
    }

    /// Determine whether a request to `url` should be retried on the next
    /// provider. Returns `true` for timeouts, HTTP 429, and 5xx status codes.
    pub fn is_retryable_status(status: u16) -> bool {
        status == 429 || status >= 500
    }

    // ── Background health checker ─────────────────────────────────────────

    /// Spawn a background Tokio task that periodically probes every provider
    /// with `getLatestLedger`.
    pub fn spawn_health_checker(
        self: &Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                registry.run_health_checks().await;
            }
        })
    }

    /// Execute a single round of health checks against all providers.
    async fn run_health_checks(&self) {
        for state in &self.states {
            let result = self.probe_provider(state).await;
            match result {
                Ok(ledger) => {
                    state.latest_ledger.store(ledger, Ordering::Relaxed);
                    state.consecutive_failures.store(0, Ordering::Relaxed);
                    let mut tripped = state.tripped_at.write().await;
                    *tripped = None;
                    tracing::debug!(
                        provider = %state.provider.name,
                        latest_ledger = ledger,
                        "Health check OK"
                    );
                }
                Err(e) => {
                    let prev = state.consecutive_failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        provider = %state.provider.name,
                        consecutive_failures = prev + 1,
                        error = %e,
                        "Health check FAILED"
                    );
                    if prev + 1 >= CIRCUIT_BREAKER_THRESHOLD {
                        let mut tripped = state.tripped_at.write().await;
                        if tripped.is_none() {
                            tracing::warn!(
                                provider = %state.provider.name,
                                "Circuit breaker TRIPPED by health checker"
                            );
                        }
                        *tripped = Some(Instant::now());
                    }
                }
            }
        }
    }

    /// Call `getLatestLedger` on a single provider. Returns the ledger
    /// sequence number on success.
    async fn probe_provider(&self, state: &ProviderState) -> Result<u64, String> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestLedger",
            "params": null
        });

        let mut req = self.client.post(&state.provider.url).json(&body);

        // Attach provider-specific auth header if configured.
        if let (Some(header), Some(value)) =
            (&state.provider.auth_header, &state.provider.auth_value)
        {
            req = req.header(header.as_str(), value.as_str());
        }

        let response = tokio::time::timeout(HEALTH_CHECK_TIMEOUT, req.send())
            .await
            .map_err(|_| "timeout".to_string())?
            .map_err(|e| format!("request error: {e}"))?;

        if !response.status().is_success() {
            return Err(format!("HTTP {}", response.status().as_u16()));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| format!("parse error: {e}"))?;

        json["result"]["sequence"]
            .as_u64()
            .ok_or_else(|| "missing sequence in response".to_string())
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    fn find_by_url(&self, url: &str) -> Option<&Arc<ProviderState>> {
        self.states.iter().find(|s| s.provider.url == url)
    }

    async fn is_available(&self, state: &ProviderState) -> bool {
        let tripped = state.tripped_at.read().await;
        match *tripped {
            None => true,
            Some(when) => when.elapsed() >= CIRCUIT_BREAKER_COOLDOWN,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_provider(name: &str, url: &str) -> RpcProvider {
        RpcProvider {
            name: name.to_string(),
            url: url.to_string(),
            auth_header: None,
            auth_value: None,
        }
    }

    fn make_provider_with_auth(name: &str, url: &str) -> RpcProvider {
        RpcProvider {
            name: name.to_string(),
            url: url.to_string(),
            auth_header: Some("X-API-Key".to_string()),
            auth_value: Some("secret-key-123".to_string()),
        }
    }

    #[tokio::test]
    async fn test_all_providers_healthy_initially() {
        let registry = ProviderRegistry::new(vec![
            make_provider("a", "http://a.test"),
            make_provider("b", "http://b.test"),
        ]);
        let healthy = registry.healthy_providers().await;
        assert_eq!(healthy.len(), 2);
        assert_eq!(healthy[0].url, "http://a.test");
        assert_eq!(healthy[1].url, "http://b.test");
    }

    #[tokio::test]
    async fn test_circuit_breaker_trips_after_threshold() {
        let registry = ProviderRegistry::new(vec![
            make_provider("a", "http://a.test"),
            make_provider("b", "http://b.test"),
        ]);

        // Simulate 3 consecutive failures on provider "a"
        for _ in 0..CIRCUIT_BREAKER_THRESHOLD {
            registry.report_failure("http://a.test").await;
        }

        let healthy = registry.healthy_providers().await;
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].url, "http://b.test");
    }

    #[tokio::test]
    async fn test_success_resets_failure_counter() {
        let registry = ProviderRegistry::new(vec![make_provider("a", "http://a.test")]);

        // Two failures, then a success
        registry.report_failure("http://a.test").await;
        registry.report_failure("http://a.test").await;
        registry.report_success("http://a.test").await;

        // Should still be healthy (counter reset before threshold)
        let healthy = registry.healthy_providers().await;
        assert_eq!(healthy.len(), 1);
    }

    #[tokio::test]
    async fn test_success_clears_tripped_state() {
        let registry = ProviderRegistry::new(vec![make_provider("a", "http://a.test")]);

        // Trip the breaker
        for _ in 0..CIRCUIT_BREAKER_THRESHOLD {
            registry.report_failure("http://a.test").await;
        }
        assert_eq!(registry.healthy_providers().await.len(), 0);

        // Report success (simulating health check recovery)
        registry.report_success("http://a.test").await;
        assert_eq!(registry.healthy_providers().await.len(), 1);
    }

    #[test]
    fn test_is_retryable_status() {
        assert!(ProviderRegistry::is_retryable_status(429));
        assert!(ProviderRegistry::is_retryable_status(500));
        assert!(ProviderRegistry::is_retryable_status(502));
        assert!(ProviderRegistry::is_retryable_status(503));
        assert!(!ProviderRegistry::is_retryable_status(200));
        assert!(!ProviderRegistry::is_retryable_status(400));
        assert!(!ProviderRegistry::is_retryable_status(404));
    }

    #[tokio::test]
    async fn test_report_failure_unknown_url_is_noop() {
        let registry = ProviderRegistry::new(vec![make_provider("a", "http://a.test")]);
        registry.report_failure("http://unknown.test").await;
        assert_eq!(registry.healthy_providers().await.len(), 1);
    }

    #[tokio::test]
    async fn test_provider_with_auth_headers() {
        let provider = make_provider_with_auth("authed", "http://authed.test");
        assert_eq!(provider.auth_header.as_deref(), Some("X-API-Key"));
        assert_eq!(provider.auth_value.as_deref(), Some("secret-key-123"));

        let registry = ProviderRegistry::new(vec![provider]);
        let healthy = registry.healthy_providers().await;
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].auth_header.as_deref(), Some("X-API-Key"));
    }

    #[tokio::test]
    async fn test_priority_order_preserved() {
        let registry = ProviderRegistry::new(vec![
            make_provider("primary", "http://primary.test"),
            make_provider("secondary", "http://secondary.test"),
            make_provider("tertiary", "http://tertiary.test"),
        ]);
        let healthy = registry.healthy_providers().await;
        assert_eq!(healthy[0].name, "primary");
        assert_eq!(healthy[1].name, "secondary");
        assert_eq!(healthy[2].name, "tertiary");
    }

    // ── ProviderStats / latency routing tests ─────────────────────────────

    /// Helper: record `count` identical samples to warm the EMA past the
    /// routing threshold and onto a stable value.
    fn warm_stats(stats: &ProviderStats, rtt_us: u64, count: u64) {
        for _ in 0..count {
            stats.record(rtt_us);
        }
    }

    #[test]
    fn ema_converges_toward_true_value() {
        let stats = ProviderStats::new(20);
        warm_stats(&stats, 1000, 100);
        // With α ≈ 0.095 and 100 identical samples, the EMA sits right
        // on the true value — anything further than 5% off would be a
        // real bug.
        let ema = stats.ema_rtt_us();
        let drift = ema.abs_diff(1000);
        assert!(drift <= 50, "EMA drifted {drift}µs from 1000µs");
        assert_eq!(stats.sample_count(), 100);
    }

    #[test]
    fn ema_first_sample_seeds_exact_value() {
        // The series starts at the first sample instead of climbing from
        // zero — otherwise early routing decisions would penalise brand
        // new providers.
        let stats = ProviderStats::new(20);
        stats.record(777);
        assert_eq!(stats.ema_rtt_us(), 777);
    }

    #[test]
    fn ema_zero_window_is_clamped_safely() {
        // A caller-supplied zero window would have divided by zero; the
        // constructor clamps it to a 1-sample minimum.
        let stats = ProviderStats::new(0);
        stats.record(500);
        stats.record(1000);
        // Just ensure no panic and the EMA moved.
        assert!(stats.ema_rtt_us() > 0);
        assert_eq!(stats.sample_count(), 2);
    }

    #[test]
    fn is_warmed_reflects_sample_count_threshold() {
        let stats = ProviderStats::new(20);
        assert!(!stats.is_warmed());
        for _ in 0..MIN_SAMPLES_FOR_EMA - 1 {
            stats.record(100);
        }
        assert!(!stats.is_warmed(), "{} samples is below threshold", stats.sample_count());
        stats.record(100);
        assert!(stats.is_warmed(), "{} samples should be warmed", stats.sample_count());
    }

    #[tokio::test]
    async fn providers_by_latency_picks_fastest_once_warm() {
        let registry = ProviderRegistry::new(vec![
            make_provider("slow", "http://slow.test"),
            make_provider("fast", "http://fast.test"),
        ]);
        // Warm both with very different latencies.
        for _ in 0..MIN_SAMPLES_FOR_EMA {
            registry.record_rtt("http://slow.test", 500_000);
            registry.record_rtt("http://fast.test", 50_000);
        }
        let ordered = registry.providers_by_latency().await;
        assert_eq!(ordered.len(), 2);
        assert_eq!(ordered[0].name, "fast");
        assert_eq!(ordered[1].name, "slow");
    }

    #[tokio::test]
    async fn providers_by_latency_round_robins_before_warmup() {
        let registry = ProviderRegistry::new(vec![
            make_provider("a", "http://a.test"),
            make_provider("b", "http://b.test"),
        ]);
        // Neither provider has any samples → round-robin fallback.
        let first = registry.providers_by_latency().await;
        let second = registry.providers_by_latency().await;
        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        // The cursor rotates by one each call, so the head differs.
        assert_ne!(first[0].name, second[0].name);
    }

    #[tokio::test]
    async fn providers_by_latency_excludes_unhealthy_providers() {
        let registry = ProviderRegistry::new(vec![
            make_provider("fast-but-down", "http://fast-down.test"),
            make_provider("slow-but-up", "http://slow-up.test"),
        ]);
        // Fast provider has the best EMA but we trip its breaker.
        for _ in 0..MIN_SAMPLES_FOR_EMA {
            registry.record_rtt("http://fast-down.test", 10_000);
            registry.record_rtt("http://slow-up.test", 500_000);
        }
        for _ in 0..CIRCUIT_BREAKER_THRESHOLD {
            registry.report_failure("http://fast-down.test").await;
        }
        let ordered = registry.providers_by_latency().await;
        assert_eq!(ordered.len(), 1);
        assert_eq!(ordered[0].name, "slow-but-up");
    }

    #[tokio::test]
    async fn providers_by_latency_empty_when_all_unhealthy() {
        let registry = ProviderRegistry::new(vec![
            make_provider("a", "http://a.test"),
            make_provider("b", "http://b.test"),
        ]);
        for _ in 0..CIRCUIT_BREAKER_THRESHOLD {
            registry.report_failure("http://a.test").await;
            registry.report_failure("http://b.test").await;
        }
        assert!(registry.providers_by_latency().await.is_empty());
    }

    #[test]
    fn record_rtt_for_unknown_url_is_noop() {
        // No panic, no sample recorded.
        let registry = ProviderRegistry::new(vec![make_provider("a", "http://a.test")]);
        registry.record_rtt("http://unknown.test", 100);
        let snap = registry.stats_snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].sample_count, 0);
    }

    #[test]
    fn stats_snapshot_reflects_recorded_samples() {
        let registry = ProviderRegistry::new(vec![
            make_provider("a", "http://a.test"),
            make_provider("b", "http://b.test"),
        ]);
        for _ in 0..5 {
            registry.record_rtt("http://a.test", 1_000);
        }
        registry.record_rtt("http://b.test", 10_000);

        let snap = registry.stats_snapshot();
        let a = snap.iter().find(|s| s.name == "a").unwrap();
        let b = snap.iter().find(|s| s.name == "b").unwrap();
        assert_eq!(a.sample_count, 5);
        assert_eq!(b.sample_count, 1);
        // EMA for `a` converged to 1_000; `b` seeded at 10_000.
        assert_eq!(a.ema_rtt_us, 1_000);
        assert_eq!(b.ema_rtt_us, 10_000);
    }
}
