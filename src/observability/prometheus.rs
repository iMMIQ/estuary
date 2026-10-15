use std::fmt::Write;

use super::{Observability, Rate, Series, window};

const COUNTERS: [&str; 19] = [
    "started",
    "success",
    "failed",
    "cancelled",
    "unknown",
    "retries",
    "retried_requests",
    "generation_requests",
    "input_tokens",
    "output_tokens",
    "cached_tokens",
    "input_samples",
    "output_samples",
    "cache_samples",
    "tpot_reported",
    "tpot_estimated",
    "route_exact",
    "route_approximate",
    "route_none",
];

pub(super) fn encode(hub: &Observability, mut body: String) -> String {
    if body.ends_with("# EOF\n") {
        body.truncate(body.len() - 6);
    }
    let state = hub.state.lock();
    write_preamble(&mut body);
    let scopes = std::iter::once(("fleet", "", &state.fleet))
        .chain(
            state
                .nodes
                .iter()
                .map(|(id, series)| ("node", id.as_str(), series)),
        )
        .chain(
            state
                .models
                .iter()
                .map(|(id, series)| ("model", id.as_str(), series)),
        );
    for (kind, id, series) in scopes {
        let id = id
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n");
        let labels = format!("scope=\"{kind}\",name=\"{id}\"");
        let counts = series.total;
        let values = [
            counts.started,
            counts.success,
            counts.failed,
            counts.cancelled,
            counts.unknown,
            counts.retries,
            counts.retried_requests,
            counts.generation_requests,
            counts.input_tokens,
            counts.output_tokens,
            counts.cached_tokens,
            counts.input_samples,
            counts.output_samples,
            counts.cache_samples,
            counts.tpot_reported,
            counts.tpot_estimated,
            counts.route_exact,
            counts.route_approximate,
            counts.route_none,
        ];
        for (name, value) in COUNTERS.into_iter().zip(values) {
            let _ = writeln!(body, "estuary_gateway_{name}_total{{{labels}}} {value}");
        }
        write_pressure(&mut body, &labels, kind, series);
        for (name, rate) in [
            ("incoming_requests", series.rates.incoming),
            ("completed_requests", series.rates.completed),
            ("input_tokens", series.rates.input),
            ("output_tokens", series.rates.output),
        ] {
            write_rate(
                &mut body,
                &format!("estuary_gateway_{name}_per_second"),
                &labels,
                rate,
            );
        }
        for (name, rate) in [
            ("input_tokens", series.engine.input),
            ("output_tokens", series.engine.output),
            ("completed_requests", series.engine.requests),
        ] {
            write_rate(
                &mut body,
                &format!("estuary_engine_{name}_per_second"),
                &labels,
                rate,
            );
        }
        for (name, value) in [
            ("cache_hit_ratio", series.engine.cache_hit_ratio),
            (
                "preemptions_per_minute",
                series.engine.preemptions_per_minute,
            ),
            (
                "reporting_nodes",
                Some(series.engine.reporting_nodes as f64),
            ),
            ("expected_nodes", Some(series.engine.expected_nodes as f64)),
        ] {
            if let Some(value) = value {
                let _ = writeln!(body, "estuary_engine_{name}{{{labels}}} {value}");
            }
        }
        write_histograms(&mut body, &labels, series);
    }
    body.push_str("# EOF\n");
    body
}

const PRESSURE: [&str; 13] = [
    "active_requests",
    "upstream_active_requests",
    "concurrency_limit",
    "queued_requests",
    "admission_waiters",
    "buffer_waiters",
    "buffer_reserved_bytes",
    "buffer_limit_bytes",
    "engine_running_requests",
    "engine_waiting_requests",
    "engine_kv_usage_ratio",
    "fresh_provider_nodes",
    "expected_provider_nodes",
];

fn write_pressure(body: &mut String, labels: &str, kind: &str, series: &Series) {
    let pressure = &series.pressure;
    let active = series
        .total
        .started
        .saturating_sub(series.total.completed());
    let _ = writeln!(body, "estuary_active_requests{{{labels}}} {active}");
    if kind == "model" {
        return;
    }
    let gauges = [
        Some(pressure.upstream_active as f64),
        Some(pressure.capacity as f64),
        (kind == "fleet").then_some(pressure.queued as f64),
        (kind == "fleet").then_some(pressure.admission_waiters as f64),
        (kind == "fleet").then_some(pressure.buffer_waiters as f64),
        (kind == "fleet").then_some(pressure.buffer_bytes as f64),
        (kind == "fleet").then_some(pressure.buffer_limit as f64),
        pressure.running.map(|v| v as f64),
        pressure.waiting.map(|v| v as f64),
        pressure.kv_usage,
        Some(pressure.fresh_nodes as f64),
        Some(pressure.provider_nodes as f64),
    ];
    for (name, value) in PRESSURE.into_iter().skip(1).zip(gauges) {
        if let Some(value) = value {
            let _ = writeln!(body, "estuary_{name}{{{labels}}} {value}");
        }
    }
}

fn write_rate(body: &mut String, name: &str, labels: &str, rate: Rate) {
    for (stat, value) in [
        ("raw", rate.raw),
        ("smoothed", rate.smoothed),
        ("mean_60s", rate.mean_60s),
    ] {
        if let Some(value) = value {
            let _ = writeln!(body, "{name}{{{labels},stat=\"{stat}\"}} {value}");
        }
    }
}
fn histogram_name(name: &str) -> String {
    format!(
        "estuary_gateway_{name}{}",
        if name == "generation_rate" {
            "_tokens_per_second"
        } else {
            "_seconds"
        }
    )
}
fn write_histograms(body: &mut String, labels: &str, series: &Series) {
    for (name, histogram) in window::LATENCIES.into_iter().zip(&series.lifetime) {
        let metric = histogram_name(name);
        let mut cumulative = 0;
        for (index, count) in histogram.bins.iter().enumerate() {
            cumulative += count;
            let le = if index == histogram.bins.len() - 1 {
                "+Inf".to_owned()
            } else {
                window::bound(index).to_string()
            };
            let _ = writeln!(body, "{metric}_bucket{{{labels},le=\"{le}\"}} {cumulative}");
        }
        let _ = writeln!(
            body,
            "{metric}_sum{{{labels}}} {}\n{metric}_count{{{labels}}} {}",
            histogram.sum, histogram.count
        );
    }
}

fn write_preamble(body: &mut String) {
    for name in PRESSURE {
        let _ = writeln!(body, "# TYPE estuary_{name} gauge");
    }
    for name in COUNTERS {
        let _ = writeln!(body, "# TYPE estuary_gateway_{name} counter");
    }
    for name in window::LATENCIES {
        let _ = writeln!(body, "# TYPE {} histogram", histogram_name(name));
    }
    for name in [
        "incoming_requests",
        "completed_requests",
        "input_tokens",
        "output_tokens",
    ] {
        let _ = writeln!(body, "# TYPE estuary_gateway_{name}_per_second gauge");
    }
    for name in ["input_tokens", "output_tokens", "completed_requests"] {
        let _ = writeln!(body, "# TYPE estuary_engine_{name}_per_second gauge");
    }
    for name in [
        "cache_hit_ratio",
        "preemptions_per_minute",
        "reporting_nodes",
        "expected_nodes",
    ] {
        let _ = writeln!(body, "# TYPE estuary_engine_{name} gauge");
    }
}
