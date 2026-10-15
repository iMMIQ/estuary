use super::*;

#[test]
fn final_outcomes_are_independent_of_logging_and_http_success() {
    let hub = Observability::new();
    for (upstream, status, delivered, streaming) in [
        (Some(Outcome::Success), 200, true, true),
        (Some(Outcome::Failed), 200, true, true),
        (None, 200, false, true),
        (None, 200, true, true),
        (None, 400, true, false),
    ] {
        let observation = hub.begin();
        observation.model("m");
        observation.assign("node", 1);
        observation.headers(status, streaming);
        if let Some(outcome) = upstream {
            observation.upstream(
                outcome,
                crate::inference_stats::Usage {
                    output_tokens: Some(2),
                    ..Default::default()
                },
                None,
                1,
            );
        }
        let mut delivery = Delivery::new(observation);
        delivery.complete = delivered;
        drop(delivery);
    }
    let snapshot = hub.snapshot("model", "m", 300);
    assert_eq!(snapshot.fleet.total.started, 5);
    assert_eq!(snapshot.fleet.total.success, 1);
    assert_eq!(snapshot.fleet.total.failed, 2);
    assert_eq!(snapshot.fleet.total.cancelled, 1);
    assert_eq!(snapshot.fleet.total.unknown, 1);
    assert_eq!(snapshot.selected.unwrap().total.started, 5);
    assert_eq!(snapshot.fleet.latency["duration"].samples, 5);
    assert_eq!(snapshot.fleet.usage_coverage, Some(0.4));
}

#[test]
fn cancelled_before_headers_and_retried_requests_count_once() {
    let hub = Observability::new();
    drop(Delivery::new(hub.begin()));
    let observation = hub.begin();
    observation.model("m");
    observation.assign("old", 1);
    observation.retry();
    observation.assign("new", 2);
    observation.headers(200, false);
    let mut delivery = Delivery::new(observation);
    delivery.complete = true;
    drop(delivery);
    let snapshot = hub.snapshot("fleet", "", 300);
    assert_eq!(snapshot.fleet.total.started, 2);
    assert_eq!(snapshot.fleet.total.completed(), 2);
    assert_eq!(snapshot.fleet.total.retries, 1);
    assert_eq!(
        snapshot
            .nodes
            .iter()
            .find(|node| node.id == "old")
            .unwrap()
            .total
            .failed,
        1
    );
    assert_eq!(snapshot.models[0].total.completed(), 1);
}

#[test]
fn histories_and_dimensions_remain_bounded() {
    let hub = Observability::new();
    for index in 0..200 {
        let observation = hub.begin();
        observation.model(&format!("model-{index}"));
        observation.headers(200, false);
        let mut delivery = Delivery::new(observation);
        delivery.complete = true;
        drop(delivery);
    }
    let mut state = hub.state.lock();
    for tick in 1..1200 {
        state
            .fleet
            .sample(f64::from(tick), u64::try_from(tick).unwrap() * 1000);
    }
    assert!(state.fleet.history.len() <= 180);
    drop(state);
    let snapshot = hub.snapshot("fleet", "", 300);
    assert_eq!(snapshot.models.len(), MAX_SCOPES_PER_KIND);
    assert!(snapshot.limited_events > 0);
    assert_eq!(snapshot.fleet.total.success, 200);
    assert!(snapshot.fleet.cache_hit_ratio.is_none());
}

#[test]
fn merged_percentiles_weight_observations_and_metrics_escape_labels() {
    let hub = Observability::new();
    for index in 0..100 {
        hub.record(
            Some("model\"\n\\"),
            None,
            true,
            Counts::default(),
            [Some(if index == 99 { 10.0 } else { 0.1 }); 7],
        );
    }
    let snapshot = hub.snapshot("fleet", "", 300);
    assert!(snapshot.fleet.latency["ttft"].p95.unwrap() < 0.2);
    let encoded = hub.encode_metrics("# EOF\n".into());
    assert!(encoded.contains("model\\\"\\n\\\\"));
    assert_eq!(encoded.matches("# EOF").count(), 1);
    assert!(encoded.contains("estuary_gateway_ttft_seconds_count{scope=\"fleet\",name=\"\"} 100"));
}

#[test]
fn node_replacement_ignores_old_completions_but_keeps_fleet_results() {
    let hub = Observability::new();
    let old = hub.begin();
    old.assign("node", 1);
    let new = hub.begin();
    new.assign("node", 2);
    for observation in [new, old] {
        observation.headers(200, false);
        let mut delivery = Delivery::new(observation);
        delivery.complete = true;
    }
    let snapshot = hub.snapshot("node", "node", 300);
    assert_eq!(snapshot.fleet.total.success, 2);
    let node = snapshot.selected.unwrap();
    assert_eq!(node.instance, Some(2));
    assert_eq!(node.total.started, 1);
    assert_eq!(node.total.success, 1);
    assert_eq!(node.pressure.active, 0);
}

#[test]
fn embeddings_do_not_lower_output_coverage_and_cache_denominators_are_paired() {
    let hub = Observability::new();
    for (generating, usage) in [
        (
            false,
            crate::inference_stats::Usage {
                input_tokens: Some(900),
                ..Default::default()
            },
        ),
        (
            true,
            crate::inference_stats::Usage {
                input_tokens: Some(100),
                output_tokens: Some(10),
                cached_tokens: Some(75),
                ..Default::default()
            },
        ),
        (
            true,
            crate::inference_stats::Usage {
                input_tokens: Some(900),
                output_tokens: Some(10),
                ..Default::default()
            },
        ),
    ] {
        let observation = hub.begin();
        if !generating {
            observation.non_generating();
        }
        observation.headers(200, false);
        observation.upstream(Outcome::Success, usage, None, 1);
        let mut delivery = Delivery::new(observation);
        delivery.complete = true;
    }
    let scope = hub.snapshot("fleet", "", 300).fleet;
    assert_eq!(scope.usage_coverage, Some(1.0));
    assert_eq!(scope.window.generation_requests, 2);
    assert_eq!(scope.cache_hit_ratio, Some(0.75));
}

#[test]
fn short_requests_preserve_peak_inflight_and_missing_usage_stays_unknown() {
    let hub = Observability::new();
    let deliveries = (0..4)
        .map(|_| Delivery::new(hub.begin()))
        .collect::<Vec<_>>();
    drop(deliveries);
    let mut state = hub.state.lock();
    state.fleet.sample(1.0, 1000);
    assert_eq!(state.fleet.pressure.active, 0);
    assert_eq!(state.fleet.history.back().unwrap().peaks.active, 4);
    assert_eq!(state.fleet.rates.output.raw, None);
    state.fleet.sample(2.0, 2000);
    assert_eq!(state.fleet.history.back().unwrap().peaks.active, 4);
}

#[test]
fn first_burst_uses_the_known_zero_baseline_for_new_scopes() {
    let mut fleet = Series::new(0.0);
    fleet.sample(0.1, 100);
    let mut model = Series::new(0.1);
    let burst = Counts {
        started: 100,
        success: 100,
        ..Counts::default()
    };
    fleet.record(0, burst, [None; 7]);
    model.record(0, burst, [None; 7]);
    fleet.sample(1.0, 1000);
    model.sample(1.0, 1000);
    assert_eq!(
        fleet.rates.completed.smoothed,
        model.rates.completed.smoothed
    );
    assert!(model.rates.completed.smoothed.unwrap() < model.rates.completed.raw.unwrap() / 10.0);
}
