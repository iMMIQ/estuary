use std::{
    collections::{HashMap, HashSet},
    env, fs,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use axum::{Json, Router, body::Bytes, http::StatusCode, routing::post};
use estuary::{
    Gateway, Settings,
    config::{
        AnthropicProtocol, NodeConfig, PrefixConfig, ProviderConfig, ProviderKind, RoutingConfig,
    },
    kv_cache::{BlockHash, CacheMutation},
    node::Node,
    prefix::routing_text,
    scheduler::Scheduler,
    vllm::VllmManager,
};
use futures_util::{StreamExt, stream};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};

const PROMPT_SIZES: [usize; 3] = [2 * 1024, 128 * 1024, 1024 * 1024];
const NODE_COUNTS: [usize; 3] = [1, 8, 32];

#[derive(Clone, Copy)]
enum Protocol {
    OpenAi,
    Anthropic,
    Codex,
}

impl Protocol {
    const ALL: [Self; 3] = [Self::OpenAi, Self::Anthropic, Self::Codex];

    fn name(self) -> &'static str {
        match self {
            Self::OpenAi => "openai_chat",
            Self::Anthropic => "anthropic_messages",
            Self::Codex => "codex_responses",
        }
    }

    fn endpoint(self) -> &'static str {
        match self {
            Self::OpenAi => "/v1/chat/completions",
            Self::Anthropic => "/v1/messages",
            Self::Codex => "/v1/responses",
        }
    }

    fn request(self, prompt: &str) -> Value {
        match self {
            Self::OpenAi => json!({
                "model": "bench",
                "stream": false,
                "messages": [{"role": "user", "content": prompt}],
            }),
            Self::Anthropic => json!({
                "model": "bench",
                "max_tokens": 16,
                "stream": false,
                "messages": [{"role": "user", "content": prompt}],
            }),
            Self::Codex => json!({
                "model": "bench",
                "stream": false,
                "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
            }),
        }
    }
}

struct Server {
    address: SocketAddr,
    task: JoinHandle<()>,
}

impl Server {
    async fn spawn(router: Router) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("benchmark server failed");
        });
        Ok(Self { address, task })
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.address)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn chat_response(_: Bytes) -> &'static str {
    r#"{"id":"chatcmpl_bench","object":"chat.completion","model":"bench","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}"#
}

async fn responses_response(_: Bytes) -> &'static str {
    r#"{"id":"resp_bench","object":"response","status":"completed","model":"bench","output":[{"id":"msg_bench","type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1}}"#
}

async fn tokenize_response(_: Bytes) -> Json<Value> {
    Json(json!({"tokens": [1, 2, 3, 4]}))
}

async fn mock_vllm() -> Result<Server> {
    Server::spawn(
        Router::new()
            .route("/v1/chat/completions", post(chat_response))
            .route("/v1/messages", post(chat_response))
            .route("/v1/responses", post(responses_response))
            .route("/tokenize", post(tokenize_response)),
    )
    .await
}

async fn gateway(upstream: &Server, node_count: usize) -> Result<Server> {
    let nodes = (0..node_count)
        .map(|index| NodeConfig {
            id: format!("node-{index:02}"),
            base_url: format!("http://{}/v1", upstream.address),
            models: HashMap::from([("bench".to_owned(), "bench".to_owned())]),
            max_concurrency: 1,
            provider: estuary::config::ProviderConfig {
                anthropic_protocol: AnthropicProtocol::Chat,
                ..estuary::config::ProviderConfig::default()
            },
            ..NodeConfig::default()
        })
        .collect();
    let mut settings = Settings {
        nodes,
        ..Settings::default()
    };
    settings.health.route_while_starting = true;
    settings.routing.queue_max_requests = 4_096;
    settings.routing.queue_max_bytes = 2 * 1024 * 1024 * 1024;
    settings.server.max_request_body_bytes = 2 * 1024 * 1024;
    let router = Gateway::build(settings)?.public_router();
    Server::spawn(router).await
}

#[tokio::main]
async fn main() -> Result<()> {
    if env::var("ESTUARY_RUN_BENCHMARK").as_deref() != Ok("1") {
        println!("performance benchmark skipped; set ESTUARY_RUN_BENCHMARK=1 to execute it");
        return Ok(());
    }
    let requests = env::var("ESTUARY_BENCH_REQUESTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100usize);
    let upstream = mock_vllm().await?;
    let client = Client::builder().no_proxy().build()?;

    println!("end-to-end mock-vLLM benchmark ({requests} requests per scenario)");
    println!("protocol\tprompt\tnodes\tthroughput_rps\tp50_ms\tp99_ms\trss_kib");
    for nodes in NODE_COUNTS {
        let gateway = gateway(&upstream, nodes).await?;
        for protocol in Protocol::ALL {
            for prompt_size in PROMPT_SIZES {
                let prompt = "x".repeat(prompt_size);
                let payload = Arc::new(protocol.request(&prompt));
                let concurrency = (nodes * 4).min(requests).max(1);
                let started = Instant::now();
                let mut latencies = stream::iter(0..requests)
                    .map(|_| {
                        let client = client.clone();
                        let url = gateway.url(protocol.endpoint());
                        let payload = Arc::clone(&payload);
                        async move {
                            let request_started = Instant::now();
                            let mut request = client.post(url).json(payload.as_ref());
                            if matches!(protocol, Protocol::Codex) {
                                request = request.header("user-agent", "codex-cli/bench");
                            }
                            let response = request.send().await?;
                            if response.status() != StatusCode::OK {
                                anyhow::bail!("gateway returned {}", response.status());
                            }
                            response.bytes().await?;
                            Ok::<_, anyhow::Error>(request_started.elapsed())
                        }
                    })
                    .buffer_unordered(concurrency)
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                let elapsed = started.elapsed();
                latencies.sort_unstable();
                println!(
                    "{}\t{}\t{}\t{:.1}\t{:.3}\t{:.3}\t{}",
                    protocol.name(),
                    prompt_size,
                    nodes,
                    requests as f64 / elapsed.as_secs_f64(),
                    percentile(&latencies, 50).as_secs_f64() * 1_000.0,
                    percentile(&latencies, 99).as_secs_f64() * 1_000.0,
                    rss_kib().unwrap_or_default(),
                );
            }
        }
    }

    benchmark_prefix_preprocessing(requests);
    benchmark_scheduler(requests).await?;
    benchmark_tokenization(&upstream, &client, requests).await?;
    Ok(())
}

async fn benchmark_tokenization(
    upstream: &Server,
    client: &Client,
    iterations: usize,
) -> Result<()> {
    let config = NodeConfig {
        id: "tokenize-node".to_owned(),
        base_url: format!("http://{}/v1", upstream.address),
        models: HashMap::from([("bench".to_owned(), "bench".to_owned())]),
        provider: ProviderConfig {
            kind: ProviderKind::Vllm,
            ..ProviderConfig::default()
        },
        ..NodeConfig::default()
    };
    let node = Node::from_config(&config)?;
    node.record_vllm_ready("0.25.0".to_owned());
    let scheduler = Arc::new(Scheduler::new(
        vec![Arc::clone(&node)],
        RoutingConfig::default(),
    ));
    scheduler.exact_cache_directory().configure_node_owned(
        node.id(),
        16,
        1024 * 1024,
        node.instance_id(),
    );
    scheduler.exact_cache_directory().apply_owned(
        node.id(),
        node.instance_id(),
        vec![CacheMutation::Store {
            hashes: vec![BlockHash::Integer(1)],
            parent: None,
            token_ids: vec![1, 2, 3, 4],
            block_size: 4,
            group: 0,
        }],
    )?;
    let manager = VllmManager::new(scheduler);

    let remote_started = Instant::now();
    for index in 0..iterations {
        let body = json!({
            "messages": [{"role": "user", "content": format!("tokenize-{index}")}]
        });
        let result = manager
            .tokenize_for_routing(client, "chat/completions", "bench", &body, true)
            .await;
        anyhow::ensure!(result.tokens.is_some(), "remote tokenization was skipped");
    }
    let remote_elapsed = remote_started.elapsed();

    let cached_body = json!({"messages": [{"role": "user", "content": "cached"}]});
    manager
        .tokenize_for_routing(client, "chat/completions", "bench", &cached_body, true)
        .await;
    let cached_started = Instant::now();
    for _ in 0..iterations {
        let result = manager
            .tokenize_for_routing(client, "chat/completions", "bench", &cached_body, true)
            .await;
        anyhow::ensure!(result.outcome == "cache_hit", "tokenization cache missed");
    }
    println!("\ntokenization");
    println!("mode\titerations\tmean_us");
    println!(
        "remote\t{}\t{:.3}",
        iterations,
        remote_elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64,
    );
    println!(
        "cache_hit\t{}\t{:.3}",
        iterations,
        cached_started.elapsed().as_secs_f64() * 1_000_000.0 / iterations as f64,
    );
    Ok(())
}

fn benchmark_prefix_preprocessing(iterations: usize) {
    println!("\nprefix preprocessing");
    println!("protocol\tprompt\titerations\tmean_us");
    let config = PrefixConfig {
        max_request_chars: 2 * 1024 * 1024,
        ..PrefixConfig::default()
    };
    for protocol in Protocol::ALL {
        for prompt_size in PROMPT_SIZES {
            let body = protocol.request(&"x".repeat(prompt_size));
            let endpoint = protocol.endpoint().trim_start_matches("/v1/");
            let started = Instant::now();
            for _ in 0..iterations {
                std::hint::black_box(routing_text(endpoint, Some("bench"), Some(&body), &config));
            }
            println!(
                "{}\t{}\t{}\t{:.3}",
                protocol.name(),
                prompt_size,
                iterations,
                started.elapsed().as_secs_f64() * 1_000_000.0 / iterations as f64,
            );
        }
    }
}

async fn benchmark_scheduler(iterations: usize) -> Result<()> {
    println!("\nscheduler prefix modes");
    println!("mode\tnodes\titerations\tmean_us");
    for nodes in NODE_COUNTS {
        for exact in [false, true] {
            let node_configs = (0..nodes)
                .map(|index| NodeConfig {
                    id: format!("node-{index:02}"),
                    base_url: format!("http://127.0.0.1:{}/v1", 10_000 + index),
                    models: HashMap::from([("bench".to_owned(), "bench".to_owned())]),
                    ..NodeConfig::default()
                })
                .collect::<Vec<_>>();
            let runtime_nodes = node_configs
                .iter()
                .map(Node::from_config)
                .collect::<Result<Vec<_>>>()?;
            let scheduler = Scheduler::new(runtime_nodes, RoutingConfig::default());
            let body = json!({"prompt": "shared-prefix-for-scheduling"});
            let mut input = routing_text(
                "completions",
                Some("bench"),
                Some(&body),
                &PrefixConfig::default(),
            );
            scheduler.prefix_directory().record("node-00", &input);
            if exact {
                input.set_token_ids(vec![1, 2, 3, 4]);
                scheduler
                    .exact_cache_directory()
                    .configure_node("node-00", 16);
                scheduler.exact_cache_directory().apply(
                    "node-00",
                    vec![CacheMutation::Store {
                        hashes: vec![BlockHash::Integer(1)],
                        parent: None,
                        token_ids: vec![1, 2, 3, 4],
                        block_size: 4,
                        group: 0,
                    }],
                )?;
            }
            let started = Instant::now();
            for _ in 0..iterations {
                let selected = scheduler
                    .acquire(Some("bench"), input.clone(), &HashSet::new(), 1024)
                    .await?;
                std::hint::black_box(selected.score);
                drop(selected.lease);
            }
            println!(
                "{}\t{}\t{}\t{:.3}",
                if exact { "exact" } else { "approximate" },
                nodes,
                iterations,
                started.elapsed().as_secs_f64() * 1_000_000.0 / iterations as f64,
            );
        }
    }
    Ok(())
}

fn percentile(values: &[Duration], percentile: usize) -> Duration {
    let index = values
        .len()
        .saturating_sub(1)
        .saturating_mul(percentile)
        .div_ceil(100);
    values[index]
}

fn rss_kib() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}
