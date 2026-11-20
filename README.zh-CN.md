[English](README.md) | 简体中文 | [日本語](README.ja.md)

# Estuary

[![CI](https://github.com/iMMIQ/estuary/actions/workflows/ci.yml/badge.svg)](https://github.com/iMMIQ/estuary/actions/workflows/ci.yml)
[![Release](https://github.com/iMMIQ/estuary/actions/workflows/release.yml/badge.svg)](https://github.com/iMMIQ/estuary/actions/workflows/release.yml)

Estuary 是一个面向 OpenAI 兼容模型服务的自托管 Rust 推理网关。它在单一公开监听端口上同时提供 OpenAI 与 Anthropic 客户端协议，综合并发负载、延迟、错误率、估算计算量与提示词前缀亲和度将请求调度到健康节点，并以单二进制形态内置中英文管理控制台。

## 核心特性

- **统一客户端协议**：提供 OpenAI Chat Completions、Responses、Completions、Embeddings、Models 与 Anthropic Messages（含 `count_tokens`），所有生成类协议统一适配到上游 Chat Completions 后端。
- **负载与前缀感知调度**：支持每节点硬并发上限、负载/Prefill/Decode/延迟/错误加权评分、面向任意服务的近似基数树前缀亲和，以及面向 vLLM 0.25+ 的精确 KV-Block 路由。
- **实时控制面**：基于 SQLite 的节点配置存储，支持不中断在途请求的节点热更新、乐观锁版本控制、主动健康探测、熔断器与有界重试。
- **内建安全控制**：可选公开端静态 API Key 鉴权、按 Key/IP 的 RPM 限流、持久化的单 IP 连接数限制、管理端 Token 认证，以及基于 BLAKE3 认证加密的落盘上游凭证保护。
- **观测与可选会话日志**：内置实时观测面板（平滑速率、滑动窗口分位数与历史曲线）、标准 OpenMetrics `/metrics` 导出，以及基于独立 SQLite 数据库的内容去重会话日志。
- **有界背压与极简部署**：连接数、准入队列、响应缓冲与流式转发均有独立背压上限并支持优雅停机；提供静态 Linux 二进制与精简 Alpine Docker 镜像。

## 快速开始

### Docker 部署

构建镜像（脚本会自动探测并选择最快的官方或国内镜像源）并通过 Docker Compose 启动：

```bash
export ESTUARY_ADMIN_TOKEN="$(openssl rand -hex 32)"
./deploy/docker-build.sh
docker compose up -d
```

- 公开推理 API：`http://127.0.0.1:8080`
- 管理控制台：`http://127.0.0.1:9090/admin/`（HTTP Basic 认证：用户名任意，密码为 `ESTUARY_ADMIN_TOKEN`）

关于预构建静态 Linux 二进制、构建版本号、持久化卷与滚动升级，请参阅 [Deployment](docs/deployment.md)。

### 从源码运行

按 [rust-toolchain.toml](rust-toolchain.toml) 安装 Rust 与 LLD 链接器（`ld.lld`），并按 [.node-version](.node-version) 准备 Node.js 与 Bun。先构建一次内置前端资源，再启动网关：

```bash
(cd web && bun install --frozen-lockfile && bun run build)
mkdir -p data
cargo run --release -- \
  --database ./data/estuary.db \
  --listen 0.0.0.0:8080 \
  --admin-listen 127.0.0.1:9090
```

## 添加节点并验证

打开 `http://127.0.0.1:9090/admin/` 新增上游节点，填写：

- 唯一节点 ID；
- OpenAI 兼容的绝对 `base_url`（通常以 `/v1` 结尾）；
- 至少一条公开模型到上游模型的映射（映射值填 `*` 表示透传原模型名）；
- 正整数 `max_concurrency` 并发上限与提供方类型（`openai` 或 `vllm`）。

空数据库可以正常启动，但在至少有一个节点可路由之前，`/health/ready` 会返回 `503`。

保存节点后，验证模型目录并发起一次对话请求：

```bash
curl -sS http://127.0.0.1:8080/v1/models

curl -sS http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "gateway-chat",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

Claude Code、Codex、OpenCode 的接入配置与完整协议边界请参阅 [API & Clients](docs/api.md)。

## 文档导航

主技术文档统一使用英文编写：

| 文档 | 内容 |
| --- | --- |
| [Documentation Index](docs/README.md) | 按使用角色划分的完整文档地图。 |
| [Deployment](docs/deployment.md) | 静态 Linux 二进制、Docker 构建、Compose 运行、优雅停机与滚动更新。 |
| [Configuration & Security](docs/configuration.md) | 进程参数与环境变量、节点配置模型、无中断热更新、鉴权、限流与凭证加密。 |
| [API & Clients](docs/api.md) | 公开推理路由、协议转换边界、Claude Code / Codex / OpenCode 接入及完整管理端 API。 |
| [vLLM Provider](docs/vllm.md) | vLLM 0.25+ 版本门禁、指标遥测、共享分词缓存、ZMQ KV 事件与精确路由。 |
| [Observability & Metrics](docs/observability.md) | 仪表盘平滑速率与直方图定义、`/admin/api/observability` 与全部 `/metrics` 指标参考。 |
| [Session Logging](docs/session-logging.md) | 独立 SQLite 请求/会话日志、内容去重存储、脱敏边界、保留清理与查询限制。 |
| [Architecture](docs/architecture.md) | 请求与背压链路、调度评分公式、前缀/KV 目录、热更新与系统边界。 |
| [Contributing](CONTRIBUTING.md) | 本地开发、CI 检查、共享配置契约生成、本地 E2E 测试与性能基准。 |

## 开源协议

[Apache License 2.0](LICENSE)
