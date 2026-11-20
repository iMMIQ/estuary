# 配置契约与模块边界

## 唯一配置来源

`src/config.rs` 定义序列化类型、反序列化默认值与 `clap::Args` CLI 绑定；`src/config/contract.rs` 定义编辑器预设、数值上下限及保留请求头。新增字段或调整默认值、边界时先修改 Rust，再重新生成前端契约。

```sh
cd web
bun run contract:generate
bun run contract:check
```

生成器通过可选的 `config-contract` 特性运行 `examples/config_contract.rs`。配置类型以及节点快照、观测快照、会话日志 DTO 均由 [ts-rs](https://docs.rs/ts-rs/12.0.1/ts_rs/) 根据 Rust 类型和 Serde 名称生成；默认值、数值边界和保留请求头由 Rust 序列化后统一输出到单个文件 `web/src/generated/config.ts`，经仓库固定版本的 Biome 格式化后与源码一起提交。不要手工修改该文件。

`contract:check` 在临时目录重新生成并比较文件内容，不修改工作区；CI 和发布流程都要求此检查通过。普通前端构建使用已提交的生成文件，不要求安装 Rust；普通网关构建不启用 `ts-rs` 依赖。

后端 `src/config/contract.rs` 与前端 `web/src/config-validation.ts` 各自直接对强类型 `NodeConfig` 执行校验，数值控件读取生成的 `NODE_INTEGER_LIMITS` 边界。前端仅额外维护编辑态检查（如未完成的模型映射行和重复行）。

必须区分两种默认值：后端省略 `max_concurrency` 时仍为 1；编辑器新建节点预设为 16，仍默认选择 vLLM。这些产品预设均定义在 Rust 中。

Rust 的 JSON 反序列化负责未知字段、枚举及整数类型检查，保存和预检 API 始终在服务端验证。浏览器只接受可精确表达的安全整数，其上限同时受 `NODE_INTEGER_LIMITS` 约束；后端仍支持完整的 `usize`/`u64` 范围。

`tests/fixtures/node-config-contract.json` 中的 57 个有效/无效用例由 Rust 和 TypeScript 同时读取，包括数值边界、条件生效的 vLLM 限制、URL 同源规则、模型能力映射、保留请求头及 KV 发布/重放地址。修改规则时同步增加边界用例。

## 职责划分

| 模块入口 | 子模块职责 |
| --- | --- |
| `proxy.rs` | 请求读取与协议识别；`request_compat` 做输入兼容处理，`upstream` 负责节点选择、重试及模型重写，`response` 管理非流式响应和缓冲预算，`streaming` 管理 SSE 转换及流生命周期，`headers` 管理转发规则。 |
| `server.rs` | 组装网关、路由、后台任务和进程退出；`transport` 管理连接与 IP 限制，`middleware` 管理认证、准入和请求观测，`assets` 提供 `.gz` 预压缩静态资源，`admin` 实现管理操作，`logs` 提供会话日志查询，`reconcile` 同步持久化配置和运行状态。 |
| `vllm.rs` | 管理节点后台任务；`monitor` 负责版本与 Prometheus 遥测，`tokenization` 负责请求和缓存，`kv_events` 负责事件订阅、重放及目录恢复。 |
| `observability.rs` / `metrics.rs` | `observability` 维护有界滑动窗口、直方图与历史采样服务仪表盘；`metrics` 通过统一的 `prometheus-client` `Registry` 导出合法 OpenMetrics `/metrics`。 |
| 前端 `App.tsx` | 页面选择、控制面状态与操作协调；`Overview`、`Upstreams`、`NodeDetails`、`SessionLogs`、`Observability` 呈现各视图，`admin-metrics` 和 `ui` 提供共享指标与基础组件，`i18n.ts` 提供轻量双语上下文，`PairEditor` 与 `NodeEditor` 管理节点编辑。 |
