# 配置契约与模块边界

## 唯一配置来源

`src/config.rs` 定义序列化类型和反序列化默认值；`src/config/contract.rs` 定义编辑器预设、节点校验规则及保留请求头。新增字段或调整默认值、边界时先修改 Rust，再重新生成前端契约。

```sh
cd web
bun run contract:generate
bun run contract:check
```

生成器通过可选的 `config-contract` 特性运行 `examples/config_contract.rs`。类型由 [ts-rs](https://docs.rs/ts-rs/12.0.1/ts_rs/) 根据 Rust 类型和 Serde 名称生成；默认值由 Rust 的 `Default` 实现序列化。生成结果经过仓库内固定版本的 Biome 格式化，保存到 `web/src/generated/`，与源码一起提交。不要手工修改这些文件。

`contract:check` 在临时目录重新生成并比较文件内容，不修改工作区；CI 和发布流程都要求此检查通过。普通前端构建使用已提交的生成文件，不要求安装 Rust。普通网关构建不启用类型生成依赖。

后端 `Settings::validate` 保留全局服务和调度约束，并调用节点契约验证；前端 `config-validation.ts` 解释同一组规则，数值控件也读取其中的上下限。前端只单独维护编辑状态的检查，例如未完成的模型映射行和重复行，这些状态尚不是可保存的配置。

必须区分两种默认值：后端省略 `max_concurrency` 时仍为 1；编辑器新建节点预设为 16，仍默认选择 vLLM。这些既有产品预设现在也定义在 Rust 中。提供方、KV 事件和模型能力的默认值不再在浏览器中逐项复制。

Rust 的 JSON 反序列化仍负责未知字段、枚举及整数类型检查，保存和预检 API 始终在服务端验证。浏览器只接受可精确表达的安全整数，其上限同时受后端规则约束；后端仍支持原有的 `usize`/`u64` 范围。生成的 `Settings` 类型覆盖完整配置，但当前管理编辑器只修改节点。

`tests/fixtures/node-config-contract.json` 中的 57 个有效/无效用例由 Rust 和 TypeScript 同时读取，包括数值边界、条件生效的 vLLM 限制、URL 同源规则、模型能力映射、保留请求头及 KV 发布/重放地址。修改规则时同步增加边界用例。

## 职责划分

| 模块入口 | 子模块职责 |
| --- | --- |
| `proxy.rs` | 请求读取与协议识别；`request_compat` 做输入兼容处理，`payload` 准备协议转换请求，`upstream` 负责节点选择、重试及模型重写，`response` 管理非流式响应和缓冲预算，`streaming` 管理 SSE 转换及流生命周期，`headers` 管理转发规则。 |
| `server.rs` | 组装网关、路由和进程退出；`transport` 管理连接与 IP 限制，`middleware` 管理认证、准入和请求观测，`assets` 提供压缩静态资源，`admin` 实现管理操作，`reconcile` 同步持久化配置和运行状态。 |
| `vllm.rs` | 管理节点后台任务；`monitor` 负责版本与 Prometheus 遥测，`tokenization` 负责请求和缓存，`kv_events` 负责事件订阅、重放及目录恢复。 |
| `supervisor.rs` | 升级事务、控制接口和恢复；`worker` 管理子进程启停与健康等待，`releases` 管理发布目录及原子持久化，`deploy` 提供部署管理页面和 HTTP 接口。 |
| 前端 `App.tsx` | 页面选择、控制面状态与操作协调；`Overview`、`Upstreams` 分别呈现页面，`admin-metrics` 提供共享指标组件，`PairEditor` 管理映射行，`NodeEditor` 保留编辑向导和提交状态。 |

Rust 子模块只向所属模块开放必要的内部接口，已有公共入口和 HTTP 路径保持兼容。原有测试移到各模块的 `tests.rs`，集成测试继续从公共入口验证实际协议和生命周期。拆分没有改变调度策略、退出期限或数据库格式。
