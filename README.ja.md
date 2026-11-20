[English](README.md) | [简体中文](README.zh-CN.md) | 日本語

# Estuary

[![CI](https://github.com/iMMIQ/estuary/actions/workflows/ci.yml/badge.svg)](https://github.com/iMMIQ/estuary/actions/workflows/ci.yml)
[![Release](https://github.com/iMMIQ/estuary/actions/workflows/release.yml/badge.svg)](https://github.com/iMMIQ/estuary/actions/workflows/release.yml)

Estuary は OpenAI 互換モデルサーバー向けのセルフホスト型 Rust ゲートウェイです。単一の公開リスナーで OpenAI と Anthropic のクライアントプロトコルを提供し、負荷・レイテンシ・エラー率・推定計算量・プロンプトプレフィックス局所性をもとに健全なアップストリームへルーティングします。英語・簡体字中国語対応の管理 UI を組み込んだ単一バイナリとして動作します。

## 主な機能

- **統一クライアントプロトコル**: OpenAI Chat Completions / Responses / Completions / Embeddings / Models および Anthropic Messages（`count_tokens` を含む）を提供し、すべての生成系プロトコルをアップストリームの Chat Completions に統一して転送します。
- **負荷・プレフィックス認識スケジューリング**: ノード単位の同時実行上限、負荷・Prefill・Decode・レイテンシ・エラーの重み付きスコアリング、汎用サーバー向けの近似 Radix Tree プレフィックス親和性、および vLLM 0.25+ 向けの厳密な KV ブロックルーティングを備えています。
- **ライブコントロールプレーン**: SQLite ベースのノード設定管理により、実行中リクエストをドレインせずに設定をライブ更新可能。楽観的リビジョン制御、アクティブヘルスチェック、サーキットブレーカー、有界リトライを搭載しています。
- **組み込みセキュリティ**: オプションの公開 API キー認証、キー/IP ごとの RPM レート制限、永続化される IP 別接続数制限、管理 API トークン認証、BLAKE3 認証付き暗号化による保存時アップストリーム資格情報保護をサポートします。
- **オブザーバビリティとセッションログ**: 平滑化レートとヒストグラムを備えたリアルタイムダッシュボード、OpenMetrics 形式の `/metrics`、別 SQLite データベースへの重複排除セッションログ記録（オプション）を提供します。
- **有界バックプレッシャーと軽量デプロイ**: 接続数・アドミッションキュー・レスポンスバッファ・ストリーミングの各層に明確な上限とグレースフルシャットダウンを実装。静的 Linux バイナリおよび軽量な Alpine Docker イメージで配布できます。

## クイックスタート

### Docker での起動

イメージをビルド（最速の公式またはミラーレジストリを自動選択）し、Docker Compose で起動します。

```bash
export ESTUARY_ADMIN_TOKEN="$(openssl rand -hex 32)"
./deploy/docker-build.sh
docker compose up -d
```

- 公開推論 API: `http://127.0.0.1:8080`
- 管理コンソール: `http://127.0.0.1:9090/admin/`（HTTP Basic 認証：ユーザー名は任意、パスワードに `ESTUARY_ADMIN_TOKEN` を入力）

ビルド済み静的 Linux バイナリ、ビルドバージョン、永続ボリューム、ローリングアップデートについては [Deployment](docs/deployment.md) を参照してください。

### ソースからのビルド

[rust-toolchain.toml](rust-toolchain.toml) に記載の Rust ツールチェーンと LLD リンカー（`ld.lld`）、および [.node-version](.node-version) の Node.js と Bun を用意します。組み込み Web UI を一度ビルドしてから起動してください。

```bash
(cd web && bun install --frozen-lockfile && bun run build)
mkdir -p data
cargo run --release -- \
  --database ./data/estuary.db \
  --listen 0.0.0.0:8080 \
  --admin-listen 127.0.0.1:9090
```

## ノード設定と動作確認

`http://127.0.0.1:9090/admin/` を開き、以下の項目を設定してアップストリームノードを追加します。

- 一意のノード ID
- OpenAI 互換の絶対 `base_url`（通常は `/v1` で終わる URL）
- 1 つ以上の公開モデルからアップストリームモデルへのマッピング（値に `*` を指定するとリクエストされたモデル名をそのまま転送）
- 正の整数 `max_concurrency` およびプロバイダー種別（`openai` または `vllm`）

空のデータベースでも起動できますが、ルーティング可能なノードが 1 つ以上になるまで `/health/ready` は `503` を返します。

保存後、モデル一覧の確認と Chat Completion リクエストを送信します。

```bash
curl -sS http://127.0.0.1:8080/v1/models

curl -sS http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "gateway-chat",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

Claude Code、Codex、OpenCode の設定方法とプロトコル仕様の詳細は [API & Clients](docs/api.md) を参照してください。

## ドキュメント

技術ドキュメントの本文は英語で管理されています。

| ドキュメント | 内容 |
| --- | --- |
| [Documentation Index](docs/README.md) | 役割別に整理されたドキュメント一覧。 |
| [Deployment](docs/deployment.md) | 静的 Linux バイナリ、Docker ビルド、Compose 運用、グレースフルシャットダウンとローリングアップデート。 |
| [Configuration & Security](docs/configuration.md) | プロセス設定・環境変数、ノードスキーマ、ライブ更新、認証・レート制限・資格情報暗号化。 |
| [API & Clients](docs/api.md) | 公開推論エンドポイント、プロトコル変換の境界、Claude Code / Codex / OpenCode 設定、管理 API 一覧。 |
| [vLLM Provider](docs/vllm.md) | vLLM 0.25+ バージョン検証、テレメトリ、共有トークナイズキャッシュ、ZMQ KV イベントと厳密キャッシュルーティング。 |
| [Observability & Metrics](docs/observability.md) | ダッシュボードの平滑化・ヒストグラム定義、`/admin/api/observability`、OpenMetrics `/metrics` リファレンス。 |
| [Session Logging](docs/session-logging.md) | 独立 SQLite へのリクエスト/セッション記録、コンテンツ重複排除、秘匿情報マスク、保持期間とクエリ上限。 |
| [Architecture](docs/architecture.md) | リクエスト処理とバックプレッシャー、スケジューリング計算式、プレフィックス/KV ディレクトリ、設計上の境界。 |
| [Contributing](CONTRIBUTING.md) | ローカル開発、CI チェック、設定コントラクト生成、E2E テストとベンチマーク。 |

## ライセンス

[Apache License 2.0](LICENSE)
