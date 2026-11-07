# iCode-ZJU data server

iCode 事实数据上报服务端（`report/1.0`）：接收客户端帧流、按内容寻址重组字节对象、在完整流上做同构派生（切窗 / 索引抽取 / 代际），并把事实落到 OBS 与 PostgreSQL。

## 目录规划

```
src/icode_data_server/
  app.py            # FastAPI 装配（lifespan + 服务容器）
  services.py       # 配置 → 存储后端 → 处理器 的装配与启动
  config.py         # 环境变量配置（默认 memory + local，零外部依赖可启动）
  frames.py         # 自定义帧编解码（u32 header_len | header JSON | payload）
  errors.py         # 拒绝码与单元结果类型
  logging_setup.py  # 单行 JSON 结构化日志（不打印 token / 载荷）
  api/              # /healthz、POST /v1/batch、POST /v1/session/{id}/manifest
  ingest/
    processor.py    # 单元分发、幂等收据、deferred 回滚、隔离区、背压
    derive.py       # 同构派生器（纯函数：切窗 / 白名单抽取 / turns / 代际）
    trajectory.py   # 轨迹行缓冲 + 窗口持久化
    assembly.py     # 字节块暂存与组装（快照块 / 工作区文件块）
    workspace.py    # IGWS 容器拼装（确定性纯函数）
    session_meta.py # session/1.0 白名单校验
    agent_config.py # agent-config/1.0 校验
    repository_identity.py
    manifest.py     # 缺块协商
    backpressure.py # 令牌桶 + 并发批次闸门
    locks.py        # 按键串行化
  storage/
    models.py       # 与 teams schema v10 在范围内表的行模型
    objects.py      # ObjectStore 协议 + LocalObjectStore / S3ObjectStore
    repository.py   # Repository 协议 + MemoryRepository
    postgres.py     # PostgresRepository（asyncpg）
    schema.sql      # DDL（teams v10 在范围内表 + 本服务扩列）
docs/               # 规格副本（server-acceptance.md、client-design.md）
tests/              # pytest + httpx.ASGITransport，禁止外网
```

## 规格

- `docs/server-acceptance.md`：服务端契约（接口字段、处理规则、存储、容量）。
- `docs/client-design.md`：客户端设计（帧格式、单元类型、白名单载荷）。
- `docs/README.md`：规格来源说明（客户端仓库 `design/otel-reporting`）。

## 运行方式

```bash
uv sync
uv run python -m icode_data_server --self-check   # 进程内冒烟（/healthz）
uv run python -m icode_data_server                # 监听 127.0.0.1:8080
uv run uvicorn icode_data_server.app:create_app --factory
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
```

默认配置零外部依赖：`REPOSITORY_BACKEND=memory` + `OBJECT_STORE_BACKEND=local`，对象落在 `data/objects/`，
分片暂存在 `data/staging/`（`data/` 已在 `.gitignore` 中）。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `REPOSITORY_BACKEND` | `memory` | `memory` \| `postgres` |
| `OBJECT_STORE_BACKEND` | `local` | `local` \| `s3` |
| `ICODE_DS_POSTGRES_DSN` | — | `postgres` 后端必填 |
| `ICODE_DS_POSTGRES_AUTO_MIGRATE` | `1` | 启动时幂等应用 `storage/schema.sql`（生产建议关闭，改由部署期执行） |
| `ICODE_DS_OBJECT_ROOT` | `data/objects` | 本地对象存储根目录 |
| `ICODE_DS_S3_BUCKET` / `ICODE_DS_S3_ENDPOINT_URL` / `ICODE_DS_S3_REGION` | — | 对接 OBS 时填 `endpoint_url` |
| `ICODE_DS_S3_ACCESS_KEY_ID` / `ICODE_DS_S3_SECRET_ACCESS_KEY` / `ICODE_DS_S3_PREFIX` | — | 凭据与键前缀 |
| `ICODE_DS_TOKENS` | `{"dev-token":{"tenant":"dev","user_id":"dev-user"}}` | token → `(tenant, user_id)` |
| `ICODE_DS_AUTH_DISABLED` | `0` | 仅本地开发：缺 token 时按 `default/anonymous` |
| `ICODE_DS_HOST` / `ICODE_DS_PORT` | `127.0.0.1` / `8080` | 监听地址 |
| `ICODE_DS_MAX_BODY_BYTES` | 16 MiB | 请求体解压前上限（超限 `413`） |
| `ICODE_DS_MAX_EXPANDED_BYTES` | 64 MiB | 解压后上限（超限 `413`） |
| `ICODE_DS_MAX_FRAME_PAYLOAD_BYTES` | 1 MiB | 单帧载荷上限（超限按 `payload_too_large` 拒绝） |
| `ICODE_DS_MAX_FRAMES_PER_BATCH` | 4096 | 单批帧数上限（超限 `413`） |
| `ICODE_DS_BATCH_TIMEOUT_S` | 30 | 单批处理上限（超时 `503`） |
| `ICODE_DS_RATE_LIMIT_PER_SECOND` / `ICODE_DS_RATE_LIMIT_BURST` | 25 / 50 | 单租户令牌桶（超限 `429` + `Retry-After`） |
| `ICODE_DS_MAX_INFLIGHT_BATCHES` | 32 | 单租户并发批次（超限整批 `deferred`） |
| `ICODE_DS_STAGING_ROOT` | `data/staging` | 分片暂存目录 |
| `ICODE_DS_STAGING_BYTES_PER_SESSION` / `..._PER_TENANT` | 64 MiB / 2 GiB | 组装预算（超限按最旧淘汰） |
| `ICODE_DS_ASSEMBLY_TTL_S` | 86400 | 缺块终态 TTL |
| `ICODE_DS_MAX_WINDOW_INDEX_LINES` / `ICODE_DS_MAX_WINDOW_BYTES` | 200 / 4 MiB | 切窗上限 |
| `ICODE_DS_AGENT_ENV_MAX_BYTES` | 256 KiB | `agent.env` 载荷上限 |

## 已实现范围（v1）

- `POST /v1/batch`：帧解析（含 gzip）、逐帧幂等（`duplicates`）、终态拒绝（`rejected` + 原因）、
  前置条件未满足（`deferred` + `retry_after_ms`）、未知类型/版本进隔离区、`413/429/400/401` 契约。
- `POST /v1/session/{id}/manifest`：`missing_chunk_hashes` / `missing_file_shas` /
  `acked_last_sequence` / `known_snapshot`，只读无副作用。
- 单元：`trajectory.lines`、`snapshot.chunk`、`snapshot.manifest`、`session.meta`、`workspace.manifest`、
  `workspace.file`、`agent.env`、`repo.identity`。
- 派生器：代际（`branch_id` 变化 / 文件重建）、确定性切窗、白名单索引抽取、`session_turns`，
  乱序与重放收敛到同一窗口集合。
- 存储：`ObjectStore`（local / s3+OBS endpoint）与 `Repository`（memory / postgres），
  `schema.sql` 与 teams v10 在范围内表列级一致。

## 尚未实现 / 已知限制

- 进程内轨迹缓冲（未切窗的行、各代际行摘要）不持久化：进程重启后需客户端 `reset` 重放才能补齐；
  已提交窗口本身在 OBS/PG 里是权威事实。
- 组装预算淘汰（`assembly_evicted`）后，同帧重发会被幂等收据去重 → 需由 manifest 协商后按新 scope 重传。
- `sessions` 锚点的 `observed_at` 平手裁决、`stale_report` 证据只落结构化日志（teams 无对应列）。
- 保留与分区策略（§6.3：`ingest_receipts` 月分区、`trajectory_events` 12 个月等）未在 DDL 中建分区。
- 未接入自身 OTel 指标（§9-5）；`event_batches` 的 `payload_etag` 等 L2 锚点列留空。
