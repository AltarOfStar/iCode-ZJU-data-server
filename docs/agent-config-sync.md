# agent-config/1.0 三仓同步契约（data-server · 服务端侧）

> 状态：生效中。权威规范：**`iCode-teams` 仓库 `docs/specs/data-platform/01-fact-flow-and-storage.md` §4.6**（本地检出：`C:\Users\wty\Desktop\LingXi\iCode-teams`）。
> 本文档是"客户端仓库 `design/otel-reporting/` 规格副本"之外的**第三份同步材料**：它定义本服务端在环境复现（`agent.env` / `agent-config/1.0`）上的 ingest 与存储口径，并列出与权威规范的差异清单。三仓（teams 插件、iCode 客户端、本服务端）任何一侧改动相关行为时，必须同批对齐另外两侧。

## 1. 契约要点（服务端侧）

- **单一写入者**：`agent.env` 帧是会话行环境三列的唯一写入者；`session.meta` **不再携带** `environment{...}`。收到仍带 `environment` 的 `session.meta` 按**未知字段显式拒绝**（与现有严格解析口径一致），不做兼容读取。
- 会话行三列（schema v10）：
  - `agent_config_sha256 text`——`agent-config/1.0` 载荷的内容寻址 sha（`key = sha256(payload)`，`ingest/agent_config.py` 既有口径）；
  - `workspace_kind text`——`git|non_git|no_repo` 枚举；
  - `agent_config_captured_at timestamptz`——**客户端采集时刻**（非会话开始时刻）。消费方用它和 `workspace_states.session_started_at` 比较区分"会话前采集"与"后补"。
- 早到锚点：`agent.env` 先于 `session.meta` 到达时补最小占位行（既有 `session_anchors` 机制保持）；`session.meta` 落地后覆写占位 `content_hash`，**不写也不清除**三列。
- 幂等：`ingest_receipts` 键 `agent-config:<session>:<contentHash>`；同键重报 → duplicate；新内容覆盖三列并按覆写汰换把旧引用对象置 `superseded`（不删 OBS 字节）。
- **信封随重报收敛**（分类变化不改变载荷，client-design §4.4）：同一 `contentHash` 再次上报时响应仍是 `duplicate`，但服务端把信封里的 `workspaceKind`/`capturedAt` 写到锚点与会话行——不新建对象、不汰换。三条边界：内容必须仍是当前锚点（旧 blob 的重放不改任何事实）；`capturedAt` 不得倒退（乱序重放的旧信封被丢弃）；信封非法时按首次校验的口径忽略刷新。
- 校验（ingest 面，与 teams `service/ingest.ts` `checkedAgentConfigPut` 同口径）：`contentHash == sha256(payload)`；`workspaceKind` 三值枚举；`capturedAt` 必填且 ISO-8601；`size ≤ 256 KiB`（`ICODE_DS_AGENT_ENV_MAX_BYTES`）。
- 载荷校验只到形状（JSON 对象 + `schema` 匹配 + `redacted[]` 键名）：`sections`（五键、`captured|absent`）与 `truncated`（布尔）为必填成员，出现未知成员按宽松忽略、缺失按显式失败处理（两处口径取一后写入本表，当前 teams 端为"必填"）。
- **格式与 teams 生产方一致（2026-11-08 对齐）**：载荷字段形状由 `01 §4.6` 的载荷表定义，两个生产方（teams 插件外置上报、iCode 特设版内置上报）产出的**字段逐项相同**：`mcp.servers[]` 的 `transport`/`connectionDigest` 无该事实时**省略**（不填空串）、`settingsDigest` 无显式 settings 时**省略**、`model.params` 只排除 `name`/`id`、`skills[].source` 取 `config|linked|""`、脱敏集合统一（含 `cookie` 段、显式清单与后缀变体、按分段的连接键、`chat_options` 的 JSON 字符串形态）。本服务的载荷形状校验因此对两个生产方同时成立；它**比 teams ingest 更严格**（teams 端不解析载荷、只校验引用与信封），这一差异只影响"畸形载荷何时被拒"，不改变合法载荷的字节或字段，故不构成跨端格式差异。
- **排序语义的一处已知差异（未对齐，待评估）**：teams 端的收据键标识**一次观察**（`agent-config:<session>:<capturedAt>`，01 §4.6），因此"配置 A→B→A"的第三个单元必定落库、锚点回到 A；本服务的单元键是内容派生的 `sha256(agent.env|session|scope|payload_sha256)`，A→B→A 的第三个单元与最初的 A **同键**，`_refresh_agent_env_envelope` 只在"内容仍是当前锚点"时生效，于是返回 `duplicate` 且锚点停在中转版本 B。修复需要让 `unit_id` 带上采集时刻（客户端 `scope_agent_env` 与服务端 `unit_id_for` 同步改），属传输层变更，须单独立项并同步本表与客户端 §4；在此之前，本服务对"改回旧配置"的会话保留旧锚点——**存证不丢（旧 blob 仍在 OBS），只是行上锚点不是最新观察**。

## 2. 本仓库与规范的差异清单（同步项）

以下 6 项已于 2026-11-07 全部落地（本仓库 `dev/wty` 工作区）；行保留作对齐记录。

| # | 位置 | 现状（已对齐） | 目标 |
|---|---|---|---|
| 1 | `ingest/session_meta.py`、`processor.py`（`_handle_session_meta`） | 已移除 `environment` 解析与 deferred 关联：三列只由 `agent.env` 写；带 `environment` 的 `session.meta` 按未知字段显式拒绝 | 同目标 |
| 2 | `ingest/processor.py`（`_handle_agent_env`）与 `frames.py` 帧信封 | 信封已新增 `workspaceKind`、`capturedAt`（`FrameHeader` 可选字段，不参与 unit_id），随锚点一起落三列 | 同目标 |
| 3 | `storage/schema.sql`（sessions / session_anchors） | 已增加第三列 `agent_config_captured_at timestamptz`（`create table` + 幂等 `alter`；schema 版本仍为 v10，随下一次变更统一 +1） | 同目标 |
| 4 | `ingest/workspace.py`、`processor.py`、`storage/postgres.py`、`storage/repository.py` | 忽略清单三列已**首采冻结**（只在 insert 写入，upsert 的 update 集合不含它们，内存实现同步）；单条 `path ≤ 512` 字符超限按 `schema_invalid`（400）拒绝 | 同目标 |
| 5 | `ingest/agent_config.py` | 已增加 `sections`（五键、`captured|absent`）与 `truncated`（布尔）必填校验；`capturedAt` 在信封侧校验（必填 RFC3339）；`workspaceKind` 三值枚举在信封侧校验 | 同目标 |
| 6 | `ingest/processor.py::_refresh_agent_env_envelope`、`storage/*::get_agent_config_anchor_state` | 同 `contentHash` 重报（分类变化、载荷不变）不再被当成无副作用重放：`duplicate` 响应之外把信封的 `workspaceKind`/`capturedAt` 覆写到锚点与会话行，仅当内容仍是当前锚点且 `capturedAt` 不倒退（client-design §4.4 的落地侧；对应 teams 端 `agent_config.put` 的"以最新一次成功为准"） | 同目标 |
| 7 | 载荷形状（`ingest/agent_config.py`） | 与 2026-11-08 的 teams 生产方对齐：`sections`/`truncated`/`redacted[]` 必填已就绪；`mcp.servers[]` 的派生字段可缺、`settingsDigest` 可缺——形状校验不涉这两处内部结构，两生产方载荷都被接受 | 同目标 |

## 3. 存储与消费口径（不变项）

- 载荷字节只进 OBS（键按既有 sha 布局），PG 只存三列引用——**服务端不解析、不落配置正文**；
- 本服务当前**没有**按 `agent_config_sha256` 取正文的读通道（与 teams `08 §5` 一致：存证 + 索引阶段）；将来若开放，须同时修订 teams `01 §4.6` 的"读侧"条目与本文档；
- 覆写汰换的对象只改 `object_registry.state`，OBS 字节永不删除。

## 4. 跨仓黄金校验（2026-11-07 已执行）

用 iCode 客户端（`dev/wty/built-in-upload` 工作树，0.34.0）真实产出的产物直打本服务端 ingest，六项全过：

1. **帧**：客户端的 `agent.env` / `session.meta` 帧经 `frames.decode_frames` 解码，`workspaceKind`/`capturedAt` 往返一致，`unit_id` 与客户端派生值逐字相同；
2. **载荷**：客户端真实配置根（profile + model + 技能 + MCP stdio·http + hooks）产出的 `agent-config/1.0` 被接受并内容寻址入库——样本留在 `tests/fixtures/agent_env_icode_agent_config_1_0.json`，由 `test_agent_config.py::test_the_icode_client_payload_is_accepted` 守卫；
3. **三列**：同一批 `agent.env` + `session.meta` 提交后，锚点、会话行与信封一致（`capturedAt` 精确到微秒、`workspaceKind = non_git`）；
4. **重报**：同内容换分类 → `duplicate` + 信封覆写（§2-6 的用例）；
5. **协商**：`POST /v1/session/{id}/manifest` 接受客户端的 `{snapshot:{content_hash,chunk_hashes},workspace:{manifest_sha256,file_shas}}`，回 `missing_chunk_hashes`/`missing_file_shas`/`acked_last_sequence`/`known_snapshot`；
6. **拒绝面**：仍带 `environment` 的 `session.meta`、缺 `sections`/`truncated` 或 `workspaceKind` 非三值的 `agent.env` 一律 `schema_invalid`。

限额两侧相容：客户端批 8 MiB / 帧 1 MiB / 4096 帧 ≤ 服务端 16 MiB / 1 MiB / 4096；`agent.env` 两侧都以 256 KiB 为界（客户端超限先降级并置 `truncated=true` 而不是失败）。
