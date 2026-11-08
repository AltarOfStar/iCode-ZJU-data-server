-- iCode data server schema v10
--
-- 与 teams schema v10 在范围内表列级一致（server-acceptance §4.2）：
--   sessions / session_turns / event_batches / trajectory_events / workspace_states /
--   repository_registry / object_registry / ingest_receipts
-- 范围外表（adoption_evidence、commit_bindings、pull_request*、attribution_*、
-- session_manifests、session_rollbacks、similarity_calibration）v1 不写入，schema 保留。
--
-- 相对 teams v10 的增量（扩列纪律：客户端抽取、服务端列、服务端校验同时改）：
--   * ingest_receipts.code / message：终态拒绝的拒绝码与说明（本服务 §2.2 rejected 需要留证）；
--   * object_registry.missing_hashes：缺块终态的缺口清单（§3.2-5）；
--   * sessions.body_state 沿用 teams v4 已扩的列；
--   * session_anchors：服务端本地簿记（非 teams 表）——`agent.env` 可能先于 `session.meta`
--     到达，先把 agent-config 锚点落在这里，等 sessions 行建立时 coalesce。

create table if not exists schema_meta (
   component text primary key,
   schema_version integer not null,
   applied_at timestamptz not null default now(),
   applied_by text not null
);

create table if not exists sessions (
   tenant text not null,
   external_session_id text not null,
   generation integer not null default 0,
   format_version integer,
   schema_version integer,
   producer_version text,
   user_id text not null,
   team_id text not null default 'default',
   team_path text[] not null default '{}',
   team_resolution text not null default 'unresolved',
   title text,
   agent_profile text,
   model_provider text,
   model_id text,
   created_at timestamptz,
   updated_at timestamptz,
   primary_cwd_digest text,
   turn_count integer not null default 0,
   message_count integer not null default 0,
   total_tokens bigint not null default 0,
   parent_external_session_id text,
   snapshot_state text not null default 'ok',
   snapshot_sha256 text,
   content_hash text not null,
   observed_at timestamptz not null default now(),
   -- v2：会话所属仓库（单调累积，≤16）
   repository_identities text[] not null default '{}',
   -- v4：正文锚点与三向比对状态
   snapshot_size bigint,
   snapshot_etag text,
   body_state text,
   body_checked_at timestamptz,
   -- v10：环境复现三列（agent-config-sync §1：唯一写入者是 agent.env）
   agent_config_sha256 text,
   workspace_kind text,
   agent_config_captured_at timestamptz,
   primary key (tenant, external_session_id)
);
-- 既有库补列（幂等；schema 版本随下一次变更统一 +1）
alter table sessions add column if not exists agent_config_captured_at timestamptz;
create index if not exists sessions_repository_identities on sessions using gin (repository_identities);
create index if not exists sessions_snapshot_sha on sessions (tenant, snapshot_sha256);
create index if not exists sessions_agent_config_sha on sessions (tenant, agent_config_sha256);
create index if not exists sessions_user_observed
   on sessions (tenant, user_id, observed_at desc, external_session_id desc);

create table if not exists session_turns (
   tenant text not null,
   session_id text not null,
   generation integer not null,
   turn_number integer not null,
   turn_id text,
   started_at timestamptz,
   ended_at timestamptz,
   duration_ms integer,
   end_reason text,
   observed_at timestamptz not null default now(),
   primary key (tenant, session_id, generation, turn_number)
);

create table if not exists event_batches (
   tenant text not null,
   session_id text not null,
   batch_id text not null,
   generation integer not null,
   client_sequence integer not null,
   first_event_sequence bigint not null,
   last_event_sequence bigint not null,
   event_count integer not null,
   unparsed_count integer not null,
   gap_count integer not null default 0,
   completeness_state text not null,
   processing_state text not null default 'processed',
   payload_sha256 text,
   payload_size bigint,
   payload_etag text,
   observed_at timestamptz not null default now(),
   primary key (tenant, batch_id)
);
create index if not exists event_batches_payload_sha on event_batches (tenant, payload_sha256);
create index if not exists event_batches_session_generation
   on event_batches (tenant, session_id, generation, first_event_sequence);

create table if not exists trajectory_events (
   tenant text not null,
   session_id text not null,
   generation integer not null,
   sequence bigint not null,
   batch_id text not null,
   event_id text,
   event_type text not null,
   turn_id text,
   operation_id text,
   actor_kind text,
   actor_role text,
   occurred_at timestamptz,
   monotonic_ns bigint,
   tool_name text,
   tool_kind text,
   duration_ms integer,
   outcome text,
   output_tokens bigint,
   input_tokens bigint,
   primary key (tenant, session_id, generation, sequence)
);
create index if not exists trajectory_events_lookup
   on trajectory_events (tenant, session_id, event_type, sequence);
create index if not exists trajectory_events_time on trajectory_events (tenant, occurred_at);

create table if not exists object_registry (
   tenant text not null,
   sha256 text not null,
   size_bytes bigint not null,
   etag text,
   channel text not null,
   first_user_id text not null default 'service',
   state text not null,
   upload_id text,
   ticket_expires_at timestamptz,
   created_at timestamptz not null default now(),
   verified_at timestamptz,
   -- 本服务扩列：缺块终态的缺口清单（§3.2-5）
   missing_hashes text[] not null default '{}',
   primary key (tenant, sha256),
   -- 取值域与 models.OBJECT_* 一致（schema 层静态可校验）
   constraint object_registry_state_check
      check (state in ('verified', 'pending', 'missing', 'superseded'))
);
create index if not exists object_registry_state_expiry
   on object_registry (tenant, state, ticket_expires_at);
-- 既有库补约束（幂等；create table if not exists 不会给老表加约束）
do $$
begin
   if not exists (
      select 1 from pg_constraint
      where conname = 'object_registry_state_check'
        and conrelid = 'object_registry'::regclass
   ) then
      alter table object_registry add constraint object_registry_state_check
         check (state in ('verified', 'pending', 'missing', 'superseded'));
   end if;
end $$;

create table if not exists ingest_receipts (
   tenant text not null,
   channel text not null,
   idempotency_key text not null,
   sha256 text not null,
   state text not null,
   created_at timestamptz not null default now(),
   -- 本服务扩列：终态拒绝的拒绝码与说明
   code text,
   message text,
   primary key (tenant, channel, idempotency_key),
   -- 取值域与 models.RECEIPT_* 一致（schema 层静态可校验）
   constraint ingest_receipts_state_check
      check (state in ('pending', 'committed', 'rejected', 'unsupported'))
);
create index if not exists ingest_receipts_state on ingest_receipts (tenant, state);
do $$
begin
   if not exists (
      select 1 from pg_constraint
      where conname = 'ingest_receipts_state_check'
        and conrelid = 'ingest_receipts'::regclass
   ) then
      alter table ingest_receipts add constraint ingest_receipts_state_check
         check (state in ('pending', 'committed', 'rejected', 'unsupported'));
   end if;
end $$;

create table if not exists workspace_states (
   tenant text not null,
   external_session_id text not null,
   repository_identity text not null,
   user_id text not null,
   head_sha text,
   head_tree_sha text,
   branch text,
   detached boolean not null default false,
   remote_kind text not null,
   staged_entries integer not null default 0,
   worktree_entries integer not null default 0,
   untracked_entries integer not null default 0,
   omitted_count integer not null default 0,
   complete boolean not null default true,
   session_started_at timestamptz,
   captured_at timestamptz,
   capture_duration_ms integer,
   body_sha256 text not null,
   body_size_bytes bigint not null,
   body_etag text,
   created_at timestamptz not null default now(),
   -- v10：忽略清单（只路径，≤200 条，正文永不入容器）
   ignored_count integer not null default 0,
   ignored_truncated boolean not null default false,
   ignored_entries jsonb not null default '[]'::jsonb,
   primary key (tenant, external_session_id, repository_identity)
);
create index if not exists workspace_states_body_sha on workspace_states (tenant, body_sha256);

create table if not exists repository_registry (
   tenant text not null,
   local_identity text not null,
   repository_key text not null,
   provider text not null,
   connection_id text not null,
   host text not null,
   api_path text not null,
   resolution text not null,
   user_id text not null default 'unknown',
   first_seen_at timestamptz not null default now(),
   last_seen_at timestamptz not null default now(),
   observed_at timestamptz not null,
   primary key (tenant, local_identity)
);
create index if not exists repository_registry_key on repository_registry (tenant, repository_key);

-- 服务端本地簿记：agent-config 锚点（`agent.env` 先于 `session.meta` 到达时使用）。
-- 环境三列的镜像：等 `sessions` 行建立时 coalesce（agent-config-sync §1）。
create table if not exists session_anchors (
   tenant text not null,
   session_id text not null,
   agent_config_sha256 text,
   workspace_kind text,
   agent_config_captured_at timestamptz,
   updated_at timestamptz not null default now(),
   primary key (tenant, session_id)
);
alter table session_anchors add column if not exists agent_config_captured_at timestamptz;

insert into schema_meta (component, schema_version, applied_by)
values ('icode-data-server', 10, 'bootstrap')
on conflict (component) do update
   set schema_version = greatest(schema_meta.schema_version, excluded.schema_version),
       applied_at = now(),
       applied_by = excluded.applied_by;
