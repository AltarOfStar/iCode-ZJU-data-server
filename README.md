# iCode-ZJU data server

iCode 事实数据上报服务端（`report/1.0`）：接收客户端帧流、按内容寻址重组字节对象、在完整流上做同构派生（切窗 / 索引抽取 / 代际），并把事实落到 OBS 与 PostgreSQL。

## 目录规划

```
src/icode_data_server/
  app.py            # FastAPI 应用装配（/v1/batch、/v1/session/{id}/manifest、/healthz）
  config.py         # 环境变量配置（uv 零外部依赖可启动）
  frames.py         # 自定义帧编解码（u32 header_len | header JSON | payload，支持 gzip）
  api/              # HTTP 接口层：认证、批处理、缺块协商、健康检查
  ingest/           # 单元处理：幂等、字节组装、派生器、IGWS 拼装、agent.env
  storage/          # ObjectStore（local / s3）与 Repository（memory / postgres）
docs/               # 规格副本（server-acceptance.md、client-design.md）
tests/              # pytest + httpx.ASGITransport，禁止外网
```

## 规格

- `docs/server-acceptance.md`：服务端契约（接口字段、处理规则、存储、容量）。
- `docs/client-design.md`：客户端设计（帧格式、单元类型、白名单载荷）。

## 运行方式

```bash
uv sync
uv run python -m icode_data_server --self-check   # 冒烟检查
uv run python -m icode_data_server                # 监听 127.0.0.1:8080
uv run uvicorn icode_data_server.app:create_app --factory
uv run pytest -q
```

默认配置零外部依赖：`REPOSITORY_BACKEND=memory` + `OBJECT_STORE_BACKEND=local`（对象落在 `data/objects/`）。
接入真实后端时设置 `REPOSITORY_BACKEND=postgres`（`ICODE_DS_POSTGRES_DSN`，DDL 见
`src/icode_data_server/storage/schema.sql`）与 `OBJECT_STORE_BACKEND=s3`（`ICODE_DS_S3_*`，可指向 OBS）。

认证：`Authorization: Bearer <token>`，token → `(tenant, user_id)` 由 `ICODE_DS_TOKENS` 提供。
