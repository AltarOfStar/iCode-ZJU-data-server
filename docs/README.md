# 规格副本（只读参考）

本目录的两份文档是客户端仓库 `iCode-ZJU-wty` 的 `design/otel-reporting/` 下的设计稿副本：

| 文件 | 来源 |
|---|---|
| `server-acceptance.md` | `iCode-ZJU-wty/design/otel-reporting/server-acceptance.md`（服务端契约，本仓库的实现依据） |
| `client-design.md` | `iCode-ZJU-wty/design/otel-reporting/client-design.md`（帧格式、单元类型、白名单载荷） |

**规格来源：客户端仓库 `design/otel-reporting`。** 这里只是为离线查阅而复制，权威版本以客户端仓库为准；
两处的差异一律按客户端仓库更新，不要在本目录里就地改规格。

代码与文档的对应关系：

- 帧格式 / 单元类型 → `client-design.md` §4.1、§4.2
- 接口契约（`/v1/batch`、`/v1/session/{id}/manifest`）→ `server-acceptance.md` §2
- 处理规则（幂等、字节组装、派生器、乱序分叉）→ `server-acceptance.md` §3
- 存储（OBS 键布局、teams schema v10 在范围内表）→ `server-acceptance.md` §4
- 容量、背压、保留 → `server-acceptance.md` §6
