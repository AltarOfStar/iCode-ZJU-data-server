"""schema.sql 与 postgres.py 的静态一致性检查（不依赖数据库）。

口径说明（口径本身就是排查线索）：

- **表**：postgres.py 内嵌 SQL 引用的表必须都在 schema.sql 的 `create table` 清单里，
  且范围内表（schema.sql 头注释列出的 8 张 teams 表 + 本地簿记 session_anchors）
  都要被 postgres.py 引用——防止写错表名，或建了表没人写。
- **列**：INSERT 列清单、SELECT 列清单、UPDATE / ON CONFLICT DO UPDATE 的 SET
  目标列都必须存在于对应表的 DDL 列清单（SELECT 里的函数名 coalesce/max 等会被
  过滤掉，`select *` 不检查）。
- **_SESSION_COLUMNS**：同名子集口径——元组的每一列都存在于 sessions DDL；
  顺序不要求一致（INSERT/SELECT 显式写列名，顺序无关紧要），DDL 允许保留服务端
  不写入的 teams 范围列（generation、team_id、snapshot_etag 等）。
- **ON CONFLICT**：目标列集合必须与该表某个 PK/UNIQUE 约束的列集合完全一致。
- **schema_meta**：schema.sql 头注释版本、schema_meta 种子行版本、postgres.py
  模块 docstring 声明的 teams schema 版本，三处必须一致（代码里没有数字常量，
  docstring 是唯一的“代码侧期望版本”，见交付报告）。
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path

from icode_data_server.storage import models, postgres

SCHEMA_SQL = postgres._SCHEMA_PATH.read_text(encoding="utf-8")
POSTGRES_SOURCE = Path(postgres.__file__).read_text(encoding="utf-8")
SESSION_COLUMNS = postgres.PostgresRepository._SESSION_COLUMNS

EXPECTED_DDL_TABLES = {
    "schema_meta",
    "sessions",
    "session_turns",
    "event_batches",
    "trajectory_events",
    "object_registry",
    "ingest_receipts",
    "workspace_states",
    "repository_registry",
    "session_anchors",
}
EXPECTED_REFERENCED_TABLES = EXPECTED_DDL_TABLES - {"schema_meta"}

# sessions DDL 中服务端不写入的 teams 范围列（扩列纪律：改 DDL 时保持显式认知）
EXPECTED_SESSION_EXTRAS = [
    "body_checked_at",
    "format_version",
    "generation",
    "schema_version",
    "snapshot_etag",
    "team_id",
    "team_path",
    "team_resolution",
]

CONSTRAINT_HEADS = frozenset({"primary", "unique", "check", "constraint", "foreign", "exclude"})


@dataclass
class TableDef:
    columns: list[str]
    uniques: list[frozenset[str]] = field(default_factory=list)
    definitions: list[str] = field(default_factory=list)


def _strip_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


_CREATE_TABLE_RE = re.compile(
    r"create\s+table\s+(?:if\s+not\s+exists\s+)?([a-z_][a-z0-9_]*)\s*\(", re.IGNORECASE
)
_CREATE_UNIQUE_INDEX_RE = re.compile(
    r"create\s+unique\s+index\s+(?:if\s+not\s+exists\s+)?[a-z_][a-z0-9_]*\s+on\s+"
    r"([a-z_][a-z0-9_]*)\s*\(([^)]*)\)",
    re.IGNORECASE,
)


def _table_body(cleaned: str, open_paren: int) -> str:
    depth = 0
    for index in range(open_paren, len(cleaned)):
        if cleaned[index] == "(":
            depth += 1
        elif cleaned[index] == ")":
            depth -= 1
            if depth == 0:
                return cleaned[open_paren + 1 : index]
    raise AssertionError("schema.sql 表定义括号不配对")


def _split_definitions(body: str) -> list[str]:
    definitions: list[str] = []
    depth = 0
    current: list[str] = []
    for character in body:
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        if character == "," and depth == 0:
            definitions.append("".join(current).strip())
            current = []
        else:
            current.append(character)
    tail = "".join(current).strip()
    if tail:
        definitions.append(tail)
    return [definition for definition in definitions if definition]


def _column_set(text: str) -> frozenset[str]:
    names = []
    for chunk in text.split(","):
        tokens = chunk.strip().split()
        if tokens:
            names.append(tokens[0].strip('"').lower())
    return frozenset(names)


def _parse_tables(sql: str) -> dict[str, TableDef]:
    cleaned = _strip_comments(sql)
    tables: dict[str, TableDef] = {}
    for match in _CREATE_TABLE_RE.finditer(cleaned):
        name = match.group(1).lower()
        body = _table_body(cleaned, match.end() - 1)
        table = TableDef(columns=[], uniques=[], definitions=[])
        for definition in _split_definitions(body):
            table.definitions.append(definition)
            head = definition.split()[0].strip('"').lower()
            if head in CONSTRAINT_HEADS:
                for key in re.finditer(
                    r"(?:primary\s+key|unique)\s*\(([^)]*)\)", definition, re.IGNORECASE
                ):
                    table.uniques.append(_column_set(key.group(1)))
                continue
            table.columns.append(head)
            # 列级内联主键/唯一（如 `component text primary key`）
            if re.search(r"\bprimary\s+key\b|\bunique\b", definition, re.IGNORECASE):
                table.uniques.append(frozenset({head}))
        tables[name] = table
    for match in _CREATE_UNIQUE_INDEX_RE.finditer(cleaned):
        table = tables.get(match.group(1).lower())
        if table is not None:
            table.uniques.append(_column_set(match.group(2)))
    return tables


TABLES = _parse_tables(SCHEMA_SQL)


def _sql_fragments(source: str, session_columns: str) -> list[str]:
    """从 postgres.py 源码提取 SQL 文本。

    相邻字符串字面量（隐式拼接）由 AST 天然合并；`"..." + self._SESSION_COLUMNS
    + "..."` 与 f-string 里的 `{self._SESSION_COLUMNS}` 代入真实列清单文本，
    其它表达式占位为单个空格；只保留以 select/insert/update/delete 开头的片段。
    """
    tree = ast.parse(source)
    fragments: list[str] = []

    def session_columns_attr(node: ast.AST) -> str | None:
        if (
            isinstance(node, ast.Attribute)
            and node.attr == "_SESSION_COLUMNS"
            and isinstance(node.value, ast.Name)
            and node.value.id == "self"
        ):
            return session_columns
        return None

    def reconstruct(node: ast.AST) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        substituted = session_columns_attr(node)
        if substituted is not None:
            return substituted
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = reconstruct(node.left)
            right = reconstruct(node.right)
            if left is not None and right is not None:
                return left + right
            return None
        if isinstance(node, ast.JoinedStr):
            pieces: list[str] = []
            for value in node.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    pieces.append(value.value)
                    continue
                expression = value.value if isinstance(value, ast.FormattedValue) else value
                substituted = session_columns_attr(expression) if expression else None
                pieces.append(substituted if substituted is not None else " ")
            return "".join(pieces)
        return None

    class _Collector(ast.NodeVisitor):
        def visit_BinOp(self, node: ast.BinOp) -> None:
            text = reconstruct(node)
            if text is not None:
                fragments.append(text)
                return
            self.generic_visit(node)

        def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
            fragments.append(reconstruct(node) or "")

        def visit_Constant(self, node: ast.Constant) -> None:
            if isinstance(node.value, str):
                fragments.append(node.value)

    _Collector().visit(tree)
    return [
        fragment
        for fragment in fragments
        # 单词边界匹配，避免把 "updated_at" 这类字典键当成 SQL 片段
        if re.match(r"(?:select|insert|update|delete)[\s(]", fragment.lstrip().lower())
    ]


SQL_FRAGMENTS = _sql_fragments(POSTGRES_SOURCE, SESSION_COLUMNS)

_INSERT_TABLE_RE = re.compile(r"\binsert\s+into\s+([a-z_][a-z0-9_]*)", re.IGNORECASE)
_INSERT_COLUMNS_RE = re.compile(
    r"\binsert\s+into\s+([a-z_][a-z0-9_]*)\s*\(([^)]*)\)", re.IGNORECASE
)
_SELECT_RE = re.compile(
    r"\bselect\s+(.*?)\s+from\s+([a-z_][a-z0-9_]*)\b", re.IGNORECASE | re.DOTALL
)
_FROM_TABLE_RE = re.compile(r"\bfrom\s+([a-z_][a-z0-9_]*)", re.IGNORECASE)
# `(?!set\b)` 排除 ON CONFLICT ... DO UPDATE SET（update 与 set 之间没有表名）
_UPDATE_TABLE_RE = re.compile(r"\bupdate\s+(?!set\b)([a-z_][a-z0-9_]*)\s+set\b", re.IGNORECASE)
_UPDATE_SET_RE = re.compile(
    r"\bupdate\s+(?!set\b)([a-z_][a-z0-9_]*)\s+set\s+(.*?)(?=\bwhere\b|$)",
    re.IGNORECASE | re.DOTALL,
)
_DO_UPDATE_SET_RE = re.compile(
    r"\bdo\s+update\s+set\s+(.*?)(?=\bwhere\b|$)", re.IGNORECASE | re.DOTALL
)
_ON_CONFLICT_RE = re.compile(r"\bon\s+conflict\s*\(([^)]*)\)", re.IGNORECASE)
_SET_TARGET_RE = re.compile(r"(?:^|[\s,])([a-z_][a-z0-9_]*)\s*=", re.IGNORECASE)

_SQL_NOISE = frozenset(
    {
        "and",
        "any",
        "array",
        "as",
        "asc",
        "avg",
        "by",
        "case",
        "coalesce",
        "count",
        "desc",
        "distinct",
        "else",
        "end",
        "excluded",
        "from",
        "greatest",
        "group",
        "insert",
        "into",
        "is",
        "least",
        "max",
        "min",
        "not",
        "now",
        "null",
        "or",
        "order",
        "select",
        "set",
        "sum",
        "then",
        "unnest",
        "update",
        "values",
        "when",
        "where",
    }
)


def _select_identifiers(select_list: str) -> list[str]:
    return [
        token
        for token in re.findall(r"\b([a-z_][a-z0-9_]*)\b", select_list.lower())
        if token not in _SQL_NOISE
    ]


def _set_targets(set_clause: str) -> list[str]:
    return [match.group(1).lower() for match in _SET_TARGET_RE.finditer(set_clause)]


def _state_check_values(table: TableDef) -> frozenset[str] | None:
    for definition in table.definitions:
        match = re.search(r"check\s*\(\s*state\s+in\s*\(([^)]*)\)", definition, re.IGNORECASE)
        if match:
            return frozenset(re.findall(r"'([^']*)'", match.group(1)))
    return None


def _session_columns() -> list[str]:
    return [column.strip() for column in SESSION_COLUMNS.split(",")]


# ── DDL 结构 ─────────────────────────────────────────────────────────────────


def test_ddl_defines_the_expected_table_set() -> None:
    assert set(TABLES) == EXPECTED_DDL_TABLES


def test_sql_references_only_known_tables() -> None:
    referenced: set[str] = set()
    for fragment in SQL_FRAGMENTS:
        for pattern in (_INSERT_TABLE_RE, _FROM_TABLE_RE, _UPDATE_TABLE_RE):
            referenced.update(m.group(1).lower() for m in pattern.finditer(fragment))
    assert referenced == EXPECTED_REFERENCED_TABLES


# ── 列级一致性 ───────────────────────────────────────────────────────────────


def test_insert_select_and_update_columns_exist_in_ddl() -> None:
    problems: list[str] = []
    for fragment in SQL_FRAGMENTS:
        for match in _INSERT_COLUMNS_RE.finditer(fragment):
            table = match.group(1).lower()
            if table in TABLES:
                for column in _column_set(match.group(2)):
                    if column not in TABLES[table].columns:
                        problems.append(f"INSERT {table}.{column}")
        for match in _SELECT_RE.finditer(fragment):
            table = match.group(2).lower()
            if table in TABLES:
                for identifier in _select_identifiers(match.group(1)):
                    if identifier not in TABLES[table].columns:
                        problems.append(f"SELECT {table}.{identifier}")
        for match in _UPDATE_SET_RE.finditer(fragment):
            table = match.group(1).lower()
            if table in TABLES:
                for target in _set_targets(match.group(2)):
                    if target not in TABLES[table].columns:
                        problems.append(f"UPDATE {table}.{target}")
        insert_table = _INSERT_TABLE_RE.search(fragment)
        if insert_table is not None and insert_table.group(1).lower() in TABLES:
            table = insert_table.group(1).lower()
            for match in _DO_UPDATE_SET_RE.finditer(fragment):
                for target in _set_targets(match.group(1)):
                    if target not in TABLES[table].columns:
                        problems.append(f"ON CONFLICT UPDATE {table}.{target}")
    assert not problems, "postgres.py 引用了 DDL 中不存在的列: " + ", ".join(sorted(problems))


def test_session_columns_match_sessions_ddl() -> None:
    """_SESSION_COLUMNS ↔ sessions DDL：同名子集口径（见模块 docstring）。"""
    columns = _session_columns()
    ddl_columns = TABLES["sessions"].columns
    assert len(columns) == 25
    assert len(set(columns)) == len(columns), "_SESSION_COLUMNS 有重复列"
    unknown = [column for column in columns if column not in ddl_columns]
    assert not unknown, f"_SESSION_COLUMNS 引用 sessions DDL 不存在的列: {unknown}"
    extras = sorted(set(ddl_columns) - set(columns))
    assert extras == EXPECTED_SESSION_EXTRAS


def test_upsert_session_placeholders_match_session_columns() -> None:
    fragment = next(f for f in SQL_FRAGMENTS if f.startswith("insert into sessions ("))
    columns = _session_columns()
    insert_columns = _INSERT_COLUMNS_RE.search(fragment)
    assert insert_columns is not None
    assert [c.strip() for c in insert_columns.group(2).split(",")] == columns
    placeholders = {int(number) for number in re.findall(r"\$(\d+)", fragment)}
    assert placeholders == set(range(1, len(columns) + 1))


# ── 状态取值域 ───────────────────────────────────────────────────────────────


def test_model_state_constants_pinned() -> None:
    assert models.TERMINAL_RECEIPT_STATES == frozenset(
        {models.RECEIPT_COMMITTED, models.RECEIPT_REJECTED, models.RECEIPT_UNSUPPORTED}
    )
    assert (
        models.RECEIPT_PENDING,
        models.RECEIPT_COMMITTED,
        models.RECEIPT_REJECTED,
        models.RECEIPT_UNSUPPORTED,
    ) == ("pending", "committed", "rejected", "unsupported")
    assert (
        models.OBJECT_VERIFIED,
        models.OBJECT_PENDING,
        models.OBJECT_MISSING,
        models.OBJECT_SUPERSEDED,
    ) == ("verified", "pending", "missing", "superseded")


def test_sql_state_and_channel_literals_match_model_constants() -> None:
    text = "\n".join(SQL_FRAGMENTS)
    quoted = set(re.findall(r"'([^']+)'", text))
    receipt_channel = models.ReceiptRecord(
        tenant="t", unit_id="u", sha256="s", state=models.RECEIPT_PENDING
    ).channel
    allowed = {receipt_channel} | {
        models.RECEIPT_PENDING,
        models.RECEIPT_COMMITTED,
        models.RECEIPT_REJECTED,
        models.RECEIPT_UNSUPPORTED,
        models.OBJECT_VERIFIED,
        models.OBJECT_PENDING,
        models.OBJECT_MISSING,
        models.OBJECT_SUPERSEDED,
    }
    assert quoted <= allowed, f"SQL 字面量超出模型取值域: {sorted(quoted - allowed)}"
    # put_object / verified_objects 都以 verified 判定；get_receipt 按 report 通道过滤
    assert models.OBJECT_VERIFIED in quoted
    assert receipt_channel in quoted


def test_state_check_constraints_match_model_constants() -> None:
    receipt_values = _state_check_values(TABLES["ingest_receipts"])
    object_values = _state_check_values(TABLES["object_registry"])
    assert receipt_values == frozenset(
        {
            models.RECEIPT_PENDING,
            models.RECEIPT_COMMITTED,
            models.RECEIPT_REJECTED,
            models.RECEIPT_UNSUPPORTED,
        }
    )
    assert object_values == frozenset(
        {
            models.OBJECT_VERIFIED,
            models.OBJECT_PENDING,
            models.OBJECT_MISSING,
            models.OBJECT_SUPERSEDED,
        }
    )


# ── ON CONFLICT 目标 ────────────────────────────────────────────────────────


EXPECTED_ON_CONFLICT_TARGETS = {
    ("ingest_receipts", frozenset({"tenant", "channel", "idempotency_key"})),
    ("object_registry", frozenset({"tenant", "sha256"})),
    ("sessions", frozenset({"tenant", "external_session_id"})),
    ("session_anchors", frozenset({"tenant", "session_id"})),
    ("session_turns", frozenset({"tenant", "session_id", "generation", "turn_number"})),
    ("event_batches", frozenset({"tenant", "batch_id"})),
    ("trajectory_events", frozenset({"tenant", "session_id", "generation", "sequence"})),
    ("workspace_states", frozenset({"tenant", "external_session_id", "repository_identity"})),
    ("repository_registry", frozenset({"tenant", "local_identity"})),
    ("schema_meta", frozenset({"component"})),
}


def test_on_conflict_targets_are_primary_key_or_unique() -> None:
    conflicts: set[tuple[str, frozenset[str]]] = set()
    for fragment in SQL_FRAGMENTS:
        table = _INSERT_TABLE_RE.search(fragment)
        if table is None:
            continue
        for match in _ON_CONFLICT_RE.finditer(fragment):
            conflicts.add((table.group(1).lower(), _column_set(match.group(1))))
    # schema.sql 自身的 schema_meta 种子行也走 on conflict
    for match in re.finditer(
        r"insert\s+into\s+([a-z_][a-z0-9_]*)[^;]*?\bon\s+conflict\s*\(([^)]*)\)",
        SCHEMA_SQL,
        re.IGNORECASE,
    ):
        conflicts.add((match.group(1).lower(), _column_set(match.group(2))))
    assert conflicts == EXPECTED_ON_CONFLICT_TARGETS
    for table, columns in sorted(conflicts, key=lambda item: item[0]):
        assert columns in TABLES[table].uniques, (
            f"ON CONFLICT 目标 {table}({', '.join(sorted(columns))}) 缺少 PK/UNIQUE 支撑"
        )
    # 任务点名的三组目标显式钉住
    assert frozenset({"tenant", "sha256"}) in TABLES["object_registry"].uniques
    assert frozenset({"tenant", "channel", "idempotency_key"}) in TABLES["ingest_receipts"].uniques
    assert frozenset({"tenant", "external_session_id"}) in TABLES["sessions"].uniques


# ── schema_meta 版本 ────────────────────────────────────────────────────────


def test_schema_meta_version_matches_code_expectation() -> None:
    header = re.search(
        r"^--\s*iCode data server schema v(\d+)\s*$",
        SCHEMA_SQL,
        re.IGNORECASE | re.MULTILINE,
    )
    seed = re.search(
        r"insert\s+into\s+schema_meta[^;]*?values\s*\(\s*'([^']*)'\s*,\s*(\d+)\s*,\s*'([^']*)'",
        SCHEMA_SQL,
        re.IGNORECASE,
    )
    docstring = ast.get_docstring(ast.parse(POSTGRES_SOURCE)) or ""
    declared = re.search(r"teams\s+schema\s+v(\d+)", docstring, re.IGNORECASE)
    assert header is not None, "schema.sql 头注释丢失版本声明"
    assert seed is not None, "schema.sql 丢失 schema_meta 种子行"
    assert declared is not None, "postgres.py 模块 docstring 丢失 teams schema 版本声明"
    assert seed.group(1) == "icode-data-server"
    versions = {int(header.group(1)), int(seed.group(2)), int(declared.group(1))}
    assert len(versions) == 1, f"schema 版本声明不一致: {sorted(versions)}"
