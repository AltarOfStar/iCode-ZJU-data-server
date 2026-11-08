"""同构派生器（纯函数，server-acceptance §3.4）。

输入：一个会话按 sequence 排序的事件行（原始字节 + 白名单字段）。
输出：切好的窗口（含 payload 字节、索引行、轮次行）与推进后的派生状态。

规则（§3.4.1 / §3.4.2 / §3.4.3）：
- 代际：会话首个单元 generation=0；`branch_id` 变化（回滚）→ generation+1，切换点前切窗；
- 切窗边界：代际变化点之前、`turn.finished` 行（含该行）、窗口索引行 ≥ 200、窗口 payload > 4 MiB；
- 只消费「从窗口起点开始连续、或被 `trajectory.gap` 声明覆盖」的前缀——这样同一行集合的两种
  到达顺序会收敛到同一窗口集合（乱序不受到达时序影响）；
- 窗口 payload = 窗口内各行原样字节按 `\\n` 连接（含尾 `\\n`），`sha256(payload)` 即对象键。

本模块不做任何 IO，全部可直接表驱动单测。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

MAX_INDEX_LINES = 200
MAX_WINDOW_BYTES = 4 * 1024 * 1024

TURN_STARTED = "turn.started"
TURN_FINISHED = "turn.finished"
TRAJECTORY_GAP = "trajectory.gap"
ROLLBACK_EVENTS = frozenset({"session.rollback", "branch.superseded"})


@dataclass(frozen=True, slots=True)
class TrajectoryLine:
    """一行原始事件：字节即真相，白名单字段只用于派生。"""

    raw: bytes
    sequence: int
    event_type: str
    document: Mapping[str, Any]

    @property
    def event_id(self) -> str | None:
        return _text(self.document.get("event_id"))

    @property
    def turn_id(self) -> str | None:
        return _text(self.document.get("turn_id"))

    @property
    def branch_id(self) -> str | None:
        return _text(self.document.get("branch_id"))

    @property
    def occurred_at(self) -> str | None:
        return _text(self.document.get("occurred_at"))

    @property
    def payload(self) -> Mapping[str, Any]:
        payload = self.document.get("payload")
        return payload if isinstance(payload, Mapping) else {}


@dataclass(frozen=True, slots=True)
class EventIndexRow:
    """`trajectory_events` 的白名单行（§3.4.3，与 teams spec 01 §5.2 逐项一致）。"""

    sequence: int
    event_type: str
    event_id: str | None = None
    turn_id: str | None = None
    operation_id: str | None = None
    actor_kind: str | None = None
    actor_role: str | None = None
    occurred_at: str | None = None
    monotonic_ns: int | None = None
    tool_name: str | None = None
    tool_kind: str | None = None
    duration_ms: int | None = None
    outcome: str | None = None
    output_tokens: int | None = None
    input_tokens: int | None = None
    # 轮次派生所需（不落 trajectory_events 列，供 session_turns 使用）
    turn_number: int | None = None
    end_reason: str | None = None


@dataclass(frozen=True, slots=True)
class TurnIndexRow:
    """`session_turns` 行（派生自窗口内的 turn.started / turn.finished）。"""

    turn_number: int
    turn_id: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    duration_ms: int | None = None
    end_reason: str | None = None


@dataclass(frozen=True, slots=True)
class DerivedWindow:
    generation: int
    first_sequence: int
    last_sequence: int
    payload: bytes
    payload_sha256: str
    event_count: int
    unparsed_count: int
    gap_count: int
    completeness: str
    events: tuple[EventIndexRow, ...]
    turns: tuple[TurnIndexRow, ...]


@dataclass(frozen=True, slots=True)
class DerivationState:
    generation: int = 0
    branch_id: str | None = None
    next_sequence: int = 0


@dataclass(frozen=True, slots=True)
class Derivation:
    windows: tuple[DerivedWindow, ...]
    consumed: int
    state: DerivationState
    gaps: tuple[tuple[int, int], ...]


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _integer(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def parse_line(raw: bytes) -> TrajectoryLine:
    """解析一行事件；结构性缺陷抛 `ValueError`（由调用方转成 schema_invalid）。"""
    text = raw[:-1] if raw.endswith(b"\n") else raw
    try:
        document = json.loads(text)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(f"event line is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError("event line must be a JSON object")
    sequence = _integer(document.get("sequence"))
    if sequence is None:
        raise ValueError("event line has no integer sequence")
    event_type = _text(document.get("event_type"))
    if event_type is None:
        raise ValueError("event line has no event_type")
    return TrajectoryLine(raw=text, sequence=sequence, event_type=event_type, document=document)


def split_payload(payload: bytes) -> list[TrajectoryLine]:
    """按 `\\n` 拆行；末尾无 `\\n` 的 torn line 直接拒收（不做善意补全，§3.3-4）。"""
    if not payload:
        return []
    if not payload.endswith(b"\n"):
        raise ValueError("trajectory.lines payload ends with a torn line")
    return [parse_line(chunk) for chunk in payload.split(b"\n") if chunk]


def merge_ranges(ranges: Sequence[tuple[int, int]]) -> tuple[tuple[int, int], ...]:
    cleaned = sorted((min(a, b), max(a, b)) for a, b in ranges if a is not None and b is not None)
    merged: list[tuple[int, int]] = []
    for start, end in cleaned:
        if merged and start <= merged[-1][1] + 1:
            previous_start, previous_end = merged[-1]
            merged[-1] = (previous_start, max(previous_end, end))
        else:
            merged.append((start, end))
    return tuple(merged)


def declared_gaps(lines: Sequence[TrajectoryLine]) -> tuple[tuple[int, int], ...]:
    """收集 `trajectory.gap` 行**权威声明**的空洞区间（`payload.lost_sequences`）。"""
    ranges: list[tuple[int, int]] = []
    for line in lines:
        if line.event_type != TRAJECTORY_GAP:
            continue
        lost = line.payload.get("lost_sequences")
        if not isinstance(lost, list) or not lost:
            continue
        numbers = [item for item in lost if isinstance(item, int) and not isinstance(item, bool)]
        if len(numbers) != len(lost):
            continue
        ranges.append((min(numbers), max(numbers)))
    return merge_ranges(ranges)


def is_covered(ranges: Sequence[tuple[int, int]], start: int, end: int) -> bool:
    if end < start:
        return True
    cursor = start
    for range_start, range_end in ranges:
        if range_end < cursor:
            continue
        if range_start > cursor:
            return False
        cursor = range_end + 1
        if cursor > end:
            return True
    return cursor > end


def extract_event_row(line: TrajectoryLine) -> EventIndexRow | None:
    """白名单抽取：缺键即省略，不用默认值冒充（teams `extractEventRow` 同口径）。"""
    event_id = _text(line.document.get("event_id"))
    if event_id is None:
        return None
    payload = line.payload
    actor = line.document.get("actor")
    actor_map = actor if isinstance(actor, Mapping) else {}
    usage = payload.get("usage")
    normalized = usage.get("normalized") if isinstance(usage, Mapping) else None
    normalized_map = normalized if isinstance(normalized, Mapping) else {}
    return EventIndexRow(
        sequence=line.sequence,
        event_type=line.event_type,
        event_id=event_id,
        turn_id=line.turn_id,
        operation_id=_text(line.document.get("operation_id")),
        actor_kind=_text(actor_map.get("kind")),
        actor_role=_text(actor_map.get("role")),
        occurred_at=line.occurred_at,
        monotonic_ns=_integer(line.document.get("monotonic_ns")),
        tool_name=_text(payload.get("tool_name")),
        tool_kind=_text(payload.get("tool_kind")),
        duration_ms=_integer(payload.get("duration_ms")),
        outcome=_text(payload.get("outcome")),
        output_tokens=_integer(normalized_map.get("output_total")),
        input_tokens=_integer(normalized_map.get("input_total")),
        turn_number=_integer(payload.get("turn_number")),
        end_reason=_text(payload.get("end_reason")),
    )


def extract_turns(rows: Sequence[EventIndexRow]) -> tuple[TurnIndexRow, ...]:
    """从窗口内的 turn.started / turn.finished 行派生轮次（不回放全量历史）。"""
    turns: dict[int, dict[str, Any]] = {}
    fallback = 0
    for row in rows:
        if row.event_type not in {TURN_STARTED, TURN_FINISHED}:
            continue
        if row.turn_number is not None:
            number = row.turn_number
        else:
            fallback += 1
            number = fallback
        current = turns.setdefault(
            number,
            {
                "turn_number": number,
                "turn_id": None,
                "started_at": None,
                "ended_at": None,
                "duration_ms": None,
                "end_reason": None,
            },
        )
        if row.turn_id:
            current["turn_id"] = row.turn_id
        if row.event_type == TURN_STARTED:
            current["started_at"] = row.occurred_at or current["started_at"]
        else:
            current["ended_at"] = row.occurred_at or current["ended_at"]
            if row.duration_ms is not None:
                current["duration_ms"] = row.duration_ms
            if row.end_reason:
                current["end_reason"] = row.end_reason
    return tuple(TurnIndexRow(**turns[number]) for number in sorted(turns))


class _Accumulator:
    __slots__ = (
        "generation",
        "start_index",
        "first",
        "last",
        "raws",
        "size",
        "rows",
        "gaps",
        "unparsed",
        "hole",
    )

    def __init__(self, generation: int, start_index: int, sequence: int) -> None:
        self.generation = generation
        self.start_index = start_index
        self.first = sequence
        self.last = sequence
        self.raws: list[bytes] = []
        self.size = 0
        self.rows: list[EventIndexRow] = []
        self.gaps = 0
        self.unparsed = 0
        self.hole = False

    def add(self, line: TrajectoryLine) -> None:
        self.last = line.sequence
        self.raws.append(line.raw)
        self.size += len(line.raw) + 1
        if line.event_type == TRAJECTORY_GAP:
            self.gaps += 1
        row = extract_event_row(line)
        if row is None:
            self.unparsed += 1
        else:
            self.rows.append(row)


def _close(acc: _Accumulator) -> DerivedWindow:
    payload = b"".join(raw + b"\n" for raw in acc.raws)
    complete = acc.unparsed == 0 and acc.gaps == 0 and not acc.hole
    return DerivedWindow(
        generation=acc.generation,
        first_sequence=acc.first,
        last_sequence=acc.last,
        payload=payload,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        event_count=len(acc.rows),
        unparsed_count=acc.unparsed,
        gap_count=acc.gaps,
        completeness="complete" if complete else "gapped",
        events=tuple(acc.rows),
        turns=extract_turns(acc.rows),
    )


def derive_windows(
    lines: Sequence[TrajectoryLine],
    *,
    state: DerivationState | None = None,
    gaps: Sequence[tuple[int, int]] = (),
    max_index_lines: int = MAX_INDEX_LINES,
    max_window_bytes: int = MAX_WINDOW_BYTES,
) -> Derivation:
    """把有序行流切成窗口；只消费可从当前水位连续推进的前缀。"""
    state = state or DerivationState()
    lines = sorted(lines, key=lambda line: line.sequence)
    covered = merge_ranges((*(tuple(gaps)), *declared_gaps(lines)))
    generation = state.generation
    branch = state.branch_id
    expected = state.next_sequence
    windows: list[DerivedWindow] = []
    acc: _Accumulator | None = None
    # 最后一次成功切窗处的状态：未闭合的尾部不推进水位（下次重放同一批行）。
    mark = (generation, branch, expected)
    stopped_at: int | None = None

    for index, line in enumerate(lines):
        if expected and line.sequence < expected:
            continue
        hole = False
        if expected and line.sequence > expected:
            if not is_covered(covered, expected, line.sequence - 1):
                # 空洞未被 trajectory.gap 声明覆盖：停止消费，后续行留在缓冲区等待补齐。
                stopped_at = index
                break
            hole = True
        if line.branch_id and branch is None:
            branch = line.branch_id
        elif line.branch_id and branch is not None and line.branch_id != branch:
            if acc is not None:
                windows.append(_close(acc))
                acc = None
            generation += 1
            branch = line.branch_id
        if acc is None:
            acc = _Accumulator(generation=generation, start_index=index, sequence=line.sequence)
        if hole:
            acc.hole = True
        acc.add(line)
        expected = line.sequence + 1
        if (
            line.event_type == TURN_FINISHED
            or len(acc.raws) >= max_index_lines
            or acc.size > max_window_bytes
        ):
            windows.append(_close(acc))
            acc = None
            mark = (generation, branch, expected)

    if acc is not None:
        consumed = acc.start_index
        final = DerivationState(generation=mark[0], branch_id=mark[1], next_sequence=mark[2])
    elif stopped_at is not None:
        consumed = stopped_at
        final = DerivationState(generation=mark[0], branch_id=mark[1], next_sequence=mark[2])
    else:
        consumed = len(lines)
        final = DerivationState(generation=generation, branch_id=branch, next_sequence=expected)
    return Derivation(
        windows=tuple(windows),
        consumed=consumed,
        state=final,
        gaps=covered,
    )
