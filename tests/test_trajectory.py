"""派生器：窗口切分、白名单抽取、turns、代际与乱序收敛（server-acceptance §3.4 / §7 T10-T13）。"""

from __future__ import annotations

import json

from icode_data_server.ingest.derive import (
    DerivationState,
    derive_windows,
    extract_event_row,
    extract_turns,
    parse_line,
    split_payload,
)

from .helpers import SESSION, TENANT, trajectory_payload, unit


def _lines(specs: list[tuple[int, str, dict, str]]) -> list:
    result = []
    for sequence, event_type, payload, branch in specs:
        document = {
            "schema_version": 1,
            "event_id": f"e{sequence}",
            "sequence": sequence,
            "event_type": event_type,
            "branch_id": branch,
            "turn_id": "t1",
            "occurred_at": "2026-09-09T08:00:00Z",
            "monotonic_ns": sequence * 1000,
            "actor": {"kind": "agent", "role": "main"},
            "payload": payload,
        }
        result.append(parse_line(json.dumps(document).encode()))
    return result


def test_turn_finished_closes_the_window() -> None:
    lines = _lines(
        [
            (1, "turn.started", {"turn_number": 1}, "b1"),
            (2, "tool.operation.started", {"tool_name": "write_file", "tool_kind": "write"}, "b1"),
            (3, "turn.finished", {"turn_number": 1, "duration_ms": 1200}, "b1"),
            (4, "turn.started", {"turn_number": 2}, "b1"),
        ]
    )
    result = derive_windows(lines)
    assert len(result.windows) == 1
    window = result.windows[0]
    assert (window.first_sequence, window.last_sequence) == (1, 3)
    assert window.completeness == "complete"
    assert window.payload == b"".join(line.raw + b"\n" for line in lines[:3])
    assert window.turns[0].turn_number == 1
    assert window.turns[0].duration_ms == 1200
    assert window.events[1].tool_name == "write_file"
    # 第 4 行未闭合：不消费，留在缓冲区
    assert result.consumed == 3
    assert result.state.next_sequence == 4


def test_branch_change_starts_a_new_generation_without_mixing() -> None:
    lines = _lines(
        [
            (1, "turn.started", {"turn_number": 1}, "b1"),
            (2, "turn.finished", {"turn_number": 1}, "b1"),
            (3, "session.rollback", {"new_branch_id": "b2", "old_branch_id": "b1"}, "b1"),
            (4, "turn.started", {"turn_number": 2}, "b2"),
            (5, "turn.finished", {"turn_number": 2}, "b2"),
        ]
    )
    result = derive_windows(lines)
    # 切换点前切窗：回滚行仍属于旧代际的窗口，之后的窗口落在 generation+1。
    assert [(w.generation, w.first_sequence, w.last_sequence) for w in result.windows] == [
        (0, 1, 2),
        (0, 3, 3),
        (1, 4, 5),
    ]


def test_reindex_lines_are_derived_per_generation() -> None:
    first = _lines(
        [
            (1, "turn.started", {"turn_number": 1}, "b1"),
            (2, "turn.finished", {"turn_number": 1}, "b1"),
        ]
    )
    second = _lines(
        [
            (3, "session.rollback", {"new_branch_id": "b2"}, "b1"),
            (4, "turn.started", {"turn_number": 2}, "b2"),
            (5, "turn.finished", {"turn_number": 2}, "b2"),
        ]
    )
    derivation = derive_windows(first)
    assert derivation.state.generation == 0
    follow = derive_windows(
        [line for line in second],
        state=DerivationState(
            generation=derivation.state.generation,
            branch_id=derivation.state.branch_id,
            next_sequence=derivation.state.next_sequence,
        ),
        gaps=derivation.gaps,
    )
    assert [(w.generation, w.first_sequence) for w in follow.windows] == [(0, 3), (1, 4)]


def test_window_cap_splits_at_index_limit() -> None:
    lines = _lines(
        [
            (sequence, "tool.operation.finished", {"tool_name": "x"}, "b1")
            for sequence in range(1, 261)
        ]
    )
    result = derive_windows(lines, max_index_lines=100)
    assert len(result.windows) == 2
    assert [(w.first_sequence, w.last_sequence) for w in result.windows] == [(1, 100), (101, 200)]
    assert result.consumed == 200


def test_gap_rows_mark_window_gapped_and_allow_progress() -> None:
    lines = _lines(
        [
            (1, "turn.started", {"turn_number": 1}, "b1"),
            (5, "trajectory.gap", {"lost_sequences": [2, 4]}, "b1"),
            (6, "turn.finished", {"turn_number": 1}, "b1"),
        ]
    )
    result = derive_windows(lines)
    assert len(result.windows) == 1
    window = result.windows[0]
    assert (window.first_sequence, window.last_sequence) == (1, 6)
    assert window.gap_count == 1
    assert window.completeness == "gapped"


def test_uncovered_hole_stops_the_prefix() -> None:
    lines = _lines(
        [
            (1, "turn.started", {"turn_number": 1}, "b1"),
            (5, "turn.finished", {"turn_number": 1}, "b1"),
        ]
    )
    result = derive_windows(lines)
    assert result.windows == ()
    assert result.consumed == 0


def test_whitelist_extraction_ignores_unknown_fields() -> None:
    line = parse_line(
        json.dumps(
            {
                "sequence": 7,
                "event_id": "e7",
                "event_type": "tool.operation.finished",
                "turn_id": "t1",
                "operation_id": "op1",
                "occurred_at": "2026-09-09T08:00:00Z",
                "monotonic_ns": 7000,
                "actor": {"kind": "agent", "role": "sub"},
                "payload": {
                    "tool_name": "read_file",
                    "tool_kind": "read",
                    "duration_ms": 12,
                    "outcome": "ok",
                    "usage": {"normalized": {"input_total": 10, "output_total": 20}},
                    "content": "正文不在白名单里",
                },
            }
        ).encode()
    )
    row = extract_event_row(line)
    assert row is not None
    assert (row.actor_kind, row.actor_role) == ("agent", "sub")
    assert (row.input_tokens, row.output_tokens) == (10, 20)
    assert not hasattr(row, "content")


def test_turns_accumulate_by_turn_number() -> None:
    rows = _lines(
        [
            (1, "turn.started", {"turn_number": 1}, "b1"),
            (2, "turn.finished", {"turn_number": 1, "duration_ms": 5, "end_reason": "done"}, "b1"),
            (3, "turn.started", {"turn_number": 2}, "b1"),
        ]
    )
    turns = extract_turns([extract_event_row(line) for line in rows])
    assert [turn.turn_number for turn in turns] == [1, 2]
    assert turns[0].duration_ms == 5
    assert turns[0].end_reason == "done"
    assert turns[1].ended_at is None


def test_split_payload_rejects_torn_line() -> None:
    import pytest

    with pytest.raises(ValueError):
        split_payload(b'{"sequence": 1, "event_type": "turn.started"}')


# ── 端点级：乱序 / 冲突 / 重建 ────────────────────────────────────────────────


async def _send(client, lines: list[bytes], *, reset: bool = False, first: int = 1, last: int = 0):
    payload = trajectory_payload(lines)
    scope = f"lines:{first}-{last or len(lines)}"
    return (
        await client.post_batch(
            unit("trajectory.lines", scope, payload, session_id=SESSION, reset=reset)
        )
    ).json()


async def test_out_of_order_frames_converge_to_same_windows(
    client, custom_client, event_line
) -> None:
    lines = [
        event_line(1, "turn.started", payload={"turn_number": 1}),
        event_line(2, "tool.operation.finished", payload={"tool_name": "a"}),
        event_line(3, "turn.finished", payload={"turn_number": 1}),
        event_line(4, "turn.started", payload={"turn_number": 2}),
        event_line(5, "turn.finished", payload={"turn_number": 2}),
    ]

    ordered = await custom_client()
    await ordered.post_batch(
        unit("trajectory.lines", "lines:1-5", trajectory_payload(lines), session_id=SESSION)
    )
    batches_a = sorted(
        ordered.repository.batches.values(), key=lambda row: row.first_event_sequence
    )
    await ordered.app.state.services.close()

    shuffled = await custom_client()
    await shuffled.post_batch(
        unit("trajectory.lines", "lines:4-5", trajectory_payload(lines[3:]), session_id=SESSION)
    )
    await shuffled.post_batch(
        unit("trajectory.lines", "lines:1-3", trajectory_payload(lines[:3]), session_id=SESSION)
    )
    batches_b = sorted(
        shuffled.repository.batches.values(), key=lambda row: row.first_event_sequence
    )

    assert [
        (row.first_event_sequence, row.last_event_sequence, row.payload_sha256) for row in batches_a
    ] == [
        (row.first_event_sequence, row.last_event_sequence, row.payload_sha256) for row in batches_b
    ]
    assert len(batches_a) == 2


async def test_a_committed_window_records_the_object_etag(custom_client, event_line) -> None:
    """`object_registry.etag` and `event_batches.payload_etag` carry the store's ETag."""
    api = await custom_client()
    lines = [event_line(1, "turn.finished", payload={"turn_number": 1})]

    await api.post_batch(
        unit("trajectory.lines", "lines:1-1", trajectory_payload(lines), session_id=SESSION)
    )

    (batch,) = api.repository.batches.values()
    stored = await api.repository.get_object(TENANT, batch.payload_sha256)
    assert stored is not None
    assert stored.etag
    assert batch.payload_etag == stored.etag


async def test_same_sequence_different_content_conflicts(client, event_line) -> None:
    first = await _send(client, [event_line(1, "turn.started", payload={"turn_number": 1})])
    assert first["accepted"], first

    conflicting = await _send(client, [event_line(1, "turn.started", payload={"turn_number": 99})])
    assert conflicting["rejected"][0]["code"] == "sequence_conflict"


async def test_reset_with_new_content_bumps_generation(client, event_line) -> None:
    await _send(
        client,
        [
            event_line(1, "turn.started", payload={"turn_number": 1}),
            event_line(2, "turn.finished", payload={"turn_number": 1}),
        ],
    )
    response = await _send(
        client,
        [
            event_line(1, "turn.started", payload={"turn_number": 2}),
            event_line(2, "tool.operation.finished", payload={"tool_name": "read_file"}),
            event_line(3, "turn.finished", payload={"turn_number": 2}),
        ],
        reset=True,
        first=1,
        last=3,
    )
    assert response["accepted"], response
    state = client.services.trajectory.state_for("dev", SESSION)
    assert state.generation == 1
    events = await client.repository.list_events("dev", SESSION)
    assert {row.generation for row in events} == {0, 1}


async def test_reset_with_identical_content_is_a_pure_duplicate(client, event_line) -> None:
    frames = [
        event_line(1, "turn.started", payload={"turn_number": 1}),
        event_line(2, "turn.finished", payload={"turn_number": 1}),
    ]
    first = await _send(client, frames)
    assert first["accepted"], first
    again = await _send(client, frames, reset=True)
    assert again["duplicates"], again
    state = client.services.trajectory.state_for("dev", SESSION)
    assert state.generation == 0


async def test_torn_line_is_rejected(client) -> None:
    payload = b'{"sequence": 1, "event_type": "turn.started"}'
    response = (
        await client.post_batch(unit("trajectory.lines", "lines:1-1", payload, session_id=SESSION))
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "torn" in response["rejected"][0]["message"]


async def test_turns_are_persisted_per_generation(client, event_line) -> None:
    await _send(
        client,
        [
            event_line(1, "turn.started", payload={"turn_number": 1}),
            event_line(2, "turn.finished", payload={"turn_number": 1, "duration_ms": 33}),
        ],
    )
    turns = await client.repository.list_turns("dev", SESSION)
    assert len(turns) == 1
    assert turns[0].turn_number == 1
    assert turns[0].duration_ms == 33
    assert turns[0].generation == 0


async def test_window_object_is_written_to_content_addressed_store(client, event_line) -> None:
    lines = [
        event_line(1, "turn.started", payload={"turn_number": 1}),
        event_line(2, "turn.finished", payload={"turn_number": 1}),
    ]
    await _send(client, lines)
    batches = sorted(client.repository.batches.values(), key=lambda row: row.first_event_sequence)
    assert len(batches) == 1
    stored = await client.object_store.get("dev", batches[0].payload_sha256)
    assert stored == trajectory_payload(lines)
    assert batches[0].completeness_state == "complete"
    assert batches[0].event_count == 2
