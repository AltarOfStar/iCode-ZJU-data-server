"""缺块协商端点（server-acceptance §2.3 / §7 T5）。"""

from __future__ import annotations

from .helpers import (
    SESSION,
    chunk_payloads,
    sha,
    snapshot_bytes,
    snapshot_frame,
    workspace_file_frames,
    workspace_manifest,
)


async def test_missing_chunks_before_snapshot_completes(client) -> None:
    content = snapshot_bytes(600 * 1024)
    content_hash = sha(content)
    chunks = chunk_payloads(content)
    hashes = [sha(chunk) for chunk in chunks]

    # 只送第 0 块
    await client.post_batch(snapshot_frame(0, chunks[0], content_hash))

    response = (
        await client.post_manifest(
            SESSION,
            {
                "snapshot": {"content_hash": content_hash, "chunk_hashes": hashes},
                "workspace": {"manifest_sha256": "c" * 64, "file_shas": []},
            },
        )
    ).json()
    assert response["known_snapshot"] is False
    assert response["missing_chunk_hashes"] == hashes[1:]

    # 补齐后：没有缺块
    for index in range(1, len(chunks)):
        await client.post_batch(snapshot_frame(index, chunks[index], content_hash))
    response = (
        await client.post_manifest(
            SESSION,
            {
                "snapshot": {"content_hash": content_hash, "chunk_hashes": hashes},
                "workspace": {"manifest_sha256": "c" * 64, "file_shas": []},
            },
        )
    ).json()
    assert response["known_snapshot"] is True
    assert response["missing_chunk_hashes"] == []


async def test_missing_file_shas(client) -> None:
    payload, contents = workspace_manifest(files=[("a.txt", b"alpha"), ("b.txt", b"beta")])
    frames = workspace_file_frames(contents)
    await client.post_batch(frames[0])  # 只送第一个文件

    present = contents[0][0]
    absent = contents[1][0]
    response = (
        await client.post_manifest(
            SESSION,
            {
                "snapshot": {"content_hash": "a" * 64, "chunk_hashes": []},
                "workspace": {"manifest_sha256": sha(payload), "file_shas": [present, absent]},
            },
        )
    ).json()
    assert response["missing_file_shas"] == [absent]


async def test_acked_last_sequence_tracks_committed_windows(client, event_line) -> None:
    from .helpers import SESSION as SID
    from .helpers import trajectory_payload, unit

    lines = [
        event_line(1, "turn.started", payload={"turn_number": 1}),
        event_line(2, "tool.operation.finished", payload={"tool_name": "read_file"}),
        event_line(3, "turn.finished", payload={"turn_number": 1, "duration_ms": 42}),
    ]
    payload = trajectory_payload(lines)
    await client.post_batch(unit("trajectory.lines", "lines:1-3", payload, session_id=SID))

    response = (
        await client.post_manifest(
            SID,
            {
                "snapshot": {"content_hash": "a" * 64, "chunk_hashes": []},
                "workspace": {"manifest_sha256": "b" * 64, "file_shas": []},
            },
        )
    ).json()
    assert response["acked_last_sequence"] == 3


async def test_manifest_is_read_only(client) -> None:
    before_receipts = dict(client.repository.receipts)
    before_objects = dict(client.repository.objects)
    await client.post_manifest(
        SESSION,
        {
            "snapshot": {"content_hash": "a" * 64, "chunk_hashes": ["b" * 64]},
            "workspace": {"manifest_sha256": "c" * 64, "file_shas": ["d" * 64]},
        },
    )
    assert client.repository.receipts == before_receipts
    assert client.repository.objects == before_objects


async def test_malformed_manifest_request_is_400(client) -> None:
    response = await client.post_manifest(SESSION, {"snapshot": {"chunk_hashes": [1, 2]}})
    assert response.status_code == 400
    assert response.json()["error"] == "manifest_format"
