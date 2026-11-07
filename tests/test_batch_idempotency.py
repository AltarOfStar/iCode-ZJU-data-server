"""批量幂等、拒绝码与隔离（server-acceptance §2.2 / §3.1 / §7 T1、T6、T17）。"""

from __future__ import annotations

import json
import struct

from icode_data_server.frames import encode_frame

from .helpers import (
    SESSION,
    agent_env_payload,
    batch,
    repository_payload,
    sha,
    unit,
    unit_id,
)


def _tamper_payload(frame: bytes) -> bytes:
    (header_len,) = struct.unpack_from(">I", frame, 0)
    offset = 4 + header_len
    return frame[:offset] + bytes([frame[offset] ^ 0xFF]) + frame[offset + 1 :]


async def test_same_batch_three_times_is_idempotent(client) -> None:
    payload = repository_payload()
    frame = unit("repo.identity", "repo:repo-identity-1", payload)
    body = batch(frame)
    expected = unit_id("repo.identity", "repo:repo-identity-1", payload)

    first = (await client.post_batch(body)).json()
    assert first["accepted"] == [expected]
    assert first["rejected"] == []

    second = (await client.post_batch(body)).json()
    third = (await client.post_batch(body)).json()
    assert second["duplicates"] == first["accepted"]
    assert third["duplicates"] == first["accepted"]
    assert second["accepted"] == [] and third["accepted"] == []

    assert len(client.repository.receipts) == 1
    assert len(client.repository.repositories) == 1


async def test_partition_covers_every_unit(client) -> None:
    good = unit("repo.identity", "repo:repo-identity-1", repository_payload())
    tampered = _tamper_payload(unit("repo.identity", "repo:x", repository_payload()))
    bad_unit_id = unit("repo.identity", "repo:y", repository_payload(), unit_id="0" * 64)
    response = (await client.post_batch(batch(good, tampered, bad_unit_id))).json()

    seen = (
        response["accepted"]
        + response["duplicates"]
        + response["deferred"]
        + [item["unit_id"] for item in response["rejected"]]
    )
    assert len(seen) == 3
    assert len(set(seen)) == 3
    assert [item["code"] for item in response["rejected"]] == [
        "payload_sha_mismatch",
        "schema_invalid",
    ]


async def test_payload_sha_mismatch_is_rejected_and_not_stored(client) -> None:
    tampered = _tamper_payload(unit("repo.identity", "repo:repo-identity-1", repository_payload()))
    response = (await client.post_batch(tampered)).json()
    assert response["rejected"][0]["code"] == "payload_sha_mismatch"
    # 终态拒绝留证：收据记 rejected，但目标表没有任何副作用。
    receipts = list(client.repository.receipts.values())
    assert len(receipts) == 1 and receipts[0].state == "rejected"
    assert client.repository.repositories == {}


async def test_unsupported_kind_is_quarantined(client) -> None:
    payload = b"opaque"
    frame = unit("control.heartbeat", "ctrl:1", payload)
    response = (await client.post_batch(frame)).json()
    assert response["rejected"][0]["code"] == "unsupported_kind"
    assert await client.object_store.exists("dev", sha(payload))
    receipt = next(iter(client.repository.receipts.values()))
    assert receipt.state == "unsupported"


async def test_unsupported_frame_version_is_rejected(client) -> None:
    frame = unit("repo.identity", "repo:repo-identity-1", repository_payload(), v=2)
    response = (await client.post_batch(frame)).json()
    assert response["rejected"][0]["code"] == "unsupported_version"


async def test_agent_env_resend_is_a_duplicate(client) -> None:
    payload = agent_env_payload()
    body = unit("agent.env", f"agentenv:{sha(payload)}", payload, session_id=SESSION)
    first = (await client.post_batch(body)).json()
    second = (await client.post_batch(body)).json()
    assert len(first["accepted"]) == 1
    assert len(second["duplicates"]) == 1
    stored = await client.object_store.get("dev", sha(payload))
    assert stored == payload


async def test_unknown_session_manifest_does_not_leak(client) -> None:
    response = await client.post_manifest(
        "does-not-exist",
        {
            "snapshot": {"content_hash": "a" * 64, "chunk_hashes": ["b" * 64]},
            "workspace": {"manifest_sha256": "c" * 64, "file_shas": ["d" * 64]},
        },
    )
    body = response.json()
    assert response.status_code == 200
    assert body["known_snapshot"] is False
    assert body["acked_last_sequence"] == 0
    assert body["missing_chunk_hashes"] == ["b" * 64]
    assert body["missing_file_shas"] == ["d" * 64]


async def test_trailing_garbage_after_valid_frames_is_a_format_error(client) -> None:
    good = encode_frame({"kind": "repo.identity", "scope": "repo:repo-identity-1"}, b"{}")
    response = await client.post_batch(good + b"\x00\x00")
    assert response.status_code == 400
    assert client.repository.receipts == {}
    assert json.dumps(response.json())  # 响应体是 JSON
