"""快照逐字节重组与缺块（server-acceptance §3.2 / §7 T2）。"""

from __future__ import annotations

import json

from .helpers import (
    SESSION,
    batch,
    chunk_payloads,
    session_meta_payload,
    sha,
    snapshot_bytes,
    snapshot_frame,
    snapshot_manifest_payload,
    unit,
)


async def test_chunks_out_of_order_are_reassembled_byte_exact(client) -> None:
    content = snapshot_bytes(700 * 1024)
    content_hash = sha(content)
    chunks = chunk_payloads(content)
    assert len(chunks) == 3

    # 乱序：2 → 0 → 1
    for index in (2, 0, 1):
        response = (
            await client.post_batch(snapshot_frame(index, chunks[index], content_hash))
        ).json()
        assert len(response["accepted"]) == 1

    stored = await client.object_store.get("dev", content_hash)
    assert stored == content
    record = await client.repository.get_object("dev", content_hash)
    assert record is not None and record.state == "verified"
    assert record.size_bytes == len(content)


async def test_incomplete_chunks_are_not_registered(client) -> None:
    content = snapshot_bytes(600 * 1024)
    content_hash = sha(content)
    chunks = chunk_payloads(content)

    response = (
        await client.post_batch(
            batch(
                snapshot_frame(0, chunks[0], content_hash),
                snapshot_frame(2, chunks[2], content_hash),
            )
        )
    ).json()
    assert len(response["accepted"]) == 2
    assert await client.repository.get_object("dev", content_hash) is None
    assert not await client.object_store.exists("dev", content_hash)

    # 补发缺块 → 组装完成
    await client.post_batch(snapshot_frame(1, chunks[1], content_hash))
    assert await client.object_store.get("dev", content_hash) == content


async def test_manifest_chunk_count_must_match_size(client) -> None:
    content = snapshot_bytes(1024)
    payload = json.loads(snapshot_manifest_payload(content))
    payload["chunks"] = payload["chunks"] * 3
    frame = unit(
        "snapshot.manifest",
        f"manifest:{sha(content)}",
        json.dumps(payload).encode(),
        session_id=SESSION,
    )
    response = (await client.post_batch(frame)).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "chunk count" in response["rejected"][0]["message"]


async def test_manifest_scope_must_match_content_hash(client) -> None:
    content = snapshot_bytes(1024)
    frame = unit(
        "snapshot.manifest",
        f"manifest:{'a' * 64}",
        snapshot_manifest_payload(content),
        session_id=SESSION,
    )
    response = (await client.post_batch(frame)).json()
    assert response["rejected"][0]["code"] == "schema_invalid"


async def test_snapshot_manifest_arriving_before_chunks_still_assembles(client) -> None:
    content = snapshot_bytes(600 * 1024)
    content_hash = sha(content)
    chunks = chunk_payloads(content)

    await client.post_batch(
        unit(
            "snapshot.manifest",
            f"manifest:{content_hash}",
            snapshot_manifest_payload(content),
            session_id=SESSION,
        )
    )
    for index, chunk in enumerate(chunks):
        await client.post_batch(snapshot_frame(index, chunk, content_hash))
    assert await client.object_store.get("dev", content_hash) == content


async def test_missing_assembly_ttl_marks_registry_and_heals(custom_client) -> None:
    client = await custom_client(assembly_ttl_s=0.0)
    content = snapshot_bytes(600 * 1024)
    content_hash = sha(content)
    chunks = chunk_payloads(content)

    # 只送第 0 块 → 组装未完成
    await client.post_batch(snapshot_frame(0, chunks[0], content_hash))
    # 任何后续批次都会触发清扫（sweep interval = 0）→ 缺块终态
    await client.post_batch(unit("repo.identity", "repo:repo-identity-1", b"{}"))

    record = await client.repository.get_object("dev", content_hash)
    assert record is not None and record.state == "missing"
    assert record.missing_hashes
    assert await client.repository.get_session("dev", SESSION) is None  # 无部分提交

    # 缺块治愈：补发缺口后状态单向推进为 verified
    for index in range(1, len(chunks)):
        await client.post_batch(snapshot_frame(index, chunks[index], content_hash))
    record = await client.repository.get_object("dev", content_hash)
    assert record is not None and record.state == "verified"
    assert await client.object_store.get("dev", content_hash) == content


async def test_assembled_snapshot_satisfies_session_meta_anchor(client) -> None:
    content = snapshot_bytes(600 * 1024)
    content_hash = sha(content)
    chunks = chunk_payloads(content)

    frames = [snapshot_frame(index, chunk, content_hash) for index, chunk in enumerate(chunks)]
    frames.append(
        unit(
            "snapshot.manifest",
            f"manifest:{content_hash}",
            snapshot_manifest_payload(content),
            session_id=SESSION,
        )
    )
    await client.post_batch(batch(*frames))

    response = (
        await client.post_batch(
            unit(
                "session.meta",
                f"meta:{content_hash}",
                session_meta_payload(content_hash),
                session_id=SESSION,
            )
        )
    ).json()
    assert response["accepted"], response
    session = await client.repository.get_session("dev", SESSION)
    assert session is not None
    assert session.snapshot_sha256 == content_hash
    assert session.workspace_kind == "git"
    assert session.linked == "linked"
