"""快照逐字节重组与缺块（server-acceptance §3.2 / §7 T2）。"""

from __future__ import annotations

import json

from .helpers import (
    SESSION,
    batch,
    chunk_payloads,
    send_snapshot,
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

    # 乱序：2 → 0 → 1。分块是内容寻址对象，顺序不影响登记；
    # 拼装由 manifest 驱动，任何时刻全块到齐即可拼出。
    for index in (2, 0, 1):
        response = (
            await client.post_batch(snapshot_frame(index, chunks[index], content_hash))
        ).json()
        assert len(response["accepted"]) == 1
    for _index, chunk in enumerate(chunks):
        record = await client.repository.get_object("dev", sha(chunk))
        assert record is not None and record.state == "verified"
    assert await client.repository.get_object("dev", content_hash) is None

    await client.post_batch(
        unit(
            "snapshot.manifest",
            f"manifest:{content_hash}",
            snapshot_manifest_payload(content),
            session_id=SESSION,
        )
    )
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
    # 无 manifest → 不登记快照行，也不落整份对象
    assert await client.repository.get_object("dev", content_hash) is None
    assert not await client.object_store.exists("dev", content_hash)

    # 补发缺块 + manifest → 组装完成
    await client.post_batch(
        batch(
            snapshot_frame(1, chunks[1], content_hash),
            unit(
                "snapshot.manifest",
                f"manifest:{content_hash}",
                snapshot_manifest_payload(content),
                session_id=SESSION,
            ),
        )
    )
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


async def test_manifest_registers_pending_chunk_list(client) -> None:
    """清单先到：登记缺块行（期望分块清单），分块补齐后行翻转为 verified。"""
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
    record = await client.repository.get_object("dev", content_hash)
    assert record is not None and record.state == "missing"
    assert list(record.missing_hashes) == [sha(chunk) for chunk in chunks]

    # 分块补齐（清单行在场）→ 最后一块触发就地拼装
    await client.post_batch(
        batch(*[snapshot_frame(index, chunk, content_hash) for index, chunk in enumerate(chunks)])
    )
    record = await client.repository.get_object("dev", content_hash)
    assert record is not None and record.state == "verified"
    assert record.missing_hashes == ()
    assert await client.object_store.get("dev", content_hash) == content


async def test_incremental_snapshot_second_version_assembles(client) -> None:
    """P0-1 回归：第二版快照只发变化块 + 新 manifest，未变化块沿用已登记对象。"""
    first = snapshot_bytes(600 * 1024)
    first_hash, first_chunks = await send_snapshot(client, first)
    assert await client.object_store.get("dev", first_hash) == first

    # 第二版只改最后 32 字节 → 只有最后一块变化
    second = first[:-32] + b"x" * 32
    second_hash = sha(second)
    second_chunks = chunk_payloads(second)
    changed = [i for i, c in enumerate(second_chunks) if sha(c) != sha(first_chunks[i])]
    assert changed == [len(second_chunks) - 1]

    response = (
        await client.post_batch(
            batch(
                snapshot_frame(changed[0], second_chunks[changed[0]], second_hash),
                unit(
                    "snapshot.manifest",
                    f"manifest:{second_hash}",
                    snapshot_manifest_payload(second),
                    session_id=SESSION,
                ),
            )
        )
    ).json()
    assert response["accepted"], response
    assert await client.object_store.get("dev", second_hash) == second

    # 第二版的 session.meta 不再永久 deferred
    meta = (
        await client.post_batch(
            unit(
                "session.meta",
                f"meta:{second_hash}",
                session_meta_payload(second_hash),
                session_id=SESSION,
            )
        )
    ).json()
    assert meta["accepted"], meta
    session = await client.repository.get_session("dev", SESSION)
    assert session is not None and session.snapshot_sha256 == second_hash


async def test_snapshot_manifest_oversize_is_rejected(custom_client) -> None:
    client = await custom_client(max_snapshot_bytes=256 * 1024)
    content = snapshot_bytes(600 * 1024)
    content_hash = sha(content)
    await client.post_batch(snapshot_frame(0, chunk_payloads(content)[0], content_hash))
    response = (
        await client.post_batch(
            unit(
                "snapshot.manifest",
                f"manifest:{content_hash}",
                snapshot_manifest_payload(content),
                session_id=SESSION,
            )
        )
    ).json()
    assert response["rejected"][0]["code"] == "payload_too_large"
    assert await client.repository.get_object("dev", content_hash) is None


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
    # 环境三列只由 agent.env 写（agent-config-sync §1）：session.meta 不再携带 environment
    assert session.workspace_kind is None
    assert session.linked == "linked"
