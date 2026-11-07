"""工作区 IGWS 容器拼装（server-acceptance §3.2-6 / §7 T14）。"""

from __future__ import annotations

import hashlib
import json
import os
import struct
import time

from icode_data_server.ingest.workspace import (
    build_container,
    container_sha256,
    content_refs,
    parse_manifest,
)

from .helpers import (
    SESSION,
    batch,
    sha,
    unit,
    workspace_file_frames,
    workspace_manifest,
)

FILES = [
    ("src/main.py", b"print('hello')\n"),
    ("README.md", "中文 README ✓\n".encode()),
    ("bin/app", b"\x00\x01\x02\x03binary"),
]


def _manifest(files=FILES, **kwargs):
    return workspace_manifest(files=files, **kwargs)


def test_container_bytes_are_deterministic_and_match_teams_layout() -> None:
    payload, contents = _manifest()
    manifest = parse_manifest(payload)
    refs = content_refs(manifest)
    data = [body for _digest, body in contents]

    first = build_container(manifest, data)
    second = build_container(parse_manifest(payload), data)
    assert first == second
    assert container_sha256(first) == hashlib.sha256(first).hexdigest()

    # 容器头：magic + 小端 version/flags/manifestLength
    assert first[:4] == b"IGWS"
    version, flags = struct.unpack_from("<HH", first, 4)
    (manifest_length,) = struct.unpack_from("<I", first, 8)
    assert version == 1
    assert flags & 1 == 1
    embedded = json.loads(first[12 : 12 + manifest_length])
    assert embedded["schema"] == "workspace-state/1.0"
    assert "ignored" not in embedded
    # 内容流按 manifest 顺序紧跟 manifest JSON
    stream = first[12 + manifest_length :]
    assert len(stream) == sum(len(body) for body in data)
    for ref, body in zip(refs, data, strict=True):
        assert stream[ref.offset : ref.offset + ref.length] == body


def test_ignored_block_does_not_change_container_bytes() -> None:
    payload, contents = _manifest()
    ignored_payload, _ = _manifest(ignored=["node_modules/", ".env"])
    data = [body for _digest, body in contents]
    first = build_container(parse_manifest(payload), data)
    second = build_container(parse_manifest(ignored_payload), data)
    assert container_sha256(first) == container_sha256(second)


async def test_workspace_units_build_container_and_row(client) -> None:
    payload, contents = _manifest()
    frames = workspace_file_frames(contents)
    await client.post_batch(batch(*frames))

    response = (
        await client.post_batch(
            unit("workspace.manifest", f"ws:{sha(payload)}", payload, session_id=SESSION)
        )
    ).json()
    assert response["accepted"], response

    expected = build_container(parse_manifest(payload), [body for _digest, body in contents])
    expected_sha = container_sha256(expected)
    assert await client.object_store.get("dev", expected_sha) == expected

    rows = await client.repository.list_workspace_states("dev", SESSION)
    assert len(rows) == 1
    row = rows[0]
    assert row.body_sha256 == expected_sha
    assert row.repository_identity == "repo-identity-1"
    assert row.staged_entries == 3
    assert row.remote_kind == "origin"
    assert row.complete is True
    assert row.ignored_entries == ()


async def test_workspace_manifest_is_deferred_until_files_arrive(client) -> None:
    payload, contents = _manifest()
    response = (
        await client.post_batch(
            unit("workspace.manifest", f"ws:{sha(payload)}", payload, session_id=SESSION)
        )
    ).json()
    assert len(response["deferred"]) == 1, response
    assert response["accepted"] == []
    assert await client.repository.list_workspace_states("dev", SESSION) == []

    await client.post_batch(batch(*workspace_file_frames(contents)))
    response = (
        await client.post_batch(
            unit("workspace.manifest", f"ws:{sha(payload)}", payload, session_id=SESSION)
        )
    ).json()
    assert response["accepted"], response
    rows = await client.repository.list_workspace_states("dev", SESSION)
    assert len(rows) == 1


async def test_workspace_ignored_entries_are_paths_only(client) -> None:
    payload, contents = _manifest(ignored=["node_modules/", "dist/"])
    await client.post_batch(batch(*workspace_file_frames(contents)))
    await client.post_batch(
        unit("workspace.manifest", f"ws:{sha(payload)}", payload, session_id=SESSION)
    )
    rows = await client.repository.list_workspace_states("dev", SESSION)
    assert rows[0].ignored_count == 2
    assert rows[0].ignored_entries == ("node_modules/", "dist/")


async def test_bad_manifest_schema_is_rejected(client) -> None:
    payload = json.dumps({"schema": "workspace-state/2.0"}).encode()
    response = (
        await client.post_batch(
            unit("workspace.manifest", f"ws:{sha(payload)}", payload, session_id=SESSION)
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert client.repository.workspaces == {}


async def test_same_file_content_is_deduplicated_across_paths(client) -> None:
    files = [("a.txt", b"shared"), ("b.txt", b"shared")]
    payload, contents = _manifest(files=files)
    assert len(contents) == 2 and contents[0][0] == contents[1][0]
    await client.post_batch(batch(*workspace_file_frames(contents)))
    assert await client.object_store.exists("dev", contents[0][0])
    response = (
        await client.post_batch(
            unit("workspace.manifest", f"ws:{sha(payload)}", payload, session_id=SESSION)
        )
    ).json()
    assert response["accepted"], response


async def test_staging_ttl_marks_missing_releases_bytes_and_heals_on_replay(
    custom_client,
) -> None:
    """P0-2 回归：TTL 释放字节后，重放已 committed 的分片必须重新物化并治愈。"""
    client = await custom_client(assembly_ttl_s=0.0)
    payload, contents = _manifest(files=[("big.bin", b"a" * 10)])
    frames = workspace_file_frames(contents, chunk_size=4)
    assert len(frames) == 3
    digest, body = contents[0]
    staging_dir = client.staging / "dev" / "wsfile" / digest

    response = (await client.post_batch(batch(frames[0], frames[1]))).json()
    assert len(response["accepted"]) == 2
    assert staging_dir.exists()
    # 下一批先触发清扫（ttl=0）：缺块终态 + 分片字节释放，manifest 随之 deferred
    response = (
        await client.post_batch(
            unit("workspace.manifest", f"ws:{sha(payload)}", payload, session_id=SESSION)
        )
    ).json()
    assert response["deferred"], response
    record = await client.repository.get_object("dev", digest)
    assert record is not None and record.state == "missing"
    assert record.missing_hashes
    assert not staging_dir.exists()

    # 客户端按协商重发：已 committed 的两个分片走 duplicates 重新落盘，
    # 第三个（此前从未送达）作为新单元接受，落最后一块即完成拼装
    response = (await client.post_batch(batch(*frames))).json()
    assert len(response["duplicates"]) == 2, response
    assert len(response["accepted"]) == 1, response
    assert await client.object_store.get("dev", digest) == body

    response = (
        await client.post_batch(
            unit("workspace.manifest", f"ws:{sha(payload)}", payload, session_id=SESSION)
        )
    ).json()
    assert response["accepted"], response
    rows = await client.repository.list_workspace_states("dev", SESSION)
    assert len(rows) == 1


async def test_staging_budget_overrun_defers_instead_of_evicting(custom_client) -> None:
    """P0-2 回归：超预算是背压（deferred），不是静默淘汰。"""
    client = await custom_client(staging_bytes_per_session=8)
    payload, contents = _manifest(files=[("big.bin", b"a" * 10)])
    frames = workspace_file_frames(contents, chunk_size=4)
    digest = contents[0][0]

    response = (await client.post_batch(batch(frames[0], frames[1]))).json()
    assert len(response["accepted"]) == 2

    # 第三个分片会超出会话预算 → deferred（无副作用），已收分片原样保留
    response = (await client.post_batch(frames[2])).json()
    assert len(response["deferred"]) == 1, response
    assert response["retry_after_ms"] > 0
    assert not await client.object_store.exists("dev", digest)
    record = await client.repository.get_object("dev", digest)
    assert record is None  # 没有被标 missing，也没有部分提交
    staged = sorted(path.name for path in (client.staging / "dev" / "wsfile" / digest).iterdir())
    assert staged == ["000000.bin", "000001.bin"]


async def test_orphaned_staging_directories_are_reclaimed(client) -> None:
    """重启后无人认领的暂存目录由清扫按 mtime 回收，活跃目录不受影响。"""
    orphan = client.staging / "dev" / "wsfile" / ("f" * 64)
    orphan.mkdir(parents=True)
    stale = orphan / "000000.bin"
    stale.write_bytes(b"x")
    old = time.time() - 48 * 3600
    os.utime(stale, (old, old))

    fresh = client.staging / "dev" / "wsfile" / ("e" * 64)
    fresh.mkdir(parents=True)
    (fresh / "000000.bin").write_bytes(b"y")

    await client.post_batch(unit("repo.identity", "repo:repo-identity-1", b"{}"))
    assert not orphan.exists()
    assert fresh.exists()


async def test_ignored_entries_are_frozen_on_first_capture(client) -> None:
    """忽略清单首采冻结（agent-config-sync §2-4）：重采不改写三列。"""
    first_payload, first_contents = _manifest(ignored=["node_modules/", "dist/"])
    await client.post_batch(batch(*workspace_file_frames(first_contents)))
    response = (
        await client.post_batch(
            unit(
                "workspace.manifest",
                f"ws:{sha(first_payload)}",
                first_payload,
                session_id=SESSION,
            )
        )
    ).json()
    assert response["accepted"], response

    # 重采：清单变了（新增 .venv/），镜像推进，但忽略清单保持首采口径
    second_payload, second_contents = _manifest(ignored=["node_modules/", "dist/", ".venv/"])
    await client.post_batch(batch(*workspace_file_frames(second_contents)))
    response = (
        await client.post_batch(
            unit(
                "workspace.manifest",
                f"ws:{sha(second_payload)}",
                second_payload,
                session_id=SESSION,
            )
        )
    ).json()
    assert response["accepted"], response

    rows = await client.repository.list_workspace_states("dev", SESSION)
    assert len(rows) == 1
    assert rows[0].ignored_count == 2
    assert rows[0].ignored_entries == ("node_modules/", "dist/")


async def test_ignored_entry_path_over_512_is_rejected(client) -> None:
    """单条忽略路径 ≤ 512 字符：超限按 manifest 结构性错误拒绝（客户端应先行折叠）。"""
    payload, contents = _manifest(ignored=["x" * 513])
    await client.post_batch(batch(*workspace_file_frames(contents)))
    response = (
        await client.post_batch(
            unit("workspace.manifest", f"ws:{sha(payload)}", payload, session_id=SESSION)
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "512" in response["rejected"][0]["message"]
    assert await client.repository.list_workspace_states("dev", SESSION) == []
