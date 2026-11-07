"""工作区 IGWS 容器拼装（server-acceptance §3.2-6 / §7 T14）。"""

from __future__ import annotations

import hashlib
import json
import struct

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
