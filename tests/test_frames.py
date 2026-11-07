"""帧编解码：round-trip、gzip、坏帧、限额。"""

from __future__ import annotations

import gzip
import hashlib
import json
import struct

import pytest

from icode_data_server.frames import FrameError, decode_frames, encode_frame, unit_id_for


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_frame_round_trip() -> None:
    payload = "快照 payload".encode()
    raw = encode_frame(
        {"v": 1, "kind": "agent.env", "session_id": "s1", "scope": "agentenv:" + "a" * 64},
        payload,
    )
    frames = decode_frames(raw)
    assert len(frames) == 1
    frame = frames[0]
    assert frame.payload == payload
    assert frame.header.kind == "agent.env"
    assert frame.header.session_id == "s1"
    assert frame.header.payload_sha256 == _sha(payload)
    assert frame.unit_id == unit_id_for("agent.env", "s1", "agentenv:" + "a" * 64, _sha(payload))


def test_multiple_frames_are_decoded_in_order() -> None:
    first = encode_frame({"kind": "repo.identity", "scope": "repo:x"}, b"one")
    second = encode_frame({"kind": "repo.identity", "scope": "repo:y"}, b"two")
    frames = decode_frames(first + second)
    assert [frame.payload for frame in frames] == [b"one", b"two"]


def test_truncated_frame_is_a_format_error() -> None:
    raw = encode_frame({"kind": "repo.identity", "scope": "repo:x"}, b"abcdef")
    with pytest.raises(FrameError):
        decode_frames(raw[:-2])


def test_bad_header_json_is_a_format_error() -> None:
    bad = struct.pack(">I", 3) + b"{}\n"
    with pytest.raises(FrameError):
        decode_frames(bad)


async def test_gzip_body_is_expanded(client) -> None:
    payload = json.dumps({"redacted": []}).encode()
    raw = encode_frame(
        {"kind": "agent.env", "session_id": "s1", "scope": "agentenv:" + _sha(payload)},
        payload,
    )
    response = await client.post_batch(gzip.compress(raw), headers={"Content-Encoding": "gzip"})
    assert response.status_code == 200, response.text
    assert response.json()["accepted"], response.json()


async def test_bad_body_returns_400(client) -> None:
    response = await client.post_batch(b"\x00\x00\x00\x10short")
    assert response.status_code == 400
    assert response.json()["error"] == "frame_format"


async def test_body_too_large_returns_413_without_side_effects(custom_client) -> None:
    client = await custom_client(max_body_bytes=4096, max_expanded_bytes=4096)
    frame = encode_frame({"kind": "repo.identity", "scope": "repo:x"}, b"x" * 8192)
    response = await client.post_batch(frame)
    assert response.status_code == 413
    assert client.repository.receipts == {}


async def test_expanded_body_too_large_returns_413(custom_client) -> None:
    client = await custom_client(max_body_bytes=1 << 20, max_expanded_bytes=1024)
    frame = encode_frame({"kind": "repo.identity", "scope": "repo:x"}, b"x" * 4096)
    response = await client.post_batch(gzip.compress(frame), headers={"Content-Encoding": "gzip"})
    assert response.status_code == 413
    assert client.repository.receipts == {}


async def test_too_many_frames_returns_413(custom_client) -> None:
    client = await custom_client(max_frames_per_batch=2)
    frames = b"".join(
        encode_frame({"kind": "repo.identity", "scope": f"repo:{index}"}, b"x")
        for index in range(3)
    )
    response = await client.post_batch(frames)
    assert response.status_code == 413
    assert client.repository.receipts == {}


async def test_frame_payload_too_large_is_a_rejection(custom_client) -> None:
    client = await custom_client(max_frame_payload_bytes=16)
    frame = encode_frame({"kind": "repo.identity", "scope": "repo:x"}, b"x" * 64)
    response = await client.post_batch(frame)
    assert response.status_code == 200
    body = response.json()
    assert body["rejected"][0]["code"] == "payload_too_large"
    assert body["accepted"] == []
