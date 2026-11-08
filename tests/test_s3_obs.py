"""华为云 OBS 适配（委托凭证 / botocore 校验开关 / 缺对象语义）。"""

from __future__ import annotations

import json
import urllib.request

import boto3
import pytest
from botocore.exceptions import ClientError

from icode_data_server.config import Settings
from icode_data_server.storage import build_object_store
from icode_data_server.storage.credentials import EcsMetadataCredentials, MetadataCredentialError
from icode_data_server.storage.objects import S3ObjectStore, _is_not_found


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._body = json.dumps(payload).encode()

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def read(self) -> bytes:
        return self._body


def _client_error(code: str, status: int, operation: str = "HeadObject") -> ClientError:
    return ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        operation,
    )


def test_is_not_found_accepts_obs_403_for_missing_key() -> None:
    assert _is_not_found(_client_error("NoSuchKey", 404))
    assert _is_not_found(_client_error("404", 404))
    assert _is_not_found(_client_error("AccessDenied", 403))
    # OBS 的 head_object 对缺失键回的是裸状态码 "403"
    assert _is_not_found(_client_error("403", 403))
    assert not _is_not_found(_client_error("AccessDenied", 400))
    assert not _is_not_found(_client_error("InternalError", 500))
    assert not _is_not_found(RuntimeError("boom"))


def test_client_is_built_with_obs_safe_config(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_client(service: str, **kwargs: object) -> object:
        captured["service"] = service
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(boto3, "client", fake_client)

    store = S3ObjectStore(
        bucket="icode-custom-reports",
        endpoint_url="https://obs.cn-north-9.myhuaweicloud.com",
        region="cn-north-9",
        access_key_id="AK",
        secret_access_key="SK",
        session_token="TOKEN",
        prefix="test/plugins",
    )
    store._ensure_client()

    assert captured["service"] == "s3"
    assert captured["aws_session_token"] == "TOKEN"
    assert captured["endpoint_url"] == "https://obs.cn-north-9.myhuaweicloud.com"
    config = captured["config"]
    assert config.request_checksum_calculation == "when_required"
    assert config.response_checksum_validation == "when_required"
    assert store._key("dev", "ab" * 32) == f"test/plugins/t/dev/o/ab/{'ab' * 32}"


async def test_missing_object_maps_to_file_not_found() -> None:
    store = S3ObjectStore(bucket="b", access_key_id="AK", secret_access_key="SK")

    class FakeClient:
        def get_object(self, **kwargs: object) -> object:
            raise _client_error("AccessDenied", 403, "GetObject")

        def head_object(self, **kwargs: object) -> object:
            raise _client_error("403", 403)

    store._client = FakeClient()

    with pytest.raises(FileNotFoundError):
        await store.get("dev", "cd" * 32)
    assert await store.exists("dev", "cd" * 32) is False


def test_ecs_metadata_credentials_cache_and_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_urlopen(url: str, timeout: float | None = None) -> _FakeResponse:
        calls.append(url)
        return _FakeResponse(
            {
                "credential": {
                    "access": "AK",
                    "secret": "SK",
                    "securitytoken": "TOKEN",
                    "expires_at": "2030-01-01T00:00:00.000000Z",
                }
            }
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    provider = EcsMetadataCredentials("http://metadata.test/securitykey")
    assert provider.resolve() == ("AK", "SK", "TOKEN")
    assert provider.resolve() == ("AK", "SK", "TOKEN")
    assert len(calls) == 1
    assert provider.stale() is False

    provider._refresh_at = 0.0
    assert provider.stale() is True
    assert provider.resolve() == ("AK", "SK", "TOKEN")
    assert len(calls) == 2


def test_ecs_metadata_without_agency_reports_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        urllib.request, "urlopen", lambda url, timeout=None: _FakeResponse({"code": "401"})
    )
    with pytest.raises(MetadataCredentialError):
        EcsMetadataCredentials().resolve()


def test_build_object_store_wires_credential_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORE_BACKEND", "s3")
    monkeypatch.setenv("ICODE_DS_S3_BUCKET", "icode-custom-reports")
    monkeypatch.setenv("ICODE_DS_S3_CREDENTIAL_SOURCE", "ecs-metadata")
    monkeypatch.setenv("ICODE_DS_S3_METADATA_URL", "http://metadata.test/securitykey")

    settings = Settings.from_env()
    store = build_object_store(settings)

    assert isinstance(store, S3ObjectStore)
    assert settings.s3_credential_source == "ecs-metadata"
    assert store._credentials is not None

    monkeypatch.setenv("ICODE_DS_S3_CREDENTIAL_SOURCE", "oauth")
    with pytest.raises(ValueError):
        build_object_store(Settings.from_env())

    monkeypatch.delenv("ICODE_DS_S3_CREDENTIAL_SOURCE")
    assert build_object_store(Settings.from_env())._credentials is None
