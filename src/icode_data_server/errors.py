"""单元处理的结果类型与拒绝码（server-acceptance §2.2）。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class RejectionCode(StrEnum):
    PAYLOAD_SHA_MISMATCH = "payload_sha_mismatch"
    PAYLOAD_TOO_LARGE = "payload_too_large"
    UNSUPPORTED_KIND = "unsupported_kind"
    UNSUPPORTED_VERSION = "unsupported_version"
    SCHEMA_INVALID = "schema_invalid"
    SEQUENCE_CONFLICT = "sequence_conflict"
    MISSING_REFERENCE = "missing_reference"


class OutcomeKind(StrEnum):
    COMMITTED = "committed"
    DUPLICATE = "duplicate"
    DEFERRED = "deferred"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class Outcome:
    kind: OutcomeKind
    code: RejectionCode | None = None
    message: str = ""

    @classmethod
    def committed(cls) -> Outcome:
        return cls(OutcomeKind.COMMITTED)

    @classmethod
    def duplicate(cls) -> Outcome:
        return cls(OutcomeKind.DUPLICATE)

    @classmethod
    def deferred(cls, message: str = "") -> Outcome:
        return cls(OutcomeKind.DEFERRED, message=message)

    @classmethod
    def rejected(cls, code: RejectionCode, message: str = "") -> Outcome:
        return cls(OutcomeKind.REJECTED, code=code, message=message)


class RollbackUnit(Exception):
    """内部信号：事务内发现前置条件不满足，回滚已产生的副作用（deferred 无副作用）。"""
