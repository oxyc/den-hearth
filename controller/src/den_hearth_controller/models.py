from __future__ import annotations

from dataclasses import asdict, dataclass, field
import re
import time
from typing import Any
import unicodedata


HEX16 = re.compile(r"^[0-9a-f]{16}$")
HEX32 = re.compile(r"^[0-9a-f]{32}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
BUNDLE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{1,254}$")


def clean_label(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("invalid_name")
    value = " ".join(value.split())
    if (
        not value
        or len(value) > 40
        or any(ord(c) < 32 or unicodedata.category(c) == "Cf" for c in value)
    ):
        raise ValueError("invalid_name")
    return value


@dataclass
class Target:
    id: str
    device_id: str
    name: str
    inbox: str
    assistant: str
    bundle_id: str
    selected_bundle_id: str | None = None
    apple_tv_id: str | None = None
    last_address: str | None = None
    last_attempt_at: int | None = None
    reachable: bool | None = None
    app_known: bool | None = None
    pairing_valid: bool | None = None
    last_stage: str | None = None
    last_stage_at: int | None = None
    last_error: str | None = None

    @classmethod
    def from_registration(
        cls, value: dict[str, Any], *, existing: Target | None = None
    ) -> Target:
        if value.get("v") != 1 or not HEX16.fullmatch(str(value.get("deviceId", ""))):
            raise ValueError("invalid_device")
        if not HEX64.fullmatch(str(value.get("householdInbox", ""))):
            raise ValueError("invalid_inbox")
        if not HEX32.fullmatch(str(value.get("assistantLane", ""))):
            raise ValueError("invalid_assistant")
        bundle = value.get("bundleId")
        if not isinstance(bundle, str) or not BUNDLE.fullmatch(bundle):
            raise ValueError("invalid_bundle")
        device_id = value["deviceId"]
        return cls(
            id=existing.id if existing else __import__("secrets").token_hex(16),
            device_id=device_id,
            name=clean_label(value.get("name")),
            inbox=value["householdInbox"],
            assistant=value["assistantLane"],
            bundle_id=bundle,
            selected_bundle_id=existing.selected_bundle_id if existing else None,
            apple_tv_id=existing.apple_tv_id if existing else None,
            last_address=existing.last_address if existing else None,
            last_attempt_at=existing.last_attempt_at if existing else None,
            reachable=existing.reachable if existing else None,
            app_known=existing.app_known if existing else None,
            pairing_valid=existing.pairing_valid if existing else None,
            last_stage=existing.last_stage if existing else None,
            last_stage_at=existing.last_stage_at if existing else None,
            last_error=existing.last_error if existing else None,
        )

    @classmethod
    def load(cls, value: dict[str, Any]) -> Target:
        target = cls(**value)
        if not HEX32.fullmatch(target.id) or not HEX16.fullmatch(target.device_id):
            raise ValueError("invalid_target_state")
        return target

    def edge(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "inbox": self.inbox,
            "tv": self.assistant,
            "capabilities": ["app-launch", "companion-wake"],
        }

    def json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WakeEvent:
    id: str
    target: str
    sources: tuple[str, ...]
    first_at: int
    last_at: int

    @classmethod
    def parse(cls, value: Any) -> WakeEvent:
        if not isinstance(value, dict) or not HEX32.fullmatch(
            str(value.get("event", ""))
        ):
            raise ValueError("invalid_event")
        if not HEX32.fullmatch(str(value.get("target", ""))):
            raise ValueError("invalid_event")
        sources = value.get("sources")
        if not isinstance(sources, list) or not sources or len(sources) > 2:
            raise ValueError("invalid_event")
        parsed = tuple(sorted(set(sources)))
        if len(parsed) != len(sources) or any(
            x not in ("assistant", "inbox") for x in parsed
        ):
            raise ValueError("invalid_event")
        first = value.get("firstAcceptedAt")
        last = value.get("lastAcceptedAt")
        if (
            not isinstance(first, int)
            or not isinstance(last, int)
            or first < 0
            or last < first
        ):
            raise ValueError("invalid_event")
        return cls(value["event"], value["target"], parsed, first, last)

    def stale(self, now_ms: int | None = None) -> bool:
        return (
            now_ms if now_ms is not None else int(time.time() * 1000)
        ) - self.last_at > 120_000


@dataclass
class Stage:
    event: str
    target: str
    stage: str
    status: str
    at: int = field(default_factory=lambda: int(time.time() * 1000))
    elapsed_ms: int | None = None
    reason: str | None = None

    def json(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "event": self.event,
            "target": self.target,
            "stage": self.stage,
            "status": self.status,
            "at": self.at,
        }
        if self.elapsed_ms is not None:
            value["elapsedMs"] = min(300_000, max(0, self.elapsed_ms))
        if self.reason is not None:
            value["reason"] = self.reason
        return value
