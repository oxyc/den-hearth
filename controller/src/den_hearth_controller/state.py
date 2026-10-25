from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import tempfile
import threading
import time
from typing import Any, Callable
import re

from .models import HEX32, Target


SECRET = re.compile(r"^[A-Za-z0-9_-]{43}$")


DEFAULT_STATE: dict[str, Any] = {
    "v": 1,
    "controller": None,
    "credential": None,
    "targets": {},
    "completed": {},
    "health": {"running": False, "edgeConnected": False, "updatedAt": None},
}


class StateStore:
    def __init__(self, directory: Path, socket_path: Path | None = None):
        self.directory = directory
        self.path = directory / "controller.json"
        self.health_path = directory / "health.json"
        self.socket_path = socket_path or directory / "controller.sock"
        self.pyatv_path = directory / "pyatv.json"
        self._lock = threading.RLock()
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(directory, 0o700)
        if not self.path.exists():
            self._write(DEFAULT_STATE.copy())
        self._check_permissions(self.path)
        self._read()

    @staticmethod
    def _check_permissions(path: Path) -> None:
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode & 0o077:
            raise PermissionError(f"state_file_permissions_{mode:o}")

    def _read(self) -> dict[str, Any]:
        with self._lock:
            value = json.loads(self.path.read_text())
            if value.get("v") != 1 or not isinstance(value.get("targets"), dict):
                raise ValueError("invalid_state")
            return value

    def _write_file(self, path: Path, value: Any) -> None:
        fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=self.directory)
        tmp = Path(raw)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w") as stream:
                json.dump(value, stream, sort_keys=True, separators=(",", ":"))
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp, path)
            dirfd = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(dirfd)
            finally:
                os.close(dirfd)
        finally:
            tmp.unlink(missing_ok=True)

    def _write(self, value: dict[str, Any]) -> None:
        with self._lock:
            self._write_file(self.path, value)

    def update(self, mutator: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        with self._lock:
            value = self._read()
            mutator(value)
            self._write(value)
            return value

    def credential(self) -> tuple[str, str] | None:
        value = self._read()
        if value.get("controller") and value.get("credential"):
            return value["controller"], value["credential"]
        return None

    def import_credential(self, credential: str) -> str:
        parts = credential.split(".")
        if (
            len(parts) != 2
            or not HEX32.fullmatch(parts[0])
            or not SECRET.fullmatch(parts[1])
        ):
            raise ValueError("invalid_controller_credential")
        self.update(
            lambda value: value.update(controller=parts[0], credential=credential)
        )
        return parts[0]

    def targets(self) -> list[Target]:
        return [Target.load(value) for value in self._read()["targets"].values()]

    def edge_targets(self) -> list[Target]:
        return [
            target
            for target in self.targets()
            if target.apple_tv_id
            and target.selected_bundle_id
            and target.app_known is not False
            and target.pairing_valid is not False
        ]

    def upsert_registration(self, registration: dict[str, Any]) -> Target:
        result: Target | None = None

        def mutate(value: dict[str, Any]) -> None:
            nonlocal result
            existing_raw = value["targets"].get(str(registration.get("deviceId", "")))
            existing = Target.load(existing_raw) if existing_raw else None
            result = Target.from_registration(registration, existing=existing)
            if len(value["targets"]) >= 16 and existing is None:
                raise ValueError("targets_full")
            for device_id, other in value["targets"].items():
                if device_id != result.device_id and (
                    other["inbox"] == result.inbox
                    or other["assistant"] == result.assistant
                ):
                    raise ValueError("target_conflict")
            value["targets"][result.device_id] = result.json()

        self.update(mutate)
        assert result is not None
        return result

    def map_target(
        self, device_id: str, apple_tv_id: str, address: str | None, bundle_id: str
    ) -> Target:
        result: Target | None = None

        def mutate(value: dict[str, Any]) -> None:
            nonlocal result
            raw = value["targets"].get(device_id)
            if raw is None:
                raise ValueError("unknown_target")
            target = Target.load(raw)
            target.apple_tv_id = apple_tv_id
            target.last_address = address
            target.selected_bundle_id = bundle_id
            target.reachable = True
            target.app_known = True
            target.pairing_valid = True
            target.last_stage = "app-known"
            target.last_stage_at = int(time.time() * 1000)
            target.last_error = None
            value["targets"][device_id] = target.json()
            result = target

        self.update(mutate)
        assert result is not None
        return result

    def record_stage(
        self, target_id: str, stage: str, status: str, reason: str | None = None
    ) -> None:
        now = int(time.time() * 1000)

        def mutate(value: dict[str, Any]) -> None:
            raw = next(
                (item for item in value["targets"].values() if item["id"] == target_id),
                None,
            )
            if raw is None:
                return
            raw["last_stage"] = stage
            raw["last_stage_at"] = now
            raw["last_error"] = reason if status == "error" else None
            if stage in (
                "companion-connected",
                "app-known",
                "launch-requested",
                "receiver-ready",
            ):
                raw["reachable"] = status != "error"
            if stage == "app-known":
                raw["app_known"] = status == "ok"
            if reason == "pairing-revoked":
                raw["pairing_valid"] = False

        self.update(mutate)

    def mark_attempt(
        self, target: Target, event_ids: list[str], at: int | None = None
    ) -> None:
        at = at or int(time.time() * 1000)

        def mutate(value: dict[str, Any]) -> None:
            raw = value["targets"].get(target.device_id)
            if raw:
                raw["last_attempt_at"] = at
            completed = value["completed"]
            for event_id in event_ids:
                completed[event_id] = at
            newest = sorted(completed.items(), key=lambda item: item[1], reverse=True)[
                :256
            ]
            value["completed"] = dict(newest)

        self.update(mutate)

    def completed(self, event_id: str) -> bool:
        return event_id in self._read()["completed"]

    def set_health(self, **fields: Any) -> None:
        now = int(time.time() * 1000)

        def mutate(value: dict[str, Any]) -> None:
            value["health"].update(fields, updatedAt=now)

        state = self.update(mutate)
        public = dict(state["health"])
        public["configuredTargets"] = len(state["targets"])
        public["mappedTargets"] = sum(
            bool(t.get("apple_tv_id")) for t in state["targets"].values()
        )
        public["credentialConfigured"] = bool(state.get("credential"))
        public["targets"] = [
            {
                "id": item["id"],
                "paired": bool(item.get("apple_tv_id"))
                and item.get("pairing_valid") is not False,
                "reachable": item.get("reachable"),
                "appKnown": item.get("app_known"),
                "lastStage": item.get("last_stage"),
                "lastStageAt": item.get("last_stage_at"),
                "lastError": item.get("last_error"),
            }
            for item in state["targets"].values()
        ]
        self._write_file(self.health_path, public)

    def health(self) -> dict[str, Any]:
        if self.health_path.exists():
            return json.loads(self.health_path.read_text())
        value = self._read()
        return value["health"]
