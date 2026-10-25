from __future__ import annotations

import asyncio
import json
import os
import socket
import struct
from typing import Any

from .state import StateStore


class RegistrationServer:
    def __init__(self, state: StateStore):
        self.state = state
        self.server: asyncio.Server | None = None

    async def start(self) -> None:
        self.state.socket_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.state.socket_path.parent, 0o700)
        self.state.socket_path.unlink(missing_ok=True)
        self.server = await asyncio.start_unix_server(
            self._handle, path=self.state.socket_path, limit=8192
        )
        os.chmod(self.state.socket_path, 0o600)

    async def close(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        self.state.socket_path.unlink(missing_ok=True)

    @staticmethod
    def _peer_allowed(writer: asyncio.StreamWriter) -> bool:
        sock = writer.get_extra_info("socket")
        if sock is None or not hasattr(socket, "SO_PEERCRED"):
            return True
        _, uid, _ = struct.unpack(
            "3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
        )
        return uid in (0, os.getuid())

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        answer: dict[str, Any]
        try:
            if not self._peer_allowed(writer):
                raise PermissionError("forbidden")
            raw = await asyncio.wait_for(reader.readline(), 3)
            if not raw.endswith(b"\n") or len(raw) > 8192:
                raise ValueError("invalid_request")
            request = json.loads(raw)
            if (
                not isinstance(request, dict)
                or request.get("op") != "upsert"
                or not isinstance(request.get("target"), dict)
            ):
                raise ValueError("invalid_request")
            target = self.state.upsert_registration(request["target"])
            answer = {"ok": True, "target": target.id}
        except (ValueError, PermissionError, json.JSONDecodeError) as exc:
            answer = {"ok": False, "error": str(exc)}
        except Exception:
            answer = {"ok": False, "error": "internal"}
        writer.write(json.dumps(answer, separators=(",", ":")).encode() + b"\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()
