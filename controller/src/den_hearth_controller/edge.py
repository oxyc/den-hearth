from __future__ import annotations

import asyncio
import json
import random
import time
from urllib.parse import urlsplit

import aiohttp

from .logging import event as log_event
from .models import Stage, Target, WakeEvent


class EdgeClient:
    def __init__(self, base_url: str, controller: str, credential: str):
        parsed = urlsplit(base_url)
        if (
            not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in ("", "/")
        ):
            raise ValueError("invalid_edge_url")
        self.url = base_url.rstrip("/") + "/wake/poll?wait=25"
        if parsed.scheme != "https" and not (
            parsed.scheme == "http" and parsed.hostname in ("127.0.0.1", "::1")
        ):
            raise ValueError("edge_url_requires_https")
        self.controller = controller
        self.credential = credential
        self._acks: list[str] = []
        self._stages: list[Stage] = []
        self._lock = asyncio.Lock()

    async def ack(self, event_ids: list[str]) -> None:
        async with self._lock:
            for event_id in event_ids:
                if event_id not in self._acks:
                    self._acks.append(event_id)
            self._acks = self._acks[-32:]

    async def stage(self, stage: Stage) -> None:
        async with self._lock:
            self._stages.append(stage)
            self._stages = self._stages[-32:]

    async def poll(
        self, session: aiohttp.ClientSession, targets: list[Target]
    ) -> list[WakeEvent]:
        async with self._lock:
            acks = list(self._acks[:32])
            stages = list(self._stages[:32])
        body = {
            "v": 1,
            "targets": [target.edge() for target in targets],
            "acks": acks,
            "stages": [stage.json() for stage in stages],
        }
        if len(json.dumps(body, separators=(",", ":")).encode()) > 32_768:
            raise ValueError("edge_request_too_large")
        timeout = aiohttp.ClientTimeout(total=35, connect=10)
        async with session.post(
            self.url,
            json=body,
            headers={"x-den-wake-controller": self.credential},
            timeout=timeout,
            max_line_size=8192,
            max_field_size=8192,
        ) as response:
            if response.status != 200:
                await response.read()
                raise ConnectionError(f"edge_status_{response.status}")
            if response.content_length is not None and response.content_length > 65_536:
                raise ValueError("edge_response_too_large")
            chunks: list[bytes] = []
            size = 0
            async for chunk in response.content.iter_chunked(8192):
                size += len(chunk)
                if size > 65_536:
                    raise ValueError("edge_response_too_large")
                chunks.append(chunk)
            raw = b"".join(chunks)
            if len(raw) > 65_536:
                raise ValueError("edge_response_too_large")
            value = json.loads(raw)
            if (
                not isinstance(value, dict)
                or value.get("v") != 1
                or not isinstance(value.get("events"), list)
            ):
                raise ValueError("invalid_edge_response")
            if len(value["events"]) > 16:
                raise ValueError("invalid_edge_response")
            events = [WakeEvent.parse(item) for item in value["events"]]
            accepted = value.get("acceptedTargets")
            if not isinstance(accepted, list) or set(accepted) != {
                target.id for target in targets
            }:
                raise ValueError("invalid_edge_response")
            if len({item.id for item in events}) != len(events) or len(
                {item.target for item in events}
            ) != len(events):
                raise ValueError("invalid_edge_response")
        async with self._lock:
            self._acks = [item for item in self._acks if item not in acks]
            sent = {id(item) for item in stages}
            self._stages = [item for item in self._stages if id(item) not in sent]
        return events

    async def run(self, targets, handler, health, stop: asyncio.Event) -> None:
        attempt = 0
        connector = aiohttp.TCPConnector(limit=2, ttl_dns_cache=300)
        async with aiohttp.ClientSession(
            connector=connector, trust_env=False
        ) as session:
            while not stop.is_set():
                try:
                    started = time.monotonic()
                    events = await self.poll(session, targets())
                    attempt = 0
                    health(edgeConnected=True, edgeError=None)
                    for item in events:
                        await handler(item)
                    if not events and time.monotonic() - started < 1.0:
                        try:
                            await asyncio.wait_for(stop.wait(), 1.0)
                        except asyncio.TimeoutError:
                            pass
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    health(edgeConnected=False, edgeError=type(exc).__name__)
                    cap = min(60.0, 1.0 * (2 ** min(attempt, 6)))
                    delay = random.uniform(0.25, cap)
                    log_event(
                        "edge_reconnect",
                        reason=type(exc).__name__,
                        attempt=attempt,
                        delay=round(delay, 2),
                    )
                    attempt += 1
                    try:
                        await asyncio.wait_for(stop.wait(), delay)
                    except asyncio.TimeoutError:
                        pass
