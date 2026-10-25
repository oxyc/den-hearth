from __future__ import annotations

import asyncio
import time

from .apple_tv import AppleTVClient
from .edge import EdgeClient
from .logging import event as log_event
from .models import Stage, Target, WakeEvent
from .state import StateStore


class Coordinator:
    def __init__(
        self,
        state: StateStore,
        edge: EdgeClient,
        apple: AppleTVClient,
        *,
        cooldown_ms: int = 30_000,
    ):
        self.state = state
        self.edge = edge
        self.apple = apple
        self.cooldown_ms = cooldown_ms
        self._lock = asyncio.Lock()
        self._groups: dict[str, list[WakeEvent]] = {}
        self._tasks: set[asyncio.Task] = set()

    async def submit(self, wake: WakeEvent) -> None:
        if self.state.completed(wake.id):
            await self.edge.ack([wake.id])
            return
        targets = {target.id: target for target in self.state.targets()}
        target = targets.get(wake.target)
        if target is None:
            await self.edge.stage(
                Stage(
                    wake.id, wake.target, "trigger-received", "error", reason="internal"
                )
            )
            await self.edge.ack([wake.id])
            return
        await self.edge.stage(Stage(wake.id, target.id, "trigger-received", "ok"))
        if wake.stale():
            await self.edge.stage(
                Stage(
                    wake.id,
                    target.id,
                    "trigger-received",
                    "error",
                    reason="stale-event",
                )
            )
            self.state.mark_attempt(target, [wake.id])
            await self.edge.ack([wake.id])
            return
        async with self._lock:
            if self.state.completed(wake.id):
                await self.edge.ack([wake.id])
                return
            group = self._groups.get(target.id)
            if group is not None:
                group.append(wake)
                return
            self._groups[target.id] = [wake]
            task = asyncio.create_task(self._run(target), name=f"wake-{target.id}")
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _run(self, target: Target) -> None:
        now = int(time.time() * 1000)
        reason: str | None = None
        if not target.apple_tv_id:
            reason = "internal"
        elif target.last_attempt_at and now - target.last_attempt_at < self.cooldown_ms:
            log_event("wake_coalesced", target=target.id)
        else:
            # Persist the cooldown before touching the receiver. A crash after launch but before Edge ACK must not
            # immediately relaunch Den when the level-triggered event is delivered again.
            self.state.mark_attempt(target, [], at=now)

            async def report(
                stage: str, status: str, elapsed: int | None, why: str | None
            ) -> None:
                async with self._lock:
                    current = self._groups[target.id][-1]
                self.state.record_stage(target.id, stage, status, why)
                await self.edge.stage(
                    Stage(
                        current.id,
                        target.id,
                        stage,
                        status,
                        elapsed_ms=elapsed,
                        reason=why,
                    )
                )

            try:
                reason = await self.apple.wake_and_launch(target, report)
            except Exception:
                reason = "internal"
        async with self._lock:
            group = self._groups[target.id]
            ids = [item.id for item in group]
            self.state.mark_attempt(target, ids)
            self._groups.pop(target.id)
        if reason:
            self.state.record_stage(target.id, "receiver-ready", "error", reason)
            for item in group:
                await self.edge.stage(
                    Stage(item.id, target.id, "receiver-ready", "error", reason=reason)
                )
        await self.edge.ack(ids)
        log_event(
            "wake_complete",
            target=target.id,
            status="error" if reason else "ok",
            reason=reason,
        )

    async def close(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
