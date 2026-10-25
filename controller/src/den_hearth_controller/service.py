from __future__ import annotations

import asyncio
import signal

from .apple_tv import AppleTVClient
from .coordinator import Coordinator
from .edge import EdgeClient
from .ipc import RegistrationServer
from .state import StateStore


async def run_service(state: StateStore, edge_url: str) -> None:
    auth = state.credential()
    if auth is None:
        raise ValueError("controller_not_enrolled")
    controller, credential = auth
    edge = EdgeClient(edge_url, controller, credential)
    apple = AppleTVClient(state.pyatv_path)
    coordinator = Coordinator(state, edge, apple)
    registration = RegistrationServer(state)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    state.set_health(running=True, edgeConnected=False, edgeError=None)
    await registration.start()
    task = asyncio.create_task(
        edge.run(state.edge_targets, coordinator.submit, state.set_health, stop),
        name="edge-poll",
    )
    try:
        await stop.wait()
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await registration.close()
        await coordinator.close()
        state.set_health(running=False, edgeConnected=False)
