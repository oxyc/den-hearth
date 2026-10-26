from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import stat
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import web
from pyatv.const import Protocol

from den_hearth_controller.apple_tv import AppleTVClient
from den_hearth_controller.coordinator import Coordinator
from den_hearth_controller.edge import EdgeClient
from den_hearth_controller.ipc import RegistrationServer
from den_hearth_controller.models import WakeEvent
from den_hearth_controller.state import StateStore


REGISTRATION = {
    "v": 1,
    "deviceId": "0011223344556677",
    "name": "Living Room",
    "householdInbox": "ab" * 32,
    "assistantLane": "cd" * 16,
    "bundleId": "fi.oxy.den.TEAM",
}


class StateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "state"
        self.store = StateStore(self.root)

    def tearDown(self):
        self.temp.cleanup()

    def test_state_is_private_atomic_and_health_is_redacted(self):
        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.store.path.stat().st_mode), 0o600)
        controller = self.store.import_credential("1" * 32 + "." + "A" * 43)
        self.assertEqual(controller, "1" * 32)
        target = self.store.upsert_registration(REGISTRATION)
        self.store.map_target(
            target.device_id, "physical-id", "192.0.2.4", target.bundle_id
        )
        self.store.set_health(running=True, edgeConnected=True)
        health = self.store.health()
        self.assertNotIn("1" * 32 + "." + "A" * 43, json.dumps(health))
        self.assertEqual(health["mappedTargets"], 1)
        self.assertFalse(list(self.root.glob(".controller.json.*")))

    def test_registration_preserves_physical_mapping_and_rotates_logical_ids(self):
        target = self.store.upsert_registration(REGISTRATION)
        self.assertEqual(self.store.edge_targets(), [])
        mapped = self.store.map_target(
            target.device_id, "physical-id", "192.0.2.4", target.bundle_id
        )
        changed = dict(
            REGISTRATION,
            householdInbox="ef" * 32,
            bundleId="fi.oxy.den.NEWTEAM",
        )
        updated = self.store.upsert_registration(changed)
        self.assertEqual(updated.id, mapped.id)
        self.assertEqual(updated.apple_tv_id, "physical-id")
        self.assertEqual(updated.inbox, "ef" * 32)
        self.assertEqual(updated.bundle_id, "fi.oxy.den.NEWTEAM")
        self.assertEqual(updated.selected_bundle_id, "fi.oxy.den.TEAM")
        self.assertEqual([item.id for item in self.store.edge_targets()], [target.id])

    def test_registration_refuses_queue_collision(self):
        self.store.upsert_registration(REGISTRATION)
        with self.assertRaisesRegex(ValueError, "target_conflict"):
            self.store.upsert_registration(
                dict(REGISTRATION, deviceId="8899aabbccddeeff")
            )

    def test_completed_events_are_durable_and_bounded(self):
        target = self.store.upsert_registration(REGISTRATION)
        for index in range(300):
            self.store.mark_attempt(target, [f"{index:032x}"], at=index + 1)
        self.assertEqual(len(self.store._read()["completed"]), 256)
        self.assertTrue(self.store.completed(f"{299:032x}"))
        self.assertFalse(self.store.completed(f"{0:032x}"))

    def test_rejects_public_state_file(self):
        os.chmod(self.store.path, 0o644)
        with self.assertRaises(PermissionError):
            StateStore(self.root)


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.store = StateStore(root / "state", root / "run" / "controller.sock")

    async def asyncTearDown(self):
        self.temp.cleanup()

    async def test_permissioned_registration_socket(self):
        server = RegistrationServer(self.store)
        await server.start()
        self.assertEqual(stat.S_IMODE(self.store.socket_path.stat().st_mode), 0o600)
        reader, writer = await asyncio.open_unix_connection(self.store.socket_path)
        writer.write(
            json.dumps({"op": "upsert", "target": REGISTRATION}).encode() + b"\n"
        )
        await writer.drain()
        answer = json.loads(await reader.readline())
        self.assertTrue(answer["ok"])
        self.assertEqual(len(self.store.targets()), 1)
        writer.close()
        await writer.wait_closed()
        await server.close()

    async def test_find_accepts_discovery_identifier_from_all_identifiers(self):
        class Config:
            identifier = "companion-id"
            all_identifiers = {"companion-id", "36:FB:AA:BB:CC:DD"}

            @staticmethod
            def get_service(protocol):
                return object() if protocol == Protocol.Companion else None

        client = AppleTVClient(Path(self.temp.name) / "pyatv.conf")
        storage = object()
        client._storage = AsyncMock(return_value=storage)
        with patch(
            "den_hearth_controller.apple_tv.pyatv.scan",
            new=AsyncMock(return_value=[Config()]),
        ) as scan:
            config, found_storage = await client._find("36:FB:AA:BB:CC:DD")

        self.assertEqual(config.identifier, "companion-id")
        self.assertIs(found_storage, storage)
        self.assertNotIn("identifier", scan.await_args.kwargs)
        self.assertNotIn("protocol", scan.await_args.kwargs)

    async def test_find_address_fallback_preserves_all_identifiers(self):
        class Config:
            identifier = "companion-id"
            all_identifiers = {"companion-id", "36:FB:AA:BB:CC:DD"}

            @staticmethod
            def get_service(protocol):
                return object() if protocol == Protocol.Companion else None

        client = AppleTVClient(Path(self.temp.name) / "pyatv.conf")
        client._storage = AsyncMock(return_value=object())
        with patch(
            "den_hearth_controller.apple_tv.pyatv.scan",
            new=AsyncMock(side_effect=[[], [Config()]]),
        ) as scan:
            config, _ = await client._find(
                "36:FB:AA:BB:CC:DD", "192.0.2.4"
            )

        self.assertEqual(config.identifier, "companion-id")
        self.assertEqual(scan.await_count, 2)
        for call in scan.await_args_list:
            self.assertNotIn("protocol", call.kwargs)
        self.assertEqual(scan.await_args_list[1].kwargs["hosts"], ["192.0.2.4"])

    async def test_find_rejects_matching_identifier_without_companion(self):
        class Config:
            identifier = "36:FB:AA:BB:CC:DD"
            all_identifiers = {identifier}

            @staticmethod
            def get_service(protocol):
                return None

        client = AppleTVClient(Path(self.temp.name) / "pyatv.conf")
        client._storage = AsyncMock(return_value=object())
        with patch(
            "den_hearth_controller.apple_tv.pyatv.scan",
            new=AsyncMock(return_value=[Config()]),
        ):
            with self.assertRaisesRegex(LookupError, "offline"):
                await client._find("36:FB:AA:BB:CC:DD")

    async def test_pair_awaits_async_close(self):
        storage_path = Path(self.temp.name) / "pyatv.conf"
        storage_path.touch()
        client = AppleTVClient(storage_path)
        config = MagicMock(
            identifier="companion-id", name="Living Room", address="192.0.2.4"
        )
        storage = MagicMock()
        storage.save = AsyncMock()
        client._find = AsyncMock(return_value=(config, storage))
        pairing = MagicMock(has_paired=True)
        pairing.begin = AsyncMock()
        pairing.finish = AsyncMock()
        pairing.close = AsyncMock()

        with patch(
            "den_hearth_controller.apple_tv.pyatv.pair",
            new=AsyncMock(return_value=pairing),
        ):
            result = await client.pair("36:FB:AA:BB:CC:DD", lambda: "1234")

        self.assertEqual(result["id"], "companion-id")
        pairing.pin.assert_called_once_with(1234)
        pairing.close.assert_awaited_once_with()

    async def test_edge_poll_uses_least_privilege_header_and_bounded_shape(self):
        seen = {}

        async def poll(request):
            seen["header"] = request.headers.get("x-den-wake-controller")
            seen["body"] = await request.json()
            target = seen["body"]["targets"][0]["id"]
            return web.json_response(
                {
                    "v": 1,
                    "serverTime": 2,
                    "acceptedTargets": [target],
                    "events": [
                        {
                            "event": "2" * 32,
                            "target": target,
                            "sources": ["inbox"],
                            "firstAcceptedAt": 1,
                            "lastAcceptedAt": 2,
                        }
                    ],
                }
            )

        app = web.Application()
        app.router.add_post("/wake/poll", poll)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        target = self.store.upsert_registration(REGISTRATION)
        edge = EdgeClient(
            f"http://127.0.0.1:{port}", "1" * 32, "1" * 32 + "." + "A" * 43
        )
        import aiohttp

        async with aiohttp.ClientSession() as session:
            events = await edge.poll(session, [target])
        await runner.cleanup()
        self.assertEqual(seen["header"], "1" * 32 + "." + "A" * 43)
        self.assertNotIn("credential", json.dumps(seen["body"]))
        self.assertEqual(
            seen["body"]["targets"][0]["capabilities"], ["app-launch", "companion-wake"]
        )
        self.assertEqual(events[0].sources, ("inbox",))

    async def test_single_flight_coalesces_and_dedupes_durably(self):
        target = self.store.upsert_registration(REGISTRATION)
        self.store.map_target(
            target.device_id, "physical", "192.0.2.4", target.bundle_id
        )
        target = self.store.targets()[0]

        class FakeEdge:
            def __init__(self):
                self.acks = []
                self.stages = []

            async def ack(self, ids):
                self.acks.extend(ids)

            async def stage(self, stage):
                self.stages.append(stage)

        class FakeApple:
            def __init__(self):
                self.calls = 0
                self.started = asyncio.Event()
                self.release = asyncio.Event()

            async def wake_and_launch(self, target, stage):
                self.calls += 1
                self.started.set()
                await self.release.wait()
                return None

        edge, apple = FakeEdge(), FakeApple()
        coordinator = Coordinator(self.store, edge, apple)
        now = int(time.time() * 1000)
        first = WakeEvent("1" * 32, target.id, ("inbox",), now, now)
        second = WakeEvent("2" * 32, target.id, ("assistant",), now, now)
        await coordinator.submit(first)
        await apple.started.wait()
        self.assertIsNotNone(self.store.targets()[0].last_attempt_at)
        await coordinator.submit(second)
        apple.release.set()
        await coordinator.close()
        self.assertEqual(apple.calls, 1)
        self.assertCountEqual(edge.acks, [first.id, second.id])
        self.assertTrue(self.store.completed(first.id))
        self.assertTrue(self.store.completed(second.id))
        await coordinator.submit(first)
        self.assertEqual(apple.calls, 1)

    async def test_stale_event_never_wakes(self):
        target = self.store.upsert_registration(REGISTRATION)

        class Edge:
            def __init__(self):
                self.acks = []
                self.stages = []

            async def ack(self, ids):
                self.acks.extend(ids)

            async def stage(self, stage):
                self.stages.append(stage)

        class Apple:
            async def wake_and_launch(self, target, stage):
                raise AssertionError("must not wake")

        edge = Edge()
        coordinator = Coordinator(self.store, edge, Apple())
        old = int(time.time() * 1000) - 120_001
        wake = WakeEvent("3" * 32, target.id, ("inbox",), old, old)
        await coordinator.submit(wake)
        self.assertEqual(edge.acks, [wake.id])
        self.assertEqual(edge.stages[-1].reason, "stale-event")


if __name__ == "__main__":
    unittest.main()
