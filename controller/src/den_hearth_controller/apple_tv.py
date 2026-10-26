from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import inspect
from ipaddress import ip_address
import os
from pathlib import Path
from typing import Any

import pyatv
from pyatv.const import Protocol
from pyatv.storage.file_storage import FileStorage

from .models import Target


StageFn = Callable[[str, str, int | None, str | None], Awaitable[None]]
PYATV_ERRORS = tuple(
    value
    for value in vars(pyatv.exceptions).values()
    if isinstance(value, type) and issubclass(value, Exception)
)


class AppleTVClient:
    def __init__(self, storage_path: Path, *, scan_timeout: int = 5):
        self.storage_path = storage_path
        self.scan_timeout = scan_timeout

    @staticmethod
    async def _close(atv) -> None:
        tasks = atv.close()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _storage(self) -> FileStorage:
        if self.storage_path.exists() and self.storage_path.stat().st_mode & 0o077:
            raise PermissionError("pyatv_storage_permissions")
        storage = FileStorage(str(self.storage_path), asyncio.get_running_loop())
        await storage.load()
        return storage

    async def discover(self) -> list[dict[str, Any]]:
        storage = await self._storage()
        configs = await pyatv.scan(
            asyncio.get_running_loop(), timeout=self.scan_timeout, storage=storage
        )
        return [
            {
                "name": config.name,
                "id": config.identifier,
                "address": str(config.address),
                "deepSleep": config.deep_sleep,
            }
            for config in configs
            if config.get_service(Protocol.Companion) is not None
        ]

    @staticmethod
    def _matches(config, identifier: str) -> bool:
        return (
            config.get_service(Protocol.Companion) is not None
            and (
                config.identifier == identifier
                or identifier in config.all_identifiers
            )
        )

    async def _find(self, identifier: str, address: str | None = None):
        storage = await self._storage()
        configs = await pyatv.scan(
            asyncio.get_running_loop(),
            timeout=self.scan_timeout,
            storage=storage,
        )
        configs = [config for config in configs if self._matches(config, identifier)]
        if not configs and address:
            ip_address(address)
            configs = await pyatv.scan(
                asyncio.get_running_loop(),
                timeout=self.scan_timeout,
                hosts=[address],
                storage=storage,
            )
            configs = [config for config in configs if self._matches(config, identifier)]
        if not configs:
            raise LookupError("offline")
        return configs[0], storage

    async def pair(
        self, identifier: str, pin_reader: Callable[[], str]
    ) -> dict[str, str]:
        config, storage = await self._find(identifier)
        pairing = await pyatv.pair(
            config, Protocol.Companion, asyncio.get_running_loop(), storage=storage
        )
        try:
            await pairing.begin()
            pairing.pin(int(pin_reader()))
            await pairing.finish()
            if not pairing.has_paired:
                raise PermissionError("pairing_failed")
            await storage.save()
            os.chmod(self.storage_path, 0o600)
        finally:
            closing = pairing.close()
            if inspect.isawaitable(closing):
                await closing
        return {
            "id": config.identifier,
            "name": config.name,
            "address": str(config.address),
        }

    async def apps(
        self, identifier: str, address: str | None = None
    ) -> tuple[list[dict[str, str | None]], str]:
        config, storage = await self._find(identifier, address)
        atv = await asyncio.wait_for(
            pyatv.connect(
                config,
                asyncio.get_running_loop(),
                protocol=Protocol.Companion,
                storage=storage,
            ),
            10,
        )
        try:
            apps = await asyncio.wait_for(atv.apps.app_list(), 10)
            return (
                [{"name": app.name, "bundleId": app.identifier} for app in apps],
                str(config.address),
            )
        finally:
            await self._close(atv)

    async def wake_and_launch(self, target: Target, stage: StageFn) -> str | None:
        if not target.apple_tv_id or not target.selected_bundle_id:
            return "internal"
        started = asyncio.get_running_loop().time()
        try:
            config, storage = await asyncio.wait_for(
                self._find(target.apple_tv_id, target.last_address), 12
            )
        except LookupError:
            return "offline"
        except (asyncio.TimeoutError, ValueError):
            return "connect-timeout"

        atv = None
        try:
            await stage("wake-requested", "started", None, None)
            atv = await asyncio.wait_for(
                pyatv.connect(
                    config,
                    asyncio.get_running_loop(),
                    protocol=Protocol.Companion,
                    storage=storage,
                ),
                10,
            )
            try:
                await asyncio.wait_for(atv.remote_control.wakeup(), 15)
            except asyncio.TimeoutError:
                # Some tvOS versions do not answer the wake command. Reconnect is the readiness proof.
                pass
            await stage(
                "receiver-awake",
                "ok",
                int((asyncio.get_running_loop().time() - started) * 1000),
                None,
            )
            await self._close(atv)
            atv = None

            config, storage = await asyncio.wait_for(
                self._find(target.apple_tv_id, str(config.address)), 12
            )
            atv = await asyncio.wait_for(
                pyatv.connect(
                    config,
                    asyncio.get_running_loop(),
                    protocol=Protocol.Companion,
                    storage=storage,
                ),
                10,
            )
            await stage(
                "companion-connected",
                "ok",
                int((asyncio.get_running_loop().time() - started) * 1000),
                None,
            )
            apps = await asyncio.wait_for(atv.apps.app_list(), 10)
            if not any(app.identifier == target.selected_bundle_id for app in apps):
                await stage("app-known", "error", None, "app-missing")
                return "app-missing"
            await stage(
                "app-known",
                "ok",
                int((asyncio.get_running_loop().time() - started) * 1000),
                None,
            )
            await stage("launch-requested", "started", None, None)
            await asyncio.wait_for(atv.apps.launch_app(target.selected_bundle_id), 10)
            await stage(
                "launch-requested",
                "ok",
                int((asyncio.get_running_loop().time() - started) * 1000),
                None,
            )
            await stage(
                "receiver-ready",
                "ok",
                int((asyncio.get_running_loop().time() - started) * 1000),
                None,
            )
            return None
        except asyncio.TimeoutError:
            return "launch-timeout" if atv is not None else "connect-timeout"
        except pyatv.exceptions.AuthenticationError:
            return "pairing-revoked"
        except pyatv.exceptions.ConnectionFailedError:
            return "offline"
        except PYATV_ERRORS:
            return "protocol-error"
        finally:
            if atv is not None:
                await self._close(atv)
