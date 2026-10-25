from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import os
from pathlib import Path
import sys
import time
from typing import Any

import pyatv

from .apple_tv import AppleTVClient, PYATV_ERRORS
from .logging import configure
from .models import HEX16
from .service import run_service
from .state import StateStore


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="den-hearth-controller")
    result.add_argument(
        "--state-dir",
        type=Path,
        default=Path(os.environ.get("DEN_CONTROLLER_STATE", "/state")),
    )
    result.add_argument(
        "--socket",
        type=Path,
        default=Path(
            os.environ.get(
                "DEN_CONTROLLER_SOCKET", "/run/den-controller/controller.sock"
            )
        ),
    )
    result.add_argument("--verbose", action="store_true")
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser("discover", help="discover Companion-capable Apple TVs")
    pair = commands.add_parser("pair", help="pair Companion and persist credentials")
    pair.add_argument("--id", required=True)
    apps = commands.add_parser("apps", help="list launchable installed apps")
    apps.add_argument("--id", required=True)
    apps.add_argument("--address")
    enroll = commands.add_parser(
        "enroll", help="import Edge's one-time controller credential from a prompt"
    )
    enroll.add_argument("--credential-file", type=Path)
    target = commands.add_parser(
        "target", help="map a registered logical TV to a paired Apple TV"
    )
    target.add_argument("--device-id", required=True)
    target.add_argument("--apple-tv-id", required=True)
    target.add_argument("--address")
    target.add_argument("--bundle-id")
    register = commands.add_parser(
        "register", help="send one target registration to the local Unix socket"
    )
    register.add_argument(
        "--file", type=Path, help="registration JSON; default is stdin"
    )
    health = commands.add_parser("health", help="print redacted controller health")
    health.add_argument("--require-running", action="store_true")
    run = commands.add_parser(
        "run", help="run the Edge poll and local registration socket"
    )
    run.add_argument("--edge", default=os.environ.get("DEN_EDGE_URL"))
    return result


def output(value: Any) -> None:
    print(json.dumps(value, sort_keys=True, separators=(",", ":")))


async def execute(args: argparse.Namespace, state: StateStore) -> int:
    apple = AppleTVClient(state.pyatv_path)
    if args.command == "discover":
        output({"devices": await apple.discover()})
        return 0
    if args.command == "pair":
        result = await apple.pair(
            args.id, lambda: input("PIN shown on Apple TV: ").strip()
        )
        output(result)
        return 0
    if args.command == "apps":
        apps, address = await apple.apps(args.id, args.address)
        output({"address": address, "apps": apps})
        return 0
    if args.command == "enroll":
        if args.credential_file:
            if args.credential_file.stat().st_mode & 0o077:
                raise PermissionError("credential_file_permissions")
            credential = args.credential_file.read_text().strip()
        else:
            credential = getpass.getpass("One-time controller credential: ").strip()
        controller = state.import_credential(credential)
        output({"controller": controller, "enrolled": True})
        return 0
    if args.command == "target":
        if not HEX16.fullmatch(args.device_id):
            raise ValueError("invalid_device")
        current = next(
            (
                target
                for target in state.targets()
                if target.device_id == args.device_id
            ),
            None,
        )
        if current is None:
            raise ValueError("unknown_target")
        apps, address = await apple.apps(args.apple_tv_id, args.address)
        bundle = args.bundle_id or current.bundle_id
        if not any(item["bundleId"] == bundle for item in apps):
            raise ValueError("app_missing")
        target = state.map_target(args.device_id, args.apple_tv_id, address, bundle)
        output(
            {
                "target": target.id,
                "deviceId": target.device_id,
                "bundleId": target.selected_bundle_id,
                "mapped": True,
            }
        )
        return 0
    if args.command == "register":
        raw = args.file.read_bytes() if args.file else sys.stdin.buffer.read(8193)
        if len(raw) > 8192:
            raise ValueError("invalid_request")
        target = json.loads(raw)
        reader, writer = await asyncio.open_unix_connection(state.socket_path)
        writer.write(
            json.dumps(
                {"op": "upsert", "target": target}, separators=(",", ":")
            ).encode()
            + b"\n"
        )
        await writer.drain()
        answer = await asyncio.wait_for(reader.readline(), 3)
        writer.close()
        await writer.wait_closed()
        response = json.loads(answer)
        output(response)
        return 0 if response.get("ok") else 2
    if args.command == "health":
        health = state.health()
        output(health)
        fresh = (
            isinstance(health.get("updatedAt"), int)
            and int(time.time() * 1000) - health["updatedAt"] < 90_000
        )
        return (
            1
            if args.require_running and (not health.get("running") or not fresh)
            else 0
        )
    if args.command == "run":
        if not args.edge:
            raise ValueError("edge_url_required")
        await run_service(state, args.edge)
        return 0
    raise AssertionError(args.command)


def main() -> None:
    args = parser().parse_args()
    configure(args.verbose)
    try:
        state = StateStore(args.state_dir, args.socket)
        code = asyncio.run(execute(args, state))
    except (ValueError, PermissionError, LookupError) as exc:
        print(str(exc), file=sys.stderr)
        code = 2
    except pyatv.exceptions.AuthenticationError:
        print("pairing_revoked", file=sys.stderr)
        code = 2
    except PYATV_ERRORS:
        print("protocol_error", file=sys.stderr)
        code = 2
    except OSError:
        print("io_error", file=sys.stderr)
        code = 2
    except KeyboardInterrupt:
        code = 130
    raise SystemExit(code)


if __name__ == "__main__":
    main()
