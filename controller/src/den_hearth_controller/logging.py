from __future__ import annotations

import json
import logging
from typing import Any


LOG = logging.getLogger("den-hearth-controller")
ALLOWED = {
    "event",
    "target",
    "stage",
    "status",
    "reason",
    "attempt",
    "delay",
    "count",
    "device",
}


def configure(verbose: bool = False) -> None:
    # pyatv debug output can contain raw service properties and credentials. Keep third-party logging silent and
    # raise only this module's allowlisted structured events to the requested level.
    logging.basicConfig(level=logging.CRITICAL, format="%(message)s")
    LOG.setLevel(logging.DEBUG if verbose else logging.INFO)


def event(name: str, **fields: Any) -> None:
    safe = {key: value for key, value in fields.items() if key in ALLOWED}
    safe["message"] = name
    LOG.info(json.dumps(safe, sort_keys=True, separators=(",", ":")))
