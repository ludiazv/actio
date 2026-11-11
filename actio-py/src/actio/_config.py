"""`ActioConfig`: defaults, the environment, and the injectable clock (*Timers and thresholds*, D-14).

Durations are **seconds** as `float`, as everywhere in asyncio, where `actio-ts` uses milliseconds.
The three environment overrides for protocol timers are given in **seconds**, per the spec.
Precedence: defaults < environment < keyword overrides. Bad values raise `ValueError` here, which
`load_config` callers see at `start()` — a bad environment must not be silently ignored.
"""

from __future__ import annotations

import dataclasses
import math
import os
import time
from collections.abc import Callable, Mapping
from typing import Any, Final

ENV_ROOT: Final = "ACTIO_DIR"
ENV_SYNC: Final = "ACTIO_SYNC"
ENV_HEARTBEAT: Final = "ACTIO_HEARTBEAT"
ENV_CONFIRM_TIMEOUT: Final = "ACTIO_CONFIRM_TIMEOUT"
ENV_CANCEL_WAIT: Final = "ACTIO_CANCEL_WAIT"

DEFAULT_SYNC: Final = "none"
DEFAULT_HEARTBEAT_INTERVAL: Final = 30.0
DEFAULT_CONFIRM_TIMEOUT: Final = 120.0
DEFAULT_CANCEL_WAIT_TIMEOUT: Final = 60.0
DEFAULT_ANNOUNCE_DELAY: Final = 0.1
DEFAULT_NUNTIUM_DEBOUNCE: Final = 1.0
DEFAULT_STOP_GRACE: Final = 30.0


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class ActioConfig:
    """Every timer of one agent, in seconds. `clock` is the seam a test injects a fake clock through."""

    root: str
    sync: str = DEFAULT_SYNC
    heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL
    confirm_timeout: float = DEFAULT_CONFIRM_TIMEOUT
    cancel_wait_timeout: float = DEFAULT_CANCEL_WAIT_TIMEOUT
    announce_delay: float = DEFAULT_ANNOUNCE_DELAY
    nuntium_debounce: float = DEFAULT_NUNTIUM_DEBOUNCE
    stop_grace: float = DEFAULT_STOP_GRACE
    clock: Callable[[], float] = time.time


def _env_seconds(env: Mapping[str, str], name: str) -> float | None:
    """One duration in seconds; unset or empty keeps the default, anything else is an error."""
    raw = env.get(name)
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive number of seconds, got {raw!r}") from None
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive number of seconds, got {raw!r}")
    return value


def load_config(
    *,
    env: Mapping[str, str] | None = None,
    root: str | None = None,
    sync: str | None = None,
    heartbeat_interval: float | None = None,
    confirm_timeout: float | None = None,
    cancel_wait_timeout: float | None = None,
    announce_delay: float | None = None,
    nuntium_debounce: float | None = None,
    stop_grace: float | None = None,
    clock: Callable[[], float] | None = None,
) -> ActioConfig:
    """The effective config: defaults < environment < overrides, validated (D-14)."""
    source: Mapping[str, str] = os.environ if env is None else env

    if root is None:
        root = source[ENV_ROOT] if ENV_ROOT in source else f"{source.get('HOME', '')}/.actio"
    if sync is None:
        sync = source[ENV_SYNC] if ENV_SYNC in source else DEFAULT_SYNC
    heartbeat = (
        heartbeat_interval
        if heartbeat_interval is not None
        else _env_seconds(source, ENV_HEARTBEAT)
    )
    confirm = (
        confirm_timeout
        if confirm_timeout is not None
        else _env_seconds(source, ENV_CONFIRM_TIMEOUT)
    )
    cancel_wait = (
        cancel_wait_timeout
        if cancel_wait_timeout is not None
        else _env_seconds(source, ENV_CANCEL_WAIT)
    )

    config = ActioConfig(
        root=root,
        sync=sync,
        heartbeat_interval=DEFAULT_HEARTBEAT_INTERVAL if heartbeat is None else heartbeat,
        confirm_timeout=DEFAULT_CONFIRM_TIMEOUT if confirm is None else confirm,
        cancel_wait_timeout=DEFAULT_CANCEL_WAIT_TIMEOUT if cancel_wait is None else cancel_wait,
        announce_delay=DEFAULT_ANNOUNCE_DELAY if announce_delay is None else announce_delay,
        nuntium_debounce=nuntium_debounce
        if nuntium_debounce is not None
        else DEFAULT_NUNTIUM_DEBOUNCE,
        stop_grace=DEFAULT_STOP_GRACE if stop_grace is None else stop_grace,
        clock=clock if clock is not None else time.time,
    )
    _validate(config)
    return config


def _validate(config: ActioConfig) -> None:
    if not isinstance(config.root, str) or not config.root:
        raise ValueError("config.root must be a non-empty string")
    if not isinstance(config.sync, str):
        raise ValueError("config.sync must be a string")
    for name in (
        "heartbeat_interval",
        "confirm_timeout",
        "cancel_wait_timeout",
        "announce_delay",
        "nuntium_debounce",
        "stop_grace",
    ):
        value: Any = getattr(config, name)
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError(f"config.{name} must be a number >= 0 (seconds), got {value!r}")
    if not callable(config.clock):
        raise ValueError("config.clock must be a callable returning seconds")
