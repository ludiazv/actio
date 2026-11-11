"""`_config.py`: defaults, the environment, precedence, and the clock seam (D-14).

Durations are **seconds** here, as everywhere in asyncio; the three protocol timers read from the
environment are given in seconds too, as the spec's *Timers and thresholds* says. A bad value is an
error at `load_config`, never a silent default.
"""

from __future__ import annotations

import dataclasses
import time
import unittest

from actio._config import (
    DEFAULT_ANNOUNCE_DELAY,
    DEFAULT_CANCEL_WAIT_TIMEOUT,
    DEFAULT_CONFIRM_TIMEOUT,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_NUNTIUM_DEBOUNCE,
    DEFAULT_STOP_GRACE,
    DEFAULT_SYNC,
    ActioConfig,
    load_config,
)

EMPTY: dict[str, str] = {}


class DefaultsTest(unittest.TestCase):
    def test_the_defaults_are_the_reference_s_in_seconds(self) -> None:
        config = load_config(env=EMPTY, root="/tmp/.actio")
        self.assertEqual(config.sync, DEFAULT_SYNC)
        self.assertEqual(config.heartbeat_interval, DEFAULT_HEARTBEAT_INTERVAL)
        self.assertEqual(config.confirm_timeout, DEFAULT_CONFIRM_TIMEOUT)
        self.assertEqual(config.cancel_wait_timeout, DEFAULT_CANCEL_WAIT_TIMEOUT)
        self.assertEqual(config.announce_delay, DEFAULT_ANNOUNCE_DELAY)
        self.assertEqual(config.nuntium_debounce, DEFAULT_NUNTIUM_DEBOUNCE)
        self.assertEqual(config.stop_grace, DEFAULT_STOP_GRACE)
        self.assertIs(config.clock, time.time)

    def test_the_root_default_is_home(self) -> None:
        self.assertEqual(load_config(env={"HOME": "/home/ada"}).root, "/home/ada/.actio")
        self.assertEqual(load_config(env={}).root, "/.actio")

    def test_the_config_is_frozen_and_keyword_only(self) -> None:
        config = load_config(env=EMPTY, root="/tmp/.actio")
        with self.assertRaises(dataclasses.FrozenInstanceError):
            config.heartbeat_interval = 1  # type: ignore[misc]
        with self.assertRaises(TypeError):
            ActioConfig("/tmp/.actio")  # type: ignore[call-arg]


class EnvironmentTest(unittest.TestCase):
    def test_the_environment_sets_root_sync_and_three_timers(self) -> None:
        config = load_config(
            env={
                "ACTIO_DIR": "/srv/.actio",
                "ACTIO_SYNC": "delay:250",
                "ACTIO_HEARTBEAT": "5",
                "ACTIO_CONFIRM_TIMEOUT": "90",
                "ACTIO_CANCEL_WAIT": "10",
            }
        )
        self.assertEqual(config.root, "/srv/.actio")
        self.assertEqual(config.sync, "delay:250")
        self.assertEqual(config.heartbeat_interval, 5.0)
        self.assertEqual(config.confirm_timeout, 90.0)
        self.assertEqual(config.cancel_wait_timeout, 10.0)

    def test_empty_is_unset(self) -> None:
        config = load_config(env={"ACTIO_HEARTBEAT": "", "ACTIO_SYNC": ""})
        self.assertEqual(config.heartbeat_interval, DEFAULT_HEARTBEAT_INTERVAL)
        self.assertEqual(config.sync, "")

    def test_a_bad_number_is_an_error_not_a_default(self) -> None:
        for value in ("abc", "0", "-5", "inf", "nan"):
            with self.assertRaises(ValueError, msg=value):
                load_config(env={"ACTIO_HEARTBEAT": value})

    def test_overrides_beat_the_environment(self) -> None:
        config = load_config(
            env={"ACTIO_HEARTBEAT": "5", "ACTIO_DIR": "/env"},
            root="/override",
            heartbeat_interval=1.5,
        )
        self.assertEqual(config.root, "/override")
        self.assertEqual(config.heartbeat_interval, 1.5)

    def test_the_clock_is_the_seam_a_test_injects(self) -> None:
        ticks = iter([1000.0, 1001.0])
        config = load_config(env=EMPTY, clock=lambda: next(ticks))
        self.assertEqual(config.clock(), 1000.0)
        self.assertEqual(config.clock(), 1001.0)


class ValidationTest(unittest.TestCase):
    def test_an_empty_root_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            load_config(env={"ACTIO_DIR": ""})

    def test_a_negative_timer_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            load_config(env=EMPTY, announce_delay=-1)

    def test_a_non_callable_clock_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            load_config(env=EMPTY, clock=object())  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
