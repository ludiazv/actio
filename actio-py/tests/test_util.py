"""`_util.py`: whole-second ISO, a parser as strict as the schema, sortable ULIDs, 128-bit nonces.

The patterns are read out of the package data (`common.schema.json` through `importlib.resources`),
never retyped: a format that the schema rejects is a format the protocol does not have.
"""

from __future__ import annotations

import datetime
import json
import re
import unittest
from importlib import resources
from typing import Any
from unittest import mock

from actio._util import CROCKFORD, format_iso, nonce, parse_iso, ulid


def schema_defs() -> dict[str, Any]:
    folder = resources.files("actio.schemas")
    text = folder.joinpath("common.schema.json").read_text(encoding="utf-8")
    data: dict[str, Any] = json.loads(text)
    defs: dict[str, Any] = data["$defs"]
    return defs


class FormatIsoTest(unittest.TestCase):
    def test_what_it_emits_is_what_the_schema_accepts(self) -> None:
        pattern = re.compile(schema_defs()["datetime"]["pattern"])
        for value in (0, 1, 123456789, 1_792_836_000, 1_792_836_000.999):
            self.assertIsNotNone(pattern.fullmatch(format_iso(value)), value)

    def test_the_fraction_is_floored_not_rounded(self) -> None:
        self.assertEqual(format_iso(1_792_836_000.999), "2026-10-24T10:00:00Z")
        self.assertEqual(format_iso(1_792_836_000.001), "2026-10-24T10:00:00Z")

    def test_round_trip_is_the_floor(self) -> None:
        for value in (0, 1, 123456789, 1_792_836_000.999):
            self.assertEqual(parse_iso(format_iso(value)), int(value // 1))


class ParseIsoTest(unittest.TestCase):
    def test_a_valid_string_is_seconds(self) -> None:
        self.assertEqual(parse_iso("2026-10-24T10:00:00Z"), 1_792_836_000.0)

    def test_a_fraction_an_offset_and_a_dead_date_are_none(self) -> None:
        self.assertIsNone(parse_iso("2026-10-24T10:00:00.000Z"))
        self.assertIsNone(parse_iso("2026-10-24T10:00:00+00:00"))
        self.assertIsNone(parse_iso("2026-10-24T12:00:00+02:00"))
        self.assertIsNone(parse_iso("2026-02-30T00:00:00Z"))

    def test_non_strings_are_none_and_nothing_raises(self) -> None:
        self.assertIsNone(parse_iso(None))
        self.assertIsNone(parse_iso(123))
        self.assertIsNone(parse_iso(1_792_836_000.5))
        self.assertIsNone(parse_iso(["2026-10-24T10:00:00Z"]))
        self.assertIsNone(parse_iso(datetime.datetime(2026, 10, 24, 10)))


class UlidTest(unittest.TestCase):
    def test_length_and_alphabet(self) -> None:
        for _ in range(100):
            value = ulid()
            self.assertEqual(len(value), 26, value)
            self.assertTrue(set(value) <= set(CROCKFORD), value)

    def test_monotonic_across_a_millisecond_boundary(self) -> None:
        with mock.patch("actio._util.time.time", return_value=1_700_000_000.0):
            earlier = ulid()
        with mock.patch("actio._util.time.time", return_value=1_700_000_000.5):
            later = ulid()
        self.assertLess(earlier, later)


class NonceTest(unittest.TestCase):
    def test_length_alphabet_and_uniqueness_over_ten_thousand_draws(self) -> None:
        pattern = re.compile(schema_defs()["nonce"]["pattern"])
        seen: set[str] = set()
        for _ in range(10_000):
            value = nonce()
            self.assertIsNotNone(pattern.fullmatch(value), value)
            seen.add(value)
        self.assertEqual(len(seen), 10_000)


if __name__ == "__main__":
    unittest.main()
