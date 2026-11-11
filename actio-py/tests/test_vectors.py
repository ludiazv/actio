"""The copied conformance vectors: every `valid` case validates, every `invalid` case fails, and
every rejection reason is one line of at most `LIMIT` characters, with no trailing period, and it
quotes none of the offending values (D-15).

The vector folders are the ten kinds: `actio`, `announce`, `cancellation`, `confirmation`, `diarium`,
`error`, `request`, `response`, `spec`, `status`. There is no `common` folder: `common` is package
data, not a kind, and it is pinned in `tests/test_schemas_pin.py`.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from actio._validate import LIMIT, SchemaKind, _validator, validate

VECTORS = Path(__file__).resolve().parents[1] / "tests" / "vectors"
KINDS: tuple[SchemaKind, ...] = (
    "actio",
    "announce",
    "cancellation",
    "confirmation",
    "diarium",
    "error",
    "request",
    "response",
    "spec",
    "status",
)
TOTAL = 111


def docs(kind: SchemaKind, case: str) -> list[tuple[Path, object]]:
    return [
        (path, json.loads(path.read_text(encoding="utf-8")))
        for path in sorted((VECTORS / kind / case).glob("*.json"))
    ]


class VectorsTest(unittest.TestCase):
    def test_the_count_is_pinned(self) -> None:
        self.assertEqual(sum(1 for _ in VECTORS.rglob("*.json")), TOTAL)

    def test_every_kind_folder_has_both_cases(self) -> None:
        for kind in KINDS:
            self.assertGreaterEqual(len(docs(kind, "valid")), 1, kind)
            self.assertGreaterEqual(len(docs(kind, "invalid")), 1, kind)

    def test_every_valid_vector_validates(self) -> None:
        for kind in KINDS:
            for path, doc in docs(kind, "valid"):
                self.assertIsNone(validate(kind, doc), f"{path}: {validate(kind, doc)}")

    def test_every_invalid_vector_fails_with_a_clean_reason(self) -> None:
        for kind in KINDS:
            for path, doc in docs(kind, "invalid"):
                reason = validate(kind, doc)
                self.assertIsNotNone(reason, f"{path} validated: it must fail")
                assert reason is not None
                self.assertLessEqual(len(reason), LIMIT, f"{path}: {reason!r}")
                self.assertNotIn("\n", reason, f"{path}: {reason!r}")
                self.assertFalse(reason.endswith("."), f"{path}: {reason!r}")
                # the offending value, when the first error points at a string: it stays out
                first = next(iter(_validator(kind).iter_errors(doc)))
                if isinstance(first.instance, str) and len(first.instance) >= 8:
                    self.assertNotIn(first.instance, reason, f"{path}: {reason!r}")


if __name__ == "__main__":
    unittest.main()
