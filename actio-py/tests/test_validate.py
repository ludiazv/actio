"""The behaviour of `_validate`: the two kind rules (name vs body key), the one rejection reason
(D-15), the capability schemas (D-19), and the rules the schemas cannot express.
"""

from __future__ import annotations

import json
import logging
import unittest
from typing import Any, cast

import jsonschema
import referencing
from jsonschema.validators import Draft202012Validator

from actio._protocol import CancellationBody, Capability, Header, RequestBody, Spec
from actio._validate import (
    LIMIT,
    SchemaKind,
    _compile_peer,
    check_params,
    compile_capability,
    envelope_kind,
    file_kind,
    malformed_reason,
    name_matches,
    reason,
    same_header,
    schema,
    unique_capability_names,
    validate,
)

RID = "01J4ZZ" + "0" * 18 + "AB"
BAD_RID = "01J4ZZ" + "0" * 18 + "AI"  # an 'I' is not in the Crockford base32 alphabet


def header(**changes: Any) -> Header:
    doc: dict[str, Any] = {
        "rid": RID,
        "src": "alpha",
        "dst": "beta",
        "datetime": "2026-10-24T10:00:00Z",
        "type": "req",
        "actio_version": "0.1",
    }
    doc.update(changes)
    return cast(Header, doc)


def body() -> dict[str, Any]:
    """A request body that passes `request.schema.json`."""
    return {
        "ts": 1792836000,
        "nonce": "a" * 32,
        "signature": None,
        "name": "echo",
        "parameters": None,
        "timeout": 0,
        "attachments": [],
    }


def cap_with(parameters: Any, result: Any = True, name: str = "echo") -> Capability:
    return {
        "name": name,
        "description": "d",
        "parameters": parameters,
        "actio": {"result": result},
    }


def spec_with(names: list[str]) -> Spec:
    return {
        "id": "alpha",
        "actio_version": "0.1",
        "datetime": "2026-10-24T10:00:00Z",
        "description": "d",
        "accepts_commands_from": [],
        "capabilities": [
            {
                "name": n,
                "description": "d",
                "parameters": {"type": "object"},
                "actio": {"result": True},
            }
            for n in names
        ],
        "security_support": [],
        "security_posture": "trusted",
    }


class FileKindTest(unittest.TestCase):
    """The name rule: for the `actio validate` command only, where a loose file has no body key."""

    def test_the_table_of_names(self) -> None:
        self.assertEqual(file_kind("spec.json"), "spec")
        self.assertEqual(file_kind("status.json"), "status")
        self.assertEqual(file_kind("actio.json"), "actio")
        self.assertEqual(file_kind("diarium.jsonl"), "diarium")
        self.assertEqual(file_kind(f"{RID}.can.json"), "cancellation")
        self.assertEqual(file_kind(f"{RID}.res.json"), "response")
        self.assertEqual(file_kind(f"{RID}.con.json"), "confirmation")
        self.assertEqual(file_kind(f"{RID}.json"), "request")
        self.assertEqual(file_kind("diarium.json"), "request")  # the diarium's name is .jsonl
        self.assertEqual(file_kind("anything.else"), None)

    def test_what_status_and_broadcast_write_reads_back_as_announce(self) -> None:
        # the round trip: the announce files `<id>.n.json` and the broadcast's `nuntium/nuntium.json`
        # read back as `announce`
        self.assertEqual(file_kind("nuntium/alpha.1.n.json"), "announce")
        self.assertEqual(file_kind("alpha.1.n.json"), "announce")
        self.assertEqual(file_kind("nuntium/nuntium.json"), "announce")
        self.assertEqual(file_kind("nuntium.json"), "announce")


class EnvelopeKindTest(unittest.TestCase):
    def test_the_body_key_is_the_only_input(self) -> None:
        self.assertEqual(envelope_kind({**header(), "request": body()}), "request")
        self.assertEqual(envelope_kind({"cancellation": {}}), "cancellation")
        self.assertEqual(envelope_kind({"response": {}}), "response")
        self.assertEqual(envelope_kind({"confirmation": {}}), "confirmation")
        self.assertIsNone(envelope_kind(header()))  # no body key
        self.assertIsNone(envelope_kind({**header(), "request": body(), "response": {}}))
        self.assertIsNone(envelope_kind(None))
        self.assertIsNone(envelope_kind([{"request": {}}]))
        self.assertIsNone(envelope_kind("request"))

    def test_the_two_rules_do_not_meet_and_the_receiver_follows_the_body_key(self) -> None:
        # a file named `<rid>.can.json` whose body key is `response`: a response to `envelope_kind`,
        # a cancellation to `file_kind`. The receiver classifies by the body key, and follows the first.
        doc = {**header(), "response": body()}
        self.assertEqual(envelope_kind(doc), "response")
        self.assertEqual(file_kind(f"{RID}.can.json"), "cancellation")


class HeaderAndNameTest(unittest.TestCase):
    def test_same_header_ignores_only_datetime(self) -> None:
        a = header()
        self.assertTrue(same_header(a, a))
        self.assertTrue(same_header(a, header(datetime="2026-10-24T10:00:01Z")))
        for key in ("rid", "src", "dst", "type", "actio_version"):
            self.assertFalse(same_header(a, header(**{key: "changed"})), key)

    def test_name_matches_the_request_the_cancellation_cancels(self) -> None:
        req = cast(RequestBody, body())
        can = cast(CancellationBody, {"name": "echo"})
        self.assertTrue(name_matches(req, can))
        self.assertFalse(name_matches(req, cast(CancellationBody, {"name": "sleep"})))

    def test_unique_capability_names_finds_the_first_duplicate(self) -> None:
        self.assertIsNone(unique_capability_names(spec_with([])))
        self.assertIsNone(unique_capability_names(spec_with(["echo", "sleep", "fail"])))
        self.assertEqual(
            unique_capability_names(spec_with(["echo", "sleep", "echo", "fail"])), "echo"
        )


class CheckParamsTest(unittest.TestCase):
    def test_a_real_capability_schema(self) -> None:
        cap = cap_with(
            {
                "type": "object",
                "properties": {"text": {"type": "string"}},
                "required": ["text"],
                "additionalProperties": False,
            }
        )
        self.assertIsNone(check_params(cap, {"text": "hi"}))
        self.assertEqual(check_params(cap, {"text": 42}), "/text must be of type string")
        self.assertEqual(check_params(cap, {}), "(root) missing 'text'")
        self.assertEqual(
            check_params(cap, {"text": "hi", "extra": 1}),
            "(root) has a property that is not allowed",
        )

    def test_boolean_parameter_schemas(self) -> None:
        self.assertIsNone(check_params(cap_with(True), {"anything": "goes"}))
        self.assertEqual(check_params(cap_with(False), {}), "(root) fails None")

    def test_a_schema_that_will_not_run_here_skips_the_check(self) -> None:
        # D-19: an uncompilable parameters schema skips the check and the peer decides
        self.assertIsNone(check_params(cap_with({"$ref": "https://example.com/params.json"}), {}))
        self.assertIsNone(check_params(cap_with({"type": "nope"}), {}))
        self.assertIsNone(check_params(cap_with({"required": "text"}), {}))


class CapabilityCompilationTest(unittest.TestCase):
    def test_an_unknown_keyword_in_an_own_capability_is_a_registration_error(self) -> None:
        with self.assertRaises(ValueError):
            compile_capability(cap_with({"type": "object", "properites": {}}))

    def test_an_invalid_schema_in_an_own_capability_is_a_registration_error(self) -> None:
        with self.assertRaises(jsonschema.exceptions.SchemaError):
            compile_capability(cap_with({"required": "text"}))

    def test_a_peers_unknown_keyword_is_skipped_and_logged_once(self) -> None:
        cap = cap_with({"type": "object", "properites": {}}, name="lonely")
        with self.assertLogs("actio.validate", level="WARNING") as logs:
            v = _compile_peer(cap)
        self.assertEqual(len(logs.records), 1)
        self.assertIn("lonely", logs.records[0].getMessage())
        self.assertEqual(v.skipped, frozenset({"parameters"}))
        self.assertIsNone(v.parameters({"anything": "goes"}))
        self.assertIsNone(v.result("a result"))  # the `True` result accepts everything

        # a second compile of the same capability does not log again
        captured: list[str] = []

        class Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured.append(record.getMessage())

        logger = logging.getLogger("actio.validate")
        handler = Capture()
        logger.addHandler(handler)
        try:
            _compile_peer(cap)
            _compile_peer(cap)
        finally:
            logger.removeHandler(handler)
        self.assertEqual(captured, [])

    def test_a_repeated_id_across_two_capabilities_is_harmless(self) -> None:
        a = cap_with({"$id": "shared.json", "type": "object"}, name="first")
        b = cap_with({"$id": "shared.json", "type": "object"}, name="second")
        va, vb = compile_capability(a), compile_capability(b)
        self.assertIsNone(va.parameters({}))
        self.assertIsNone(vb.parameters({}))
        self.assertEqual(vb.parameters("nope"), "(root) must be of type object")

    def test_a_peers_ref_cannot_reach_out(self) -> None:
        cap = cap_with({"$ref": "https://example.com/params.json"}, name="distant")
        v = _compile_peer(cap)
        self.assertEqual(v.skipped, frozenset())  # it compiles; the ref only fails at call time
        self.assertIsNone(v.parameters({"anything": "goes"}))
        self.assertIsNone(check_params(cap, {"anything": "goes"}))


class ReasonShapeTest(unittest.TestCase):
    """D-15: one line, at most LIMIT characters, no trailing period, and the value stays out."""

    def _assert_shape(self, reason: str | None) -> str:
        assert reason is not None
        self.assertLessEqual(len(reason), LIMIT)
        self.assertNotIn("\n", reason)
        self.assertFalse(reason.endswith("."))
        return reason

    def test_a_hundred_kilobyte_value_stays_out(self) -> None:
        secret = "SECRET-TOKEN\n" + "x" * 100_000
        doc = {
            "id": "alpha",
            "status": "idle",
            "datetime": "2026-10-24T10:00:00Z",
            "capability_status": [],
            "load": secret,
        }
        reason = self._assert_shape(validate("status", doc))
        self.assertEqual(reason, "/load must be of type number")
        self.assertNotIn("SECRET-TOKEN", reason)

    def test_a_multiline_key_stays_out(self) -> None:
        key = "a\nb" + "k" * 5_000
        cap = cap_with({"type": "object", "additionalProperties": {"type": "integer"}})
        doc_reason = self._assert_shape(check_params(cap, {key: "x"}))
        self.assertNotIn(key, doc_reason)
        # and so does the reason of the error whose path is the peer's key itself:
        # the segment is escaped with json.dumps, which keeps it on one line
        validator = Draft202012Validator(
            {"type": "object", "additionalProperties": {"type": "integer"}},
            registry=referencing.Registry(),
        )
        keyed = [
            e for e in validator.iter_errors({key: "x"}) if key in [str(p) for p in e.absolute_path]
        ]
        self.assertTrue(keyed)
        for e in keyed:
            r = reason(e)
            self.assertLessEqual(len(r), LIMIT)
            self.assertNotIn("\n", r)
            self.assertNotIn(key, r)
        self.assertTrue(r.startswith("/a\\nb"))  # the key is escaped, not raw

    def test_a_path_twenty_deep_stays_short(self) -> None:
        schema_doc: Any = {"type": "integer"}
        value: Any = "leak-me"
        for i in range(20):
            schema_doc = {
                "type": "object",
                "properties": {f"lvl{i}": schema_doc},
                "required": [f"lvl{i}"],
            }
            value = {f"lvl{i}": value}
        reason = self._assert_shape(check_params(cap_with(schema_doc), value))
        self.assertNotIn("leak-me", reason)


class MalformedReasonTest(unittest.TestCase):
    def test_the_prefix_and_no_value_in_the_parser_error(self) -> None:
        hostile = [
            '"abc',
            '{"a": 1',
            "[1, 2,",
            "SECRET" * 25_000,  # a 100 KB value that is not JSON
        ]
        for text in hostile:
            with self.assertRaises(ValueError) as ctx:
                json.loads(text)
            reason = malformed_reason(ctx.exception)
            self.assertTrue(reason.startswith("malformed JSON: "), reason)
            self.assertNotIn("\n", reason)
            self.assertNotIn("SECRET", reason)


class CommonConstraintThroughRefTest(unittest.TestCase):
    def test_a_non_crockford_rid_fails_the_request(self) -> None:
        # the constraint lives in common.schema.json and reaches the envelope through its $ref
        doc = {**header(rid=BAD_RID), "request": body()}
        self.assertEqual(validate("request", doc), "/rid fails 'pattern'")

    def test_a_crockford_rid_passes(self) -> None:
        self.assertIsNone(validate("request", {**header(), "request": body()}))

    def test_the_ten_kinds_load_and_cache(self) -> None:
        kinds: tuple[SchemaKind, ...] = (
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
        for kind in kinds:
            self.assertEqual(schema(kind)["$id"], f"{kind}.schema.json")
        self.assertIs(schema("spec"), schema("spec"))


if __name__ == "__main__":
    unittest.main()
