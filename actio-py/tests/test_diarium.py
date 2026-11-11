"""`_diarium.py`: the five protocol events, the validated append, and the read-only view.

Every builder's output validates against `diarium.schema.json` — the schema, not a hand-written dict,
is the fixture. `append` refuses an invalid line before it touches the file; `read_diarium` gives the
valid lines in file order and the ignored ones with their 1-based number and a one-line reason.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from actio._diarium import (
    announce_event,
    append,
    inreq_event,
    log_event,
    outreq_event,
    read_diarium,
    status_event,
)
from actio._protocol import DiariumEntry, ExchangeRecord, InreqFinalStatus, OutreqFinalStatus
from actio._sync import create_sync_provider
from actio._validate import validate
from helpers import ActioTestCase, actio_tree

AGENT = "summarizer"
TS = 1_792_836_000.0  # 2026-10-24T10:00:00Z


def exchange_from_vector() -> ExchangeRecord:
    """The four envelopes of one real exchange, as the conformance vectors store them."""
    path = Path(__file__).parent / "vectors" / "diarium" / "valid" / "07-a1-inreq-confirmed.json"
    line = json.loads(path.read_text(encoding="utf-8"))
    return cast(ExchangeRecord, line["event_data"])


class BuildersTest(ActioTestCase):
    def test_each_builder_validates_against_the_schema(self) -> None:
        exchange = exchange_from_vector()
        for entry in (
            status_event(AGENT, TS, "idle"),
            announce_event(AGENT, TS),
            log_event(AGENT, TS, "an ignored file in the inbox"),
            inreq_event(AGENT, TS, "confirmed", exchange),
            outreq_event(AGENT, TS, "confirmed", exchange),
        ):
            self.assertIsNone(validate("diarium", entry), entry)

    def test_the_event_data_shapes(self) -> None:
        exchange = exchange_from_vector()
        self.assertEqual(status_event(AGENT, TS, "busy")["event_data"], "busy")
        self.assertIsNone(announce_event(AGENT, TS)["event_data"])
        self.assertEqual(log_event(AGENT, TS, "hi")["event_data"], "hi")
        inreq = inreq_event(AGENT, TS, "dropped", exchange)
        self.assertEqual(inreq["event"], "inreq")
        data = cast(ExchangeRecord, inreq["event_data"])
        # The parameter's final status wins over whatever the record carried.
        self.assertEqual(data["final_status"], "dropped")
        for key in ("req", "can", "res", "con"):
            self.assertIn(key, data)
        outreq = outreq_event(AGENT, TS, "sent", exchange)
        self.assertEqual(outreq["event"], "outreq")
        self.assertEqual(cast(ExchangeRecord, outreq["event_data"])["final_status"], "sent")

    def test_envelopes_can_be_null(self) -> None:
        empty: ExchangeRecord = {
            "final_status": "confirmed",
            "req": None,
            "can": None,
            "res": None,
            "con": None,
        }
        self.assertIsNone(validate("diarium", inreq_event(AGENT, TS, "completed", empty)))
        self.assertIsNone(validate("diarium", outreq_event(AGENT, TS, "abandoned", empty)))

    def test_inreq_and_outreq_use_their_own_final_status_sets(self) -> None:
        exchange = exchange_from_vector()
        # `sent` is an outreq final status only; the schema's `if`/`then` per event rejects it here.
        self.assertIsNotNone(
            validate("diarium", inreq_event(AGENT, TS, cast(InreqFinalStatus, "sent"), exchange))
        )
        # `dropped` is an inreq final status only.
        self.assertIsNotNone(
            validate(
                "diarium", outreq_event(AGENT, TS, cast(OutreqFinalStatus, "dropped"), exchange)
            )
        )

    def test_the_line_carries_the_agent_id_and_a_whole_second_datetime(self) -> None:
        entry = log_event(AGENT, TS + 0.9, "x")
        self.assertEqual(entry["id"], AGENT)
        self.assertEqual(entry["datetime"], "2026-10-24T10:00:00Z")


class AppendTest(ActioTestCase):
    async def test_append_writes_one_line_per_event(self) -> None:
        with actio_tree() as root:
            agent = root / AGENT
            agent.mkdir()  # the agent folder exists: it is created at registration
            folder = str(agent)
            provider = create_sync_provider("none")
            first = log_event(AGENT, TS, "one")
            second = status_event(AGENT, TS + 1, "idle")
            await append(folder, first, provider)
            await append(folder, second, provider)
            raw = (Path(folder) / "diarium.jsonl").read_text(encoding="utf-8")
            lines = [json.loads(line) for line in raw.splitlines()]
            self.assertEqual(lines, [first, second])

    async def test_append_refuses_an_invalid_entry_before_writing(self) -> None:
        with actio_tree() as root:
            folder = str(root / AGENT)
            provider = create_sync_provider("none")
            # Not one of the four states: the schema's `if`/`then` for `status` rejects it.
            bad: DiariumEntry = {
                "id": AGENT,
                "datetime": "2026-10-24T10:00:00Z",
                "event": "status",
                "event_data": "asleep",
            }
            with self.assertRaises(ValueError):
                await append(folder, bad, provider)
            self.assertFalse((Path(folder) / "diarium.jsonl").exists())


class ReadDiariumTest(ActioTestCase):
    async def test_a_missing_file_reads_as_empty(self) -> None:
        with actio_tree() as root:
            result = await read_diarium(str(root / AGENT))
            self.assertEqual(result.lines, [])
            self.assertEqual(result.ignored, [])

    async def test_an_empty_file_reads_as_empty(self) -> None:
        with actio_tree() as root:
            folder = root / AGENT
            folder.mkdir()
            (folder / "diarium.jsonl").write_text("", encoding="utf-8")
            result = await read_diarium(str(folder))
            self.assertEqual(result.lines, [])
            self.assertEqual(result.ignored, [])

    async def test_the_valid_lines_come_back_in_file_order(self) -> None:
        with actio_tree() as root:
            agent = root / AGENT
            agent.mkdir()  # the agent folder exists: it is created at registration
            folder = str(agent)
            provider = create_sync_provider("none")
            entries = [
                status_event(AGENT, TS, "inactive"),
                announce_event(AGENT, TS + 1),
                log_event(AGENT, TS + 2, "three"),
            ]
            for entry in entries:
                await append(folder, entry, provider)
            result = await read_diarium(folder)
            self.assertEqual(result.lines, entries)
            self.assertEqual(result.ignored, [])

    async def test_bad_lines_are_ignored_with_their_number_and_reason(self) -> None:
        with actio_tree() as root:
            folder = root / AGENT
            folder.mkdir()
            good = log_event(AGENT, TS, "good")
            invalid = {
                "id": AGENT,
                "datetime": "2026-10-24T10:00:00Z",
                "event": "log",
            }  # no event_data
            (folder / "diarium.jsonl").write_text(
                f"{json.dumps(good)}\nnot json at all\n{json.dumps(invalid)}\n", encoding="utf-8"
            )
            result = await read_diarium(str(folder))
            self.assertEqual(result.lines, [good])
            self.assertEqual(len(result.ignored), 2)
            self.assertEqual(result.ignored[0].line, 2)
            self.assertTrue(result.ignored[0].reason)
            self.assertEqual(result.ignored[1].line, 3)
            self.assertTrue(result.ignored[1].reason)

    async def test_a_truncated_last_line_is_one_ignored_line_and_the_rest_survive(self) -> None:
        with actio_tree() as root:
            folder = root / AGENT
            folder.mkdir()
            first = log_event(AGENT, TS, "first")
            second = log_event(AGENT, TS + 1, "second")
            (folder / "diarium.jsonl").write_text(
                f"{json.dumps(first)}\n{json.dumps(second)}\n{json.dumps(second)}",
                encoding="utf-8",
            )  # the third line has no trailing newline: an append in progress
            result = await read_diarium(str(folder))
            self.assertEqual(result.lines, [first, second])
            self.assertEqual(len(result.ignored), 1)
            self.assertEqual(result.ignored[0].line, 3)

    async def test_blank_lines_are_skipped_and_line_numbers_are_file_lines(self) -> None:
        with actio_tree() as root:
            folder = root / AGENT
            folder.mkdir()
            good = log_event(AGENT, TS, "good")
            (folder / "diarium.jsonl").write_text(f"\n{json.dumps(good)}\n", encoding="utf-8")
            result = await read_diarium(str(folder))
            self.assertEqual(result.lines, [good])
            self.assertEqual(result.ignored, [])

    async def test_custom_event_names_are_not_checked(self) -> None:
        with actio_tree() as root:
            agent = root / AGENT
            agent.mkdir()  # the agent folder exists: it is created at registration
            folder = str(agent)
            provider = create_sync_provider("none")
            custom: DiariumEntry = {
                "id": AGENT,
                "datetime": "2026-10-24T10:00:00Z",
                "event": "my-own-event",
                "event_data": {"anything": "goes"},
            }
            # The schema lets implementations define their own events; `append` writes it as is.
            await append(folder, custom, provider)
            result = await read_diarium(folder)
            self.assertEqual(result.lines, [custom])
            self.assertEqual(result.ignored, [])
