"""The public surface was frozen at M0: this test pins the names, the shapes and the signatures.

A work package that changes one of these names, one keyword-only marker or one dataclass field has to
stop and ask, and this test says so first.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import typing
import unittest
from typing import Any

import actio
from actio._types import AgentContext, AgentFolders

# The table of *Public API*, plus the protocol types the package exports.
REQUIRED = (
    # the agent and its API
    "ActioAgent",
    # functions
    "check_params",
    "load_config",
    "scan",
    "health",
    "read_diarium",
    # exceptions
    "ActioError",
    "ActioCallError",
    "ActioResultError",
    # results and views
    "AgentView",
    "ScanResult",
    "DiscoveredAgent",
    "IgnoredFile",
    "IgnoredLine",
    "DiariumRead",
    "RequestOutcome",
    "ActioConfig",
    "Health",
    "CapabilityContext",
    "CommandContext",
    "ActioEvent",
    "StatusEvent",
    "AnnounceEvent",
    "InreqEvent",
    "OutreqEvent",
    "LogEvent",
    "IgnoredEvent",
    # the protocol shapes of _protocol.py
    "ActioMeta",
    "Announce",
    "CancellationBody",
    "CancellationFile",
    "Capability",
    "CapabilityActio",
    "CapabilityStatusEntry",
    "ConfirmationBody",
    "ConfirmationFile",
    "DiariumEntry",
    "ErrorResult",
    "ExchangeRecord",
    "Header",
    "RequestFile",
    "RequestBody",
    "ResponseFile",
    "ResponseBody",
    "SecurityAttribute",
    "Spec",
    "Status",
    "StatusTask",
)

# The method names that table gives, on the class.
AGENT_METHODS = (
    "capability",
    "on_command",
    "on_event",
    "start",
    "stop",
    "wait_stopped",
    "agmen",
    "refresh",
    "request",
    "command",
    "fail",
    "diarium",
    "log",
)


def parameters_of(func: Any) -> list[str]:
    return [name for name in inspect.signature(func).parameters if name != "self"]


def keyword_only(func: Any) -> set[str]:
    return {
        name for name, p in inspect.signature(func).parameters.items() if p.kind is p.KEYWORD_ONLY
    }


def positional(func: Any) -> list[str]:
    return [
        name
        for name, p in inspect.signature(func).parameters.items()
        if p.kind is p.POSITIONAL_OR_KEYWORD and name != "self"
    ]


class SurfaceTest(unittest.TestCase):
    def test_all_is_unique_and_complete(self) -> None:
        """No duplicates. The order is by role, so the tuple reads like the table, not like a dictionary."""
        self.assertEqual(len(actio.__all__), len(set(actio.__all__)))

    def test_everything_advertised_exists(self) -> None:
        for name in actio.__all__:
            self.assertTrue(hasattr(actio, name), name)

    def test_nothing_public_is_unadvertised(self) -> None:
        """A name that reaches `actio.` without being in `__all__` is a public accident."""
        extra = {name for name in dir(actio) if not name.startswith("_")} - set(actio.__all__)
        self.assertEqual(extra, set())

    def test_the_frozen_names_are_present(self) -> None:
        for name in REQUIRED:
            if not hasattr(actio, name):
                self.fail(f"*Public API* names {name!r}, and it is not in the package")
        for name in AGENT_METHODS:
            if not callable(getattr(actio.ActioAgent, name, None)):
                self.fail(f"*Public API* names agent.{name}(), and it is not on ActioAgent")


class AgentSurfaceTest(unittest.TestCase):
    def test_the_constructor_is_keyword_only_and_frozen(self) -> None:
        self.assertEqual(
            parameters_of(actio.ActioAgent.__init__),
            [
                "id",
                "description",
                "accepts_commands_from",
                "namespaces",
                "depends",
                "wants",
                "info",
                "config",
            ],
        )
        self.assertEqual(
            keyword_only(actio.ActioAgent.__init__),
            {
                "id",
                "description",
                "accepts_commands_from",
                "namespaces",
                "depends",
                "wants",
                "info",
                "config",
            },
        )

    def test_the_method_names_of_the_table(self) -> None:
        for name in AGENT_METHODS:
            self.assertTrue(callable(getattr(actio.ActioAgent, name)), name)
        for name in (
            "start",
            "stop",
            "wait_stopped",
            "request",
            "command",
            "refresh",
            "fail",
            "diarium",
            "log",
            "__aenter__",
            "__aexit__",
        ):
            self.assertTrue(inspect.iscoroutinefunction(getattr(actio.ActioAgent, name)), name)

    def test_request_and_command_signatures(self) -> None:
        self.assertEqual(positional(actio.ActioAgent.request), ["dst", "name", "params"])
        self.assertEqual(
            keyword_only(actio.ActioAgent.request), {"timeout", "attachments", "cancel"}
        )
        self.assertEqual(parameters_of(actio.ActioAgent.command), ["dst", "name", "params"])
        self.assertEqual(keyword_only(actio.ActioAgent.stop), {"force"})
        self.assertEqual(parameters_of(actio.ActioAgent.capability), ["capability", "handler"])
        self.assertEqual(parameters_of(actio.ActioAgent.on_command), ["name", "handler"])
        self.assertEqual(parameters_of(actio.ActioAgent.on_event), ["handler"])
        self.assertEqual(parameters_of(actio.ActioAgent.fail), ["message"])

    def test_the_agent_subclasses_nothing_and_exports_no_seam(self) -> None:
        """The seam the engines use is a private object, not a base class.

        A class that inherits a Protocol is abstract to a consumer's `mypy` — it inherits the Protocol's
        members as abstract — and would put `emit`, `sync`, `folders`, `config`, `id`, `now`, `spec`,
        `state` and `append_diarium` on the public surface, none of which the table names.
        """
        self.assertEqual(actio.ActioAgent.__bases__, (object,))
        seam = set(AgentContext.__annotations__) | {
            name for name in vars(AgentContext) if not name.startswith("_")
        }
        self.assertTrue(seam, "the seam has to have members for this to mean anything")
        for name in sorted(seam):
            self.assertFalse(hasattr(actio.ActioAgent, name), f"agent.{name} is not public API")

    def test_a_consumer_can_build_an_agent_and_use_both_registration_forms(self) -> None:
        """A consumer's own call sites, type-checked by `mypy` from here.

        `tests` is in mypy's files, so if a frozen signature could not be satisfied from outside — the
        way an abstract base class defeats it — this fails the type check rather than a review. Registering
        is a dict write: no folders, no config, no `start()`. The stub-era version of this test asked for
        `NotImplementedError`, which pinned the *stub* rather than the API and failed by construction the
        moment the module was implemented.
        """
        agent = actio.ActioAgent(id="echo", description="d", accepts_commands_from=["*"])
        cap: actio.Capability = {
            "name": "echo",
            "description": "Echo the text back",
            "parameters": {"type": "object", "properties": {"text": {"type": "string"}}},
            "actio": {"result": {"type": "object"}},
        }

        async def echo(params: Any, ctx: actio.CapabilityContext) -> Any:
            return params

        agent.capability(cap, echo)  # the call form

        upper: actio.Capability = {
            "name": "upper",
            "description": "Upper the text",
            "parameters": {"type": "object", "properties": {"text": {"type": "string"}}},
            "actio": {"result": {"type": "object"}},
        }

        @agent.capability(upper)  # the decorator form, as *Public API* shows it
        async def decorated(params: Any, ctx: actio.CapabilityContext) -> Any:
            return params

        async def plain(params: Any, ctx: actio.CapabilityContext) -> Any:
            return params

        # A decorator that wrapped the handler would replace what the caller wrote: `@` above and the
        # explicit application below must both leave the caller's own coroutine in hand.
        self.assertTrue(inspect.iscoroutinefunction(decorated))
        self.assertIs(agent.capability({**upper, "name": "lower"})(plain), plain)


class FunctionSurfaceTest(unittest.TestCase):
    def test_the_functions_of_the_table(self) -> None:
        self.assertTrue(inspect.iscoroutinefunction(actio.scan))
        self.assertTrue(inspect.iscoroutinefunction(actio.read_diarium))
        self.assertEqual(positional(actio.health), ["status", "now"])
        self.assertEqual(positional(actio.check_params), ["cap", "params"])

    def test_choosing_a_sync_provider_is_not_public(self) -> None:
        """A caller picks a provider with `ActioConfig.sync`, a string, as `actio-ts` does. Creating one is
        internal, so neither the factory nor its Protocol reaches `actio.`."""
        for name in ("create_sync_provider", "SyncProvider"):
            self.assertFalse(hasattr(actio, name), name)
            self.assertNotIn(name, actio.__all__)

    def test_load_config_takes_the_env_and_every_field(self) -> None:
        names = set(inspect.signature(actio.load_config).parameters)
        self.assertEqual(
            names,
            {
                "env",
                "root",
                "sync",
                "heartbeat_interval",
                "confirm_timeout",
                "cancel_wait_timeout",
                "announce_delay",
                "nuntium_debounce",
                "stop_grace",
                "clock",
            },
        )
        self.assertEqual(keyword_only(actio.load_config), names)


class ShapeTest(unittest.TestCase):
    def test_health_is_the_four_states_or_none(self) -> None:
        self.assertEqual(
            set(typing.get_args(typing.get_args(actio.Health)[0])),
            {"healthy", "online", "degraded", "zombie"},
        )
        self.assertIn(type(None), typing.get_args(actio.Health))

    def test_request_outcome(self) -> None:
        fields = [f.name for f in dataclasses.fields(actio.RequestOutcome)]
        self.assertEqual(fields, ["final_status", "response"])
        self.assertEqual(
            set(typing.get_args(typing.get_type_hints(actio.RequestOutcome)["final_status"])),
            {"confirmed", "cancelled", "abandoned"},
        )

    def test_the_dataclasses_of_the_table(self) -> None:
        expected = {
            actio.AgentView: [
                "id",
                "folder",
                "spec",
                "status",
                "health",
                "commands",
                "capabilities",
            ],
            actio.ScanResult: ["agents", "duplicates", "ignored"],
            actio.DiscoveredAgent: ["folder", "spec", "status"],
            actio.IgnoredFile: ["path", "reason"],
            actio.DiariumRead: ["lines", "ignored"],
            actio.IgnoredLine: ["line", "reason"],
            AgentFolders: ["root", "agent", "inbox", "forum", "nuntium"],
        }
        for cls, fields in expected.items():
            self.assertEqual([f.name for f in dataclasses.fields(cls)], fields, cls.__name__)
            self.assertTrue(dataclasses.is_dataclass(cls), cls.__name__)

    def test_events_are_frozen_dataclasses_with_slots(self) -> None:
        expected = {
            actio.StatusEvent: ["state"],
            actio.AnnounceEvent: ["text"],
            actio.InreqEvent: ["rid", "final_status"],
            actio.OutreqEvent: ["rid", "final_status"],
            actio.LogEvent: ["message"],
            actio.IgnoredEvent: ["path", "reason"],
        }
        for cls, fields in expected.items():
            self.assertEqual([f.name for f in dataclasses.fields(cls)], fields, cls.__name__)
            self.assertIn("__slots__", cls.__dict__, cls.__name__)
            factory: Any = cls  # the dict is heterogeneous; mypy cannot call each class itself
            made = factory(*[None] * len(fields))
            with self.assertRaises(dataclasses.FrozenInstanceError):
                setattr(made, fields[0], "changed")
        self.assertEqual(
            set(typing.get_args(actio.ActioEvent)),
            set(expected),
        )

    def test_the_contexts_are_protocols_with_the_named_members(self) -> None:
        """The members a handler may rely on; a Protocol states them as annotations or methods."""
        for cls, members in (
            (
                actio.CapabilityContext,
                ["rid", "src", "cancelled", "attachments", "publish_attachment"],
            ),
            (actio.CommandContext, ["rid", "src"]),
        ):
            hints = typing.get_type_hints(cls)
            for name in members:
                if name not in hints and not callable(getattr(cls, name, None)):
                    self.fail(f"{cls.__name__} does not state {name}")

    def test_a_handler_context_satisfies_the_protocol(self) -> None:
        class Ctx:
            def __init__(self) -> None:
                self.rid = "01HZY000000000000000000000"
                self.src = "peer"
                self.cancelled = asyncio.Event()
                self.attachments: list[str] = []

            async def publish_attachment(self, name: str, data: bytes) -> str:
                return name

        self.assertIsInstance(Ctx(), actio.CapabilityContext)


class ExceptionTest(unittest.TestCase):
    def test_actio_error_enforces_the_protocol_codes(self) -> None:
        for code in (0, 3, 999):
            with self.assertRaises(ValueError):
                actio.ActioError(code, "no")
        error = actio.ActioError(1000, "refused", {"why": "policy"})
        self.assertEqual(error.code, 1000)
        self.assertEqual(error.details, {"why": "policy"})
        self.assertEqual(str(error), "refused")

    def test_call_error_carries_its_three_parts(self) -> None:
        error = actio.ActioCallError("beta", "echo", "no shared namespace")
        self.assertEqual(
            (error.dst, error.name, error.reason), ("beta", "echo", "no shared namespace")
        )
        self.assertEqual(str(error), "cannot call beta/echo: no shared namespace")

    def test_result_error_carries_the_response(self) -> None:
        response = {"result": {"nope": 1}}
        error = actio.ActioResultError("the result failed the schema", response)  # type: ignore[arg-type]
        self.assertIs(error.response, response)


if __name__ == "__main__":
    unittest.main()
