"""The `actio` command: the same subcommands, flags, output and exit codes as the reference CLI (§6,
`../actio-ts/src/cli.ts`). WP8.

`ls`, `diarium <agent> [-n N]`, `call <src-id> <dst> <capability> <json-params> [--timeout s]` (default
60, never 0), `cmd <src-id> <dst> <name> [json-params]`, `broadcast "<text>"`, `validate <file>
[--as kind]`, and a global `--json`. Standard `argparse`, one file, at most 320 lines.

Two Python traps: `argparse` exits with code 2 on a usage error, which must be overridden to match the
reference's codes, and `validate` decides the kind through `_validate.file_kind` — never a second copy
of the name rule, which is how `actio-go` came to reject every real announce file (§6, v1.8).
"""

from __future__ import annotations

from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Run one CLI command and return its exit code; `python -m actio` and the console script share it."""
    raise NotImplementedError("WP8")
