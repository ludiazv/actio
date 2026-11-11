"""`python -m actio` runs the same CLI as the `actio` console script (plan §3, §6). WP8 fills `_cli`.

The console script in `pyproject.toml` exits with what `main` returns, and so does this: `SystemExit`
carries the CLI's exit code, which is one of the codes `actio-ts/src/cli.ts` uses (§6).
"""

from ._cli import main

if __name__ == "__main__":
    raise SystemExit(main())
