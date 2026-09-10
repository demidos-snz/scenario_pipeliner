"""Thin consumer entrypoint (same contract as ``scenario_pipeliner run``)."""

from __future__ import annotations

import asyncio

from scenario_pipeliner.worker.runtime import RunnerApp


def main() -> int:
    try:
        asyncio.run(_run())
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}")
        return 1


async def _run() -> None:
    app = await RunnerApp.from_env()
    await app.run()


if __name__ == "__main__":
    raise SystemExit(main())
