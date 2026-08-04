"""Load process environment from a local ``.env`` file."""

from __future__ import annotations

from pathlib import Path

from dotenv import find_dotenv, load_dotenv


def load_environment_file(*, override: bool = False) -> Path | None:
    """Load the nearest ``.env`` (cwd and parents) into ``os.environ``.

    Existing process environment variables win unless ``override=True``.
    Returns the loaded file path, or ``None`` when no ``.env`` is found.
    """
    dotenv_path = find_dotenv(usecwd=True)
    if not dotenv_path:
        return None
    load_dotenv(dotenv_path, override=override)
    return Path(dotenv_path).expanduser().resolve()
