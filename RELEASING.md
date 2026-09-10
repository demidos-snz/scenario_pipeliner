# Releasing

This document defines version bump policy and release steps for `scenario_pipeliner`.

## Versioning policy

Project uses SemVer with current stage `<1.0.0` (alpha):

- `0.Y.Z`:
  - `Y` bump for incompatible API/behavior changes.
  - `Z` bump for backward-compatible fixes and improvements.
- Do not publish breaking changes under the same `0.Y.*` line.
- Package version lives in `scenario_pipeliner/version.py` (`__version__`).
  `core_version` defaults and `plugin init` `core_compat` follow it.

## Release checklist

1. Bump `__version__` in `scenario_pipeliner/version.py`.
2. Move notes from `[Unreleased]` in `CHANGELOG.md` into the new version section.
3. Run full local quality gates:
   - `uv run ruff check .`
   - `uv run ruff format --check .`
   - `uv run mypy .`
   - `uv run pytest`
4. Build and validate artifacts:
   - `uv build`
   - `uv run twine check dist/*`
5. Ensure GitHub Actions CI is green on the release commit.
6. Publish to PyPI:
   - push tag `v<version>` (e.g. `v0.0.1`), or
   - run the `Publish` workflow (`workflow_dispatch`).
7. Confirm the project page: https://pypi.org/project/scenario-pipeliner/
8. Optional: GitHub release notes for the tag.

## PyPI install smoke

```bash
python -m venv .venv-pypi
source .venv-pypi/bin/activate
pip install -U pip
pip install scenario-pipeliner
python -c "import scenario_pipeliner; print(scenario_pipeliner.__version__)"
scenario_pipeliner --help
```

## Trusted publishing setup (required once)

Configure a trusted publisher on PyPI for this repository/workflow:

- Owner: `demidos-snz`
- Repository: `scenario_pipeliner`
- Workflow: `.github/workflows/publish.yml`
- Environment: `pypi`

After this binding is configured, no API tokens are required in GitHub secrets.
