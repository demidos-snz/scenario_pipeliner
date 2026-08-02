# Releasing

This document defines version bump policy and release steps for `scenario_pipeliner`.

## Versioning policy

Project uses SemVer with current stage `<1.0.0` (alpha):

- `0.Y.Z`:
  - `Y` bump for incompatible API/behavior changes.
  - `Z` bump for backward-compatible fixes and improvements.
- Do not publish breaking changes under the same `0.Y.*` line.
- `main` always tracks the next unreleased version in `CHANGELOG.md`.

## Release checklist

1. Update `CHANGELOG.md`
   - Move relevant entries from `Unreleased` into a new version section.
   - Set release date.
2. Bump version in `pyproject.toml` (`project.version`).
3. Run full local quality gates from `src/scenario_pipeliner`:
   - `uv run ruff check .`
   - `uv run mypy .`
   - `uv run pytest`
4. Build and validate artifacts:
   - `uv build`
   - `uv run twine check dist/*`
5. Validate migrate-core smoke:
   - SQLite smoke via tests.
   - PostgreSQL smoke (`SCENARIO_PIPELINER_RUN_POSTGRES_TESTS=1 ...`).
6. Trigger CI `release_dry_run` workflow and ensure all jobs pass.
7. Publish to TestPyPI (manual `Publish` workflow, target=`testpypi`).
8. Verify installation from TestPyPI in a clean venv.
9. Publish to production PyPI (`Publish` workflow target=`pypi`, or push tag `v*`).
10. Tag release in git (e.g., `v0.1.1`) and publish GitHub release notes.

## TestPyPI install smoke

```bash
python -m venv .venv-testpypi
source .venv-testpypi/bin/activate
pip install --upgrade pip
pip install -i https://test.pypi.org/simple scenario-pipeliner
python -m scenario_pipeliner.cli --help
```

## Trusted publishing setup (required once)

Configure trusted publishers in both PyPI and TestPyPI for this repository/workflows:

- Owner: `demidos-snz`
- Repository: `scenario_pipeliner`
- Workflow: `.github/workflows/publish.yml`
- Environment:
  - `testpypi` for TestPyPI
  - `pypi` for production PyPI

After this binding is configured, no API tokens are required in GitHub secrets for publish jobs.
