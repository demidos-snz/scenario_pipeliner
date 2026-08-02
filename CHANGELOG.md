# Changelog

All notable changes to `scenario_pipeliner` are documented in this file.

The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Core DB migration flow (`db migrate-core`) backed by Alembic.
- SQLite and PostgreSQL smoke coverage for core migration bootstrap.
- Stable top-level public API surface for migrate operations.
- CI release gates: lint/type/test + Postgres smoke + release dry-run.

### Changed
- Packaging metadata hardened for PyPI distribution checks.
- Public API exports narrowed to minimal supported surface.

## [0.1.0] - 2026-08-02

### Added
- Initial public alpha of `scenario_pipeliner`.
- Plugin manifest discovery and validation.
- Dry-run migration planning API/CLI.
- Runtime policy checks (compatibility/checksum) for plugin loading.
- Worker prototype package for plugin-based DB execution experiments.
