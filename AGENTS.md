# AGENTS.md — django-altimeter

Rules for every agent and person working in this repository. Keep this file
lean; detail belongs in bead contracts, package docs and tests.

## What this repo is

django-altimeter is K2's reusable observability package for Django apps. It
sends traces, logs and metrics through the OpenTelemetry SDK and OTLP/HTTP to
PostHog. V1 supports two deployment shapes: ASGI (Python 3.13, Django 6.0,
uvicorn, Ninja, Celery) and WSGI (Python 3.12, Django 5.2, gunicorn gthread,
DRF, RQ). Requests per second and queue length are first-class outputs.

Telemetry must never change application behavior. When the package is
disabled, misconfigured, overloaded or unable to export, the application keeps
working. V1 supports Django only; add no FastAPI code.

## Work tracking

- Work lives in beads (`br`, `bv`). Epic `alt-v1-4du` is the batch record.
  Each bead carries its full contract, acceptance criteria and test commands.
  Read `br show <bead>` and `br show alt-v1-4du` before you start.
- Use `bv` only with `--robot-*` flags. Bare `bv` opens a TUI that blocks you.
- Planning material is local and git-ignored: `docs/plans/`, `docs/design/`,
  the root requirements document, `.skill-loop-progress.md`, `.beads/`,
  `.cass/`, `.ntm/` and `.bv/`. Never stage, force-add or commit these paths.
  `docs/` itself holds package documentation and is committed.
- This repository is public. Never name client organizations, their products
  or private dependencies in committed files, tests, comments or commit
  messages. Describe the supported deployment shapes (ASGI, WSGI) instead.

## Commands

The package uses uv, Ruff, mypy, pytest and setuptools. These commands work
once `pyproject.toml` exists:

- Install: `uv sync --all-extras --dev`
- Lint and format check: `uv run ruff check .` and `uv run ruff format --check .`
- Typecheck: `uv run mypy src`
- Targeted tests: `uv run pytest <paths>`
- Build wheel and sdist: `uv build`
- Real-process profiles, after `uv build`, with a unique project name per run:
  `docker compose -p <unique-name> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest <paths>`
  then `docker compose -p <unique-name> -f reference/compose.yaml --profile asgi down --remove-orphans`
  (use `--profile wsgi` in both commands and `verify-wsgi` for the Python 3.12 gunicorn/RQ shape)
- Wall-clock performance acceptance, only in the matching local image after `uv build`:
  `docker compose -p <unique> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest -m performance tests/performance/test_web_overhead.py`
  then `docker compose -p <unique> -f reference/compose.yaml --profile asgi down --remove-orphans`
  WSGI: `docker compose -p <unique> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest -m performance tests/performance/test_wsgi_overhead.py`
  then `docker compose -p <unique> -f reference/compose.yaml --profile wsgi down --remove-orphans`
- Gunicorn gthread protocol, lifecycle and combined request-work checks after `uv build`:
  `docker compose -p <unique-wsgi-web-name> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_wsgi.py tests/integration/test_gthread.py tests/integration/test_wsgi_headers.py tests/integration/test_driver_spans.py::TestWSGIDriverFlow tests/integration/test_outbound_context.py::TestWSGIPropagation tests/integration/test_logging_topology.py::TestWSGILogFlow tests/integration/test_wsgi_metrics.py`
  then `docker compose -p <unique-wsgi-web-name> -f reference/compose.yaml --profile wsgi down --remove-orphans`
- Request-work reference checks, after `uv build`, with isolated Compose services:
  `docker compose -p <unique-asgi-work-name> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest tests/integration/test_reference_work.py`
  then `docker compose -p <unique-asgi-work-name> -f reference/compose.yaml --profile asgi down --remove-orphans`
  and standalone Python 3.12 checks:
  `docker compose -p <unique-wsgi-work-name> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_reference_work.py::TestStandaloneReferenceWork tests/integration/test_reference_work.py::test_request_work_extras_install_pinned_clients`
  then `docker compose -p <unique-wsgi-work-name> -f reference/compose.yaml --profile wsgi down --remove-orphans`
- Browser setup (Bun 1.3.14, Node 26.12.1), after `uv sync --all-extras --dev` and `uv build`:
  `cd tests/browser && bun install --frozen-lockfile && bun run install:browser`
- Real Celery publication, worker lifecycle and metric/log checks after `uv build`:
  `docker compose -p <unique-celery> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest tests/integration/test_celery_publication.py tests/integration/test_celery_workers.py tests/integration/test_celery_metrics_logs.py`
  then `docker compose -p <unique-celery> -f reference/compose.yaml --profile asgi down --remove-orphans`
  (repeat with `--profile wsgi` and `verify-wsgi` for Python 3.12 compatibility).
- Predeclared Celery measurements, local ASGI image only after `uv build`:
  `docker compose -p <unique-celery-perf> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest -m performance -s tests/performance/test_worker_overhead.py`
  then `docker compose -p <unique-celery-perf> -f reference/compose.yaml --profile asgi down --remove-orphans`
- Process-resource and optional-source web checks, after `uv build`:
  `docker compose -p <unique-process-asgi> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest tests/integration/test_process_metrics.py tests/integration/test_process_web.py`
  then `docker compose -p <unique-process-asgi> -f reference/compose.yaml --profile asgi down --remove-orphans`
  `docker compose -p <unique-process-wsgi> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_process_metrics.py tests/integration/test_process_web.py`
  then `docker compose -p <unique-process-wsgi> -f reference/compose.yaml --profile wsgi down --remove-orphans`
- Predeclared process-collection overhead, local matching images after `uv build`:
  `docker compose -p <unique-process-perf-asgi> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest -m performance -s tests/performance/test_process_overhead.py`
  then `docker compose -p <unique-process-perf-asgi> -f reference/compose.yaml --profile asgi down --remove-orphans`
  `docker compose -p <unique-process-perf-wsgi> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest -m performance -s tests/performance/test_process_overhead.py`
  then `docker compose -p <unique-process-perf-wsgi> -f reference/compose.yaml --profile wsgi down --remove-orphans`
- Optional collector configuration, after `uv build`; source boundaries and
  parameters are documented in [docs/collector.md](docs/collector.md):
  `docker compose -p <unique-collector-asgi> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest tests/integration/test_collector_profiles.py`
  then `docker compose -p <unique-collector-asgi> -f reference/compose.yaml --profile asgi down --remove-orphans`
  `docker compose -p <unique-collector-wsgi> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_collector_profiles.py`
  then `docker compose -p <unique-collector-wsgi> -f reference/compose.yaml --profile wsgi down --remove-orphans`
  Full Linux host suites require `otelcol-contrib` from the image pinned in
  `reference/Dockerfile` on `PATH`; CI installs it before pytest.
- Browser unit tests: `cd tests/browser && bun run test:unit`
- Browser TypeScript checks (harness plus ASGI/WSGI compiler profiles):
  `cd tests/browser && bun run typecheck`
- Real Chromium, cross-origin axios and installed-wheel uvicorn checks:
  `cd tests/browser && bun run test:e2e resource_timing.spec.ts journey_e2e.spec.ts`
  The browser harness owns temporary origins, ports, processes and its OTLP receiver;
  it tears them down on exit. Build the wheel again after changing shipped source.

Default test runs, including CI, deselect the `performance` marker. Explicit
`-m performance` overrides that default. Only the wall-clock acceptance tests
have this marker; their harness self-tests remain in the default suite. A performance
command that selects zero tests is a failure.

Do not add a second command layer (Makefile, justfile, scripts) unless a bead
asks for it. When a change adds or changes a command, update this list and the
README in the same commit.

In a swarm, run full-suite, full-build, browser e2e and Docker profile runs through the
build slot: `am am-run build --exclusive -- <command>`. Targeted per-file checks
may run directly. The orchestrator runs the integrated gate for each slice.
After every Compose run, whether it succeeds or fails, run the matching project's
`down --remove-orphans` before releasing the build slot. `run --rm` removes only
the test container; Postgres, Redis and the network otherwise remain. Clean up
only your own project, never another agent's. Use a shell exit trap or an
`if: always()` CI step so a failing test cannot bypass cleanup.

## Code style

- Write correct, readable code: clear names, one reason to change per module,
  explicit error handling, and the simplest design that meets the bead.
- No premature abstractions. A small helper does not justify a class hierarchy.
- No bare `except`. Catch telemetry failures at the package boundary and report
  them as bounded diagnostic codes; never let them reach application code.
- Ruff and mypy must pass on every touched file.

## Architecture rules

- Source lives in `src/django_altimeter/`. Create a module only in the bead
  that owns it; no empty placeholder files, profiles or tests.
- Only `apps.py`, `checks.py`, `django.py` and `management/commands/` import
  Django. Core modules (`conf`, `context`, `privacy`, `runtime`, `exporting`)
  never import consumer apps, Django models or the optional Celery/RQ libraries.
- No Django models, migrations, persistence, disk spool or outbox.
- Use frozen dataclasses for values and one composed `ProcessRuntime` per
  process. No singleton base class, service locator, registry, plugin system,
  DI container, event bus or generic worker/queue adapter. Keep Celery and RQ
  code concrete.
- Use native OTel, Django, Celery and RQ extension points. A subclass keeps
  upstream behavior and calls `super()` once.
- Setup is fail-open. Nothing starts a network call or exporter thread at
  import, `AppConfig.ready()`, migrations or shell.
- Pin narrowly: OTel SDK `>=1.45.1,<1.46` with the matching `0.66b1` contrib
  family; Django `>=5.2,<6.1`; Python 3.12 and 3.13. Widen a bound only after
  testing the new combination.

## Privacy and safety

- Export by allow-list only. Never export SQL text or parameters, raw paths or
  query strings, bodies, cookies, auth headers, task arguments or results,
  exception messages or locals, or unknown structured fields.
- Bound memory before batching. Reject oversized values by bounded traversal;
  never scan or encode unbounded user data to find out it is too large.
- Never put credentials in resources, logs or displayed configuration. Never
  hardcode secrets.
- Tests isolate Postgres, Redis databases and key prefixes, ports and browser
  origins. Never flush a shared Redis.

## Testing

- Each bead ships behavior, tests and docs together. Start with a failing test
  that exercises the observable contract, not a missing import.
- Prove behavior at the strength the bead requires: decoded OTLP from a real
  receiver, real servers, real Linux forks, the built wheel in a clean image,
  a real browser. An in-memory exporter, test client, eager task, SimpleWorker
  or mock never replaces a stronger required check.
- Mock only external boundaries. Keep a safe positive case beside every
  canary, forbidden-value or failure case.
- Never weaken an assertion, skip or xfail a failing test, regenerate goldens
  to force green, or hard-code a success path. A selection that runs zero
  tests is a failure.
- Do not silence stderr in a command whose output you cite as evidence.

## Git rules

- The orchestrator owns branches. Agents never create, switch, rebase, merge,
  push or run `gh stack`. Work only on the branch the orchestrator names.
- No git worktrees. Never pass `--worktrees`.
- Never amend, `git reset --hard`, `git clean`, `rm -rf`, or delete a tracked
  file without the orchestrator's approval. Never rewrite `main` or merged
  history.
- One index is shared by every lane in this checkout. Commit only the files you
  reserved, with the same path list on both commands:
  `git add -- <paths>` then `git commit -F <message-file> -- <same-paths>`.
  Never use bare `git commit`, `git commit -a`, `git add -A` or `git add .`.
- Commit message: `type(scope): summary (<bead-id>)`. Types: `feat`, `fix`,
  `refactor`, `test`, `docs`, `chore`.
- Work that belongs to a lower slice: stop that edit, comment on the owning
  bead and tell the orchestrator. Work for a later slice: file or update a bead
  with that slice's label. Never fold either into the current slice.

## Swarm coordination

1. Register with Agent Mail first. If registration fails, stop and post a bead
   comment.
2. Reserve paths exclusively before editing (bounded TTL, reason = bead ID).
   On a conflict, choose other work or wait. Never edit through a conflict or
   release another agent's reservation.
3. Release your reservations after you commit.
4. Post claims, blockers, review requests and handoffs as bead comments, and
   poll `br comments list <bead>` while active. Agent Mail messages are
   secondary.
5. Shell only. No Computer Use, GUI apps or editors.
6. Commit before you report a bead done. The handoff comment names the changed
   paths, exact commands, results and revision.
7. Only the orchestrator closes beads, citing evidence bound to a revision.

## Honest credit

- The goal is working, deployable capability. Certificates, ledgers,
  dashboards and meta-reports are not progress; create one only when it gates
  a named feature.
- Refusal-only or guard-only work never closes a positive-capability bead.
- Incomplete work stays open with a comment. Do not move unmet acceptance into
  a follow-up bead to close the original.
- Never present a fixture, mock or local run as live proof. Label each result's
  evidence class: unit, local real-process, browser or live.

## Maintenance

Update this file only when repo-wide workflow, safety rules or commands change.
