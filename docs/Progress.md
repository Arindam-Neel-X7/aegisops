# AegisOps Implementation Progress

**Last updated:** 2026-09-18
**Current phase:** 0 - Foundation
**Current step:** 3 - PostgreSQL and Alembic foundation ✅
**Status:** Complete — all live PostgreSQL verifications passed

## Verified completed work

### Step 1 - Monorepo scaffold and conventions

The repository has the planned top-level backend, frontend, infrastructure,
research, documentation, and scripts directories. The README, Git ignore rules,
editor configuration, and GitHub remote are present.

### Step 2 - Backend skeleton and health endpoints

The FastAPI application factory is operational with correlation IDs, structured
logging, CORS configuration, security response headers, health endpoints, and
OpenAPI documentation. Its test suite covers these contracts.

### Step 3 - Database and migration foundation ✅

The backend now contains:

- Async PostgreSQL connectivity through SQLAlchemy and `asyncpg`, including
  connection pooling and disposal during application shutdown.
- A database-backed `/ready` endpoint. It returns `200 {"status":"ready"}`
  only after `SELECT 1` succeeds; otherwise it returns `503`.
- SQLAlchemy models for the schema's global `plans` dependency and the planned
  tenant, user, tenant-membership, and session foundations.
- Shared UUID, UTC timestamp, and soft-delete columns on all foundation models.
- An asynchronous Alembic environment and a reversible first migration,
  `20260918_0001`.
- PostgreSQL `pgcrypto` UUID defaults, required indexes, risk and role check
  constraints, `updated_at` triggers, plan seed data, and RLS policies.
- A `docker-compose.yml` with PostgreSQL 16 Alpine, health checks, and a
  named volume for development data persistence.

`plans` is intentionally included in the first migration because `tenants` has
a required foreign key to `plans.id`; omitting it would make the schema invalid.

## Live PostgreSQL verification performed

All verifications were executed against a disposable `postgres:16-alpine`
Docker container on 2026-09-18.

### alembic upgrade head
- Migration `20260918_0001` applied cleanly with no errors.

### Foundation tables (5 of 5 present)
- `plans`, `users`, `tenants`, `tenant_memberships`, `sessions`
- All tables have the `id`, `created_at`, `updated_at`, `is_deleted` columns.

### Seed data
- 3 plan rows inserted: Free Trial ($0), Professional ($49), Enterprise ($199).
- Deterministic UUIDs (`00000000-0000-0000-0000-00000000000{1,2,3}`).

### Indexes (17 total)
- Primary keys, unique constraints, and query-performance indexes all present.

### Check constraints
- `ck_membership_role` restricts role to `admin`, `sre`, `viewer`.
- `ck_membership_max_risk_tolerance` restricts risk tolerance to 0–100.

### updated_at triggers (5 of 5)
- `trg_{table}_updated_at` BEFORE UPDATE triggers on all foundation tables.

### RLS policies (5 of 5)
- `plans` and `users`: `USING (true)` — globally readable.
- `tenants`: tenant isolation via `id = current_setting('app.current_tenant_id')`.
- `tenant_memberships` and `sessions`: tenant isolation via `tenant_id`.

### pgcrypto extension
- Confirmed present and providing `gen_random_uuid()` for UUID defaults.

### /ready endpoint
- FastAPI app started successfully with `uvicorn`.
- `GET /ready` returned `200 {"status": "ready"}` with live database.
- `GET /health` returned `200 {"status": "ok", "version": "0.1.0"}`.

### Downgrade/upgrade round trip
- `alembic downgrade base` dropped all 5 tables, the trigger function, and
  left only `alembic_version`.
- `alembic upgrade head` recreated all tables, seed data, indexes, constraints,
  triggers, RLS policies, and the pgcrypto extension identically.

### Test suite
- All 8 backend tests pass (API contracts, DB failure, model metadata, migration head).

## Immediate next action

Step 4 (frontend foundation) is ready to begin.
