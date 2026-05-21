# PostgreSQL Schema And Migration Plan

## Purpose

Define the first PostgreSQL schema for BM Log Analyzer and the migration order from filesystem-based persistence to database-backed persistence.

This plan assumes:

- raw archives and extracted files stay on disk or object storage
- reports and AI results are stored in PostgreSQL
- stage and prod use separate databases

## Storage Boundary

Keep on disk:

- uploaded raw archives
- extracted raw files
- temporary work directories
- large binary source artifacts

Store in PostgreSQL:

- users
- sessions
- auth journal
- upload metadata
- history metadata
- reports
- AI analysis results
- storage policy
- admin dictionaries

## Core Tables

### `users`

Purpose: application users and roles.

Fields:

- `id` uuid primary key
- `email` citext unique not null
- `name` text not null
- `password_hash` text not null
- `role` text not null
- `created_at` timestamptz not null
- `updated_at` timestamptz not null

Indexes:

- unique on `email`
- index on `role`

### `sessions`

Purpose: login sessions with idle timeout.

Fields:

- `token` text primary key
- `user_id` uuid not null references `users(id)`
- `created_at` timestamptz not null
- `last_activity_at` timestamptz not null
- `expires_at` timestamptz not null
- `ip_address` inet null
- `user_agent` text null

Indexes:

- index on `user_id`
- index on `expires_at`

### `auth_journal`

Purpose: audit trail of authentication events.

Fields:

- `id` bigserial primary key
- `event_type` text not null
- `user_id` uuid null references `users(id)`
- `email` citext null
- `status` text null
- `ip_address` inet null
- `user_agent` text null
- `details` text null
- `created_at` timestamptz not null

Indexes:

- index on `created_at desc`
- index on `event_type`
- index on `email`

### `storage_policies`

Purpose: runtime-configurable retention and session policy.

Fields:

- `id` smallint primary key
- `archive_retention_days` integer not null
- `session_idle_minutes` integer not null
- `updated_at` timestamptz not null

Recommendation:

- keep a single row with `id = 1`

### `uploads`

Purpose: uploaded file metadata.

Fields:

- `upload_id` text primary key
- `created_at` timestamptz not null
- `original_name` text not null
- `stored_path` text not null
- `size_bytes` bigint not null
- `status` text not null
- `status_message` text not null
- `owner_user_id` uuid null references `users(id)`
- `owner_email` citext null
- `owner_name` text null
- `report_id` uuid null references `reports(id)`
- `report_url` text null
- `report_has_ai` boolean not null default false
- `retention_expires_at` timestamptz null
- `retention_note` text null

Indexes:

- index on `created_at desc`
- index on `owner_user_id`
- index on `report_id`
- index on `status`

### `history_runs`

Purpose: analysis run index and summary metadata.

Fields:

- `run_id` text primary key
- `created_at` timestamptz not null
- `mode` text not null
- `source` text not null
- `input_path` text not null
- `reports_dir` text not null
- `version` text not null
- `total` integer not null
- `success_count` integer not null
- `decline_count` integer not null
- `technical_error_count` integer not null
- `unknown_count` integer not null
- `bm_logs` integer not null
- `reader_logs` integer not null
- `system_logs` integer not null
- `owner_user_id` uuid null references `users(id)`
- `owner_email` citext null
- `owner_name` text null

Indexes:

- index on `created_at desc`
- index on `mode`
- index on `owner_user_id`

### `reports`

Purpose: canonical report storage.

Fields:

- `id` uuid primary key
- `run_id` text null references `history_runs(run_id)`
- `upload_id` text null references `uploads(upload_id)`
- `report_kind` text not null
- `schema_version` text not null
- `report_title` text not null
- `generated_at` timestamptz not null
- `source_version` text not null
- `report_json` jsonb not null
- `manifest_json` jsonb not null
- `html_cache` text null
- `html_updated_at` timestamptz null
- `content_checksum` text not null
- `is_stale` boolean not null default false

Indexes:

- unique partial index for active report per `run_id`
- unique partial index for active report per `upload_id`
- index on `generated_at desc`
- index on `schema_version`
- index on `is_stale`

### `ai_reports`

Purpose: persisted AI analysis payload tied to a report.

Fields:

- `id` uuid primary key
- `report_id` uuid not null references `reports(id)` on delete cascade
- `model` text not null
- `schema_version` text not null
- `generated_at` timestamptz not null
- `ai_json` jsonb not null
- `content_checksum` text not null
- `is_stale` boolean not null default false

Indexes:

- unique on `report_id`
- index on `generated_at desc`
- index on `is_stale`

### `check_cases`

Purpose: admin-managed validation rules.

Fields:

- `check_id` text primary key
- `title` text not null
- `description` text not null
- `severity` text not null
- `condition_type` text not null
- `condition_value` text not null
- `enabled` boolean not null default true
- `version` text not null
- `created_at` timestamptz not null
- `updated_at` timestamptz not null

### `carrier_rules`

Purpose: reusable dictionary for carrier detection.

Fields:

- `name` text primary key
- `match_type` text not null
- `markers` text[] not null
- `created_at` timestamptz not null
- `updated_at` timestamptz not null

## Report Versioning Strategy

Store reports as immutable versions.

Recommended rule:

- a new analysis run creates a new `reports` row
- the previous report is not overwritten
- `uploads.report_id` or `history_runs.run_id` points to the current report
- when a source upload changes, mark related report and AI rows `is_stale = true`

This preserves auditability and avoids silent mutation of earlier results.

## AI Result Strategy

AI payload should be stored alongside the report it was generated from.

Recommended rule:

- `ai_reports.report_id` is unique
- if report HTML changes materially, AI becomes stale
- if raw log input changes, AI becomes stale
- the UI should display cached AI immediately when present

## Query Flow

### `uploads`

1. read `uploads`
2. join `reports` if `report_id` exists
3. join `ai_reports` if cached AI exists
4. expose `report_has_ai`, `retention_note`, `retention_expires_at`

### `report/{run_id}`

1. find `history_runs`
2. find active `reports` row
3. if `html_cache` exists and is fresh, serve it
4. otherwise render from `report_json`, write HTML cache, and serve it

### `api/runs/{run_id}/ai-analysis`

1. find `reports` row
2. find `ai_reports` row
3. if present and not stale, return it
4. otherwise return `not_started` or trigger analysis depending on config

## Migration Order

### Phase 1

- add PostgreSQL settings and connection pool
- add migration runner
- add schema for `users`, `sessions`, `auth_journal`, `storage_policies`

### Phase 2

- migrate uploads metadata
- migrate history metadata
- keep filesystem-backed raw files unchanged

### Phase 3

- migrate report storage to PostgreSQL
- persist HTML cache plus canonical JSON
- persist AI payloads

### Phase 4

- migrate admin dictionaries
- drop legacy JSON file persistence for migrated entities

## Stage First Rule

Stage must always be migrated and tested before prod.

Required checks:

- apply migrations on stage database
- run app smoke tests
- verify `/uploads`
- verify `/report/{run_id}`
- verify AI result reuse
- verify retention and auth journal behavior

Only after stage is stable:

- apply the same migration set to prod

## Operational Requirements

- stage and prod databases must never share credentials
- stage and prod backups must be separate
- connection strings must live in separate secret files
- migration versioning must be identical between environments

## Open Questions

- Should report HTML be regenerated on deploy or only on schema change?
- Should old AI results remain visible after a report edit, or should they be hidden as stale?
- Do we keep a filesystem fallback for one release during the transition?

Recommended answers:

- regenerate only on schema change or cache miss
- hide stale AI by default, but keep it in history for audit
- keep a short filesystem fallback during the first release, then remove it after stage proves stable
