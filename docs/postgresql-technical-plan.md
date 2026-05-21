# PostgreSQL Technical Plan

## Goal

Move persistent application state into PostgreSQL, while keeping uploaded raw archives on disk or in object storage.

Reports should be stored in PostgreSQL so that we do not rebuild them on every request, especially AI analysis results.

## Scope

Store in PostgreSQL:

- users
- sessions
- auth journal
- storage policy
- upload metadata
- history/run metadata
- generated reports
- AI analysis results
- admin check cases
- carrier rules and other reusable dictionaries

Keep outside PostgreSQL:

- raw uploaded archives
- extracted raw files
- temporary work directories
- large binary attachments if they appear later

## Why Reports Belong In PostgreSQL

Reports are derived artifacts, not source data.

Storing them in PostgreSQL gives us:

- no repeated rebuilds on every page open
- faster access to the last report and its AI result
- one stable source of truth for report metadata
- easier lifecycle management for report versions
- simpler backup and restore of “what the user saw”
- cleaner separation between raw logs and derived analysis

For AI analysis this matters even more, because the AI result is expensive to produce and should be reused until the underlying report changes.

## Recommended Storage Model

Use PostgreSQL as the system of record for all persistent metadata and generated report content.

Suggested data model:

- `users`
  - id, name, email, password hash, role, created_at, updated_at
- `sessions`
  - token, user_id, created_at, last_activity_at, expires_at, ip, user_agent
- `auth_journal`
  - id, event_type, user_id/email, status, ip, user_agent, details, created_at
- `storage_policies`
  - archive retention days, session idle minutes, updated_at
- `uploads`
  - upload id, original name, owner, created_at, status, size, stored path, report id, retention metadata
- `reports`
  - report id, upload id / run id, report kind, report title, schema version, created_at, generated_at, content JSON, HTML cache, manifest JSON, checksum, source version
- `ai_reports`
  - report id, generated_at, model, payload JSON, status, checksum
- `history_runs`
  - run id, source, mode, input path, reports dir, counts, snapshot JSON, report reference
- `check_cases`
  - rule catalog with enabled flag, severity, condition, version
- `carrier_rules`
  - markers and matching mode

## Report Storage Approach

Do not store only HTML.

Store the canonical data and derived rendering artifacts together:

- canonical report snapshot in JSONB
- rendered HTML in TEXT if we want instant serving
- report manifest in JSONB
- AI result in JSONB
- report checksum / version fields

That lets us:

- serve the last rendered report immediately
- rebuild HTML from canonical JSON only when the renderer changes
- compare versions and detect stale cache
- keep AI output tied to the exact report it was produced from

## Caching Rule

When a user opens a report:

1. check PostgreSQL for the current report record
2. if HTML cache exists and is fresh, serve it
3. if HTML cache is missing or stale, regenerate from canonical report JSON, save back to PostgreSQL, then serve

For AI:

1. if AI payload exists for the report, show it
2. if no AI payload exists, show the “run AI” action
3. if the report changed, invalidate the previous AI payload for that report version

## Stage And Prod Isolation

Stage and prod must have separate databases.

Required separation:

- different database names
- different users/passwords
- different connection strings
- separate backups
- separate migrations
- no shared credentials

Recommended naming:

- stage: `bm_log_analyzer_stage`
- prod: `bm_log_analyzer_prod`

## Deployment Rules

- stage is always updated first
- stage gets migrations first
- stage is used to validate schema and report compatibility
- prod is updated only after stage passes

## Migration Strategy

Migrations should be explicit and versioned.

Recommended order:

1. add PostgreSQL settings and connection layer
2. keep current filesystem storage as fallback
3. move auth metadata to PostgreSQL
4. move uploads metadata to PostgreSQL
5. move report metadata and AI payloads to PostgreSQL
6. move history indexes to PostgreSQL
7. move admin catalogs to PostgreSQL
8. remove filesystem dependency for derived artifacts after parity is proven

## API Impact

The web app should continue using the same core analysis engine.

Only persistence changes:

- reading users, sessions, uploads, reports, and settings from PostgreSQL
- saving generated report artifacts into PostgreSQL
- loading the latest report and AI payload from PostgreSQL instead of rebuilding from disk

Raw archive processing remains unchanged.

## Operational Benefits

PostgreSQL will give us:

- fewer duplicate computations
- deterministic report retrieval
- easier audit of who created what and when
- easier stage/prod separation
- simpler admin queries
- more reliable backup/restore of operational state
- a foundation for multi-user workflows
- room for future search/filtering over reports and AI results

## Risks

- report blobs can grow quickly
- backup size and restore time will increase
- migrations need discipline
- stale cache handling must be explicit
- stage/prod separation must be enforced in deployment, not only in code

## Suggested First Implementation

1. add a PostgreSQL connection module
2. implement tables for users, sessions, uploads, reports, auth journal, policies
3. persist generated reports and AI results in PostgreSQL
4. keep filesystem archives unchanged
5. make `/uploads` and `/report/{run_id}` read report artifacts from PostgreSQL first
6. verify stage with a dedicated stage database
7. promote the same migration set to prod only after stage is validated

## Open Decision

We should decide whether report HTML is stored:

- only as rendered HTML
- or as canonical JSON plus cached HTML

Recommended option: canonical JSON plus cached HTML, because it is safer for future renderer changes and easier to invalidate.
