# PostgreSQL Stage Rollout Checklist

## Goal

Roll out PostgreSQL on stage first, validate behavior, then promote the same migration set to prod only after stage is proven stable.

## Preconditions

- stage and prod already run on separate servers or separate isolated deployment stacks
- each environment has its own PostgreSQL database
- raw archives remain on disk or object storage
- reports and AI results will move into PostgreSQL

## Required Environment Separation

Stage:

- dedicated database name
- dedicated database user
- dedicated password
- dedicated backup target
- dedicated migration history

Prod:

- separate database name
- separate database user
- separate password
- separate backup target
- separate migration history

Never reuse stage credentials in prod.

## Stage Rollout Steps

### 1. Prepare database

- create the stage database
- create the stage database user
- grant only the required schema permissions
- store the connection string in stage-only secret storage

### 2. Apply schema migrations

- run migrations on stage
- verify migration version recorded in the database
- verify schema objects exist

### 3. Load seed or migrated data

- migrate users
- migrate auth journal
- migrate storage policy
- migrate uploads metadata
- migrate history metadata
- migrate reports and AI payloads
- migrate admin dictionaries

### 4. Switch stage app persistence layer

- point stage app to PostgreSQL
- keep raw archives on disk
- verify app can read existing data
- verify app can write new records

### 5. Validate page behavior

- open `/uploads`
- open `/report/{run_id}`
- open `/admin`
- open `/api/runs/{run_id}/ai-analysis`
- verify auth journal still renders
- verify session idle timeout still works
- verify report links and AI labels still work

### 6. Validate cache behavior

- open a report twice
- confirm the second open uses stored report data and does not rebuild
- open a report with AI data
- confirm AI result is reused

### 7. Validate failure behavior

- stop PostgreSQL and confirm the app fails clearly
- confirm no silent data corruption
- confirm raw archives remain untouched

## Report Validation Checklist

For at least one existing upload:

- report can be opened without regeneration
- AI result can be opened without regeneration
- report HTML can be served from stored cache
- `report_has_ai` is true when AI data exists
- `retention_note` is displayed correctly

## Audit And Retention Checklist

- auth journal entries are still written
- session expiration still removes old sessions
- retention policy still computes archive expiry
- expired uploads still disappear from listings when source files are gone

## Backup Checklist

- stage DB backup can be restored to a clean test database
- restored database opens with the same schema version
- restored reports and AI payloads remain readable
- raw archive storage backup is handled separately

## Smoke Test Commands

Use these checks after migration:

```bash
curl -fsS https://bm-stage.proskurnin.ru/health
curl -fsS https://bm-stage.proskurnin.ru/uploads
curl -fsS https://bm-stage.proskurnin.ru/admin
curl -fsS https://bm-stage.proskurnin.ru/api/runs/latest
```

When report data is available:

```bash
curl -fsS https://bm-stage.proskurnin.ru/api/uploads?limit=5
curl -fsS https://bm-stage.proskurnin.ru/api/runs/<run_id>/ai-analysis
curl -fsS https://bm-stage.proskurnin.ru/report/<run_id>
```

## Go / No-Go Criteria

Proceed to prod only if all of the following are true:

- stage migration completed successfully
- stage app starts cleanly after migration
- stage `/uploads` renders with report links and retention notes
- stage AI payloads are served from storage, not recomputed
- no 500 responses on the tested paths
- no regression in auth, sessions, or admin pages
- backup and restore were verified on stage

## Rollback Plan

If stage fails:

- keep the filesystem-backed code path enabled for one release if needed
- revert the app deployment on stage
- leave prod untouched
- fix schema or application compatibility on stage first

## Promotion Rule

Only after stage passes:

1. apply the same migration set to prod
2. switch prod to the same persistence behavior
3. re-run the smoke checks
4. verify that the prod database is separate from stage

## Notes

- Reports should be stored as canonical JSON plus cached HTML.
- AI analysis should be stored separately from the base report, but linked to it.
- Raw archives should not move into PostgreSQL.
