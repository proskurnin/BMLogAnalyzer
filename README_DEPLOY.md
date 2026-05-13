# BM Log Analyzer Deployment

This document describes the Docker Compose deployment used by the current FastAPI web MVP.

## Release checklist

1. Set a release version and changelog entry.
2. Prepare persistent data directories.
3. Configure production environment variables.
4. Set up GitHub Actions secrets for deploy.
5. Run the test suite before deployment.
6. Run the service behind Nginx with HTTPS.
7. Back up the persistent data directories.

## Production environment

Create environment files on the server. The deploy workflow preserves these files across `git reset --hard`.

Required in production:

```bash
BM_APP_ENV=production
BM_DATA_DIR=/app/_workdir
BM_ADMIN_EMAIL=admin@example.com
BM_ADMIN_PASSWORD='use-a-long-random-password'
BM_COOKIE_SECURE=true
```

The app refuses to start in production if `BM_ADMIN_EMAIL` or `BM_ADMIN_PASSWORD` are missing, or if the password is still `admin`.

Upload limits:

```bash
BM_MAX_UPLOAD_FILE_MB=512
BM_MAX_UPLOAD_SESSION_MB=2048
BM_MAX_UPLOAD_FILES=200
```

Data stored under `BM_DATA_DIR`:

```text
auth/          users and sessions
upload_store/  uploaded original files
web_history/   analysis runs and generated reports
```

## Server Layout

The deployment mirrors deTilda:

```text
/opt/bm-log-analyzer-stage/src  staging git checkout
/opt/bm-log-analyzer-prod/src   production git checkout
/var/lib/bm-log-analyzer-stage  staging persistent data mounted as /app/_workdir
/var/lib/bm-log-analyzer-prod   production persistent data mounted as /app/_workdir
/home/bm/.env.staging           staging secrets, copied into checkout during deploy
/home/bm/.env.prod              production secrets, copied into checkout during deploy
```

The server needs Docker Compose:

```bash
docker compose version
```

## GitHub Actions

The repository contains two workflows:

* `.github/workflows/tests.yml` runs tests on every push to `main` and every pull request targeting `main`.
* `.github/workflows/deploy.yml` matches the deTilda flow:
  * push to `main` runs tests and deploys staging
  * push to `prod` runs tests and deploys production
  * pull requests targeting `main` or `prod` run tests only
  * manual `workflow_dispatch` is supported

Required GitHub secrets:

```bash
STAGING_HOST=109.172.30.33
STAGING_USER=root
STAGING_SSH_KEY='private key for the staging deploy account'
PROD_HOST=109.172.30.33
PROD_USER=root
PROD_SSH_KEY='private key for the production deploy account'
```

During migration, `deploy.yml` also accepts the older shared `DEPLOY_HOST`, `DEPLOY_USER` and `DEPLOY_SSH_KEY` secrets as a fallback.

Recommended GitHub environments:

* `stage`
* `production`

For `production`, set required reviewers in GitHub so that prod deploys need an explicit approval.

The deploy workflow updates the already provisioned server directories and then runs Docker Compose:

```bash
docker compose -f docker-compose.staging.yml up -d --build --force-recreate
docker compose -f docker-compose.prod.yml up -d --build --force-recreate
```

## Docker Compose

Staging exposes the app on localhost port `8010`:

```bash
docker compose -f docker-compose.staging.yml up -d --build
```

Production exposes the app on localhost port `8011`:

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

## Nginx

Nginx configs are stored in the repository and copied by the deploy workflow:

```text
nginx/staging.conf -> /etc/nginx/sites-available/bm-log-analyzer-stage
nginx/prod.conf    -> /etc/nginx/sites-available/bm-log-analyzer-prod
```

Stage uses `bm-stage.proskurnin.ru` and proxies to `127.0.0.1:8010`.
Production uses `bm.proskurnin.ru` and proxies to `127.0.0.1:8011`.

## Backup and restore

Back up persistent data regularly. The minimal backup units are the entire data directories:

```bash
sudo tar -czf bm-log-analyzer-stage-data-$(date +%F).tar.gz /var/lib/bm-log-analyzer-stage
sudo tar -czf bm-log-analyzer-prod-data-$(date +%F).tar.gz /var/lib/bm-log-analyzer-prod
```

Restore by stopping the container, replacing the directory contents, fixing ownership, and starting the container:

```bash
docker compose -f docker-compose.prod.yml down
sudo chown -R bm:bm /var/lib/bm-log-analyzer-prod
docker compose -f docker-compose.prod.yml up -d
```

## Manual smoke test

1. Open `/login`.
2. Log in as the configured admin.
3. Open `/admin` and create a regular user.
4. Log in as that user.
5. Upload a `.log` or supported archive.
6. Open `/profile` and confirm the uploaded file is listed.
7. Log in as admin and confirm `/uploads` shows the `Пользователь` column.
8. Build a report from selected uploads and verify the report opens.

## Current storage note

This release still uses JSON files and local filesystem storage. That is acceptable for a small closed beta, provided `BM_DATA_DIR` is persistent and backed up. PostgreSQL should be added before multi-tenant or high-volume production use.
