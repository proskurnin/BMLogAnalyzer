# BM Log Analyzer Deployment

This document describes the minimum production deployment for the current FastAPI web MVP.

## Release checklist

1. Set a release version and changelog entry.
2. Prepare a persistent data directory, for example `/var/lib/bm-log-analyzer`.
3. Configure production environment variables.
4. Run the test suite before deployment.
5. Run the service behind Nginx with HTTPS.
6. Back up the persistent data directory.

## Production environment

Copy `.env.production.example` and change every secret before first start.

Required in production:

```bash
BM_APP_ENV=production
BM_DATA_DIR=/var/lib/bm-log-analyzer
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

## Install

Example on a Linux server:

```bash
sudo useradd --system --home /opt/bm-log-analyzer --shell /usr/sbin/nologin bmlog
sudo mkdir -p /opt/bm-log-analyzer /var/lib/bm-log-analyzer
sudo chown -R bmlog:bmlog /opt/bm-log-analyzer /var/lib/bm-log-analyzer
```

Deploy the project into `/opt/bm-log-analyzer`, create a venv, then install dependencies:

```bash
cd /opt/bm-log-analyzer
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt -r requirements-web.txt
.venv/bin/python -m pytest
```

## systemd

Create `/etc/systemd/system/bm-log-analyzer.service`:

```ini
[Unit]
Description=BM Log Analyzer web service
After=network.target

[Service]
User=bmlog
Group=bmlog
WorkingDirectory=/opt/bm-log-analyzer
EnvironmentFile=/etc/bm-log-analyzer.env
ExecStart=/opt/bm-log-analyzer/.venv/bin/python -m web --host 127.0.0.1 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now bm-log-analyzer
sudo systemctl status bm-log-analyzer
```

## Nginx

Example server block:

```nginx
server {
    listen 80;
    server_name logs.example.com;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl http2;
    server_name logs.example.com;

    client_max_body_size 2048m;

    ssl_certificate /etc/letsencrypt/live/logs.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/logs.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
    }
}
```

Use Let’s Encrypt or another valid TLS certificate.

## Backup and restore

Back up `BM_DATA_DIR` regularly. The minimal backup unit is the entire directory:

```bash
sudo tar -czf bm-log-analyzer-data-$(date +%F).tar.gz /var/lib/bm-log-analyzer
```

Restore by stopping the service, replacing the directory contents, fixing ownership, and starting the service:

```bash
sudo systemctl stop bm-log-analyzer
sudo chown -R bmlog:bmlog /var/lib/bm-log-analyzer
sudo systemctl start bm-log-analyzer
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
