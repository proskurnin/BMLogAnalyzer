FROM python:3.13-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends libarchive-tools \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-dev.txt requirements-web.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt -r requirements-web.txt

COPY . .

EXPOSE 8000
CMD ["python", "-m", "web", "--host", "0.0.0.0", "--port", "8000"]
