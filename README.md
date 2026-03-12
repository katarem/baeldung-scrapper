# baeldung-java-daily-scrapper

Foundational implementation for a daily Baeldung Java article scraper with pluggable cloud storage sync.

Current phase includes:

- project structure and dependency manifests
- domain contracts and interfaces
- runtime configuration validation
- S3-compatible storage path and manifest conventions
- initial tests for configuration and domain invariants
- single-run pipeline entrypoint for external schedulers

## Configuration

All runtime settings use the `SCRAPER_` prefix.

### Destination folder configuration

- `SCRAPER_DESTINATION_FOLDER_PATH` - provider-agnostic artifact root path (relative path, no leading `/`)
- `SCRAPER_STORAGE_MANIFEST_PATH` - manifest file path relative to `SCRAPER_DESTINATION_FOLDER_PATH`

### Fetch backend selection

- `SCRAPER_FETCH_BACKEND` - `playwright` (default) or `httpx`
- `SCRAPER_LOG_LEVEL` - `DEBUG`, `INFO` (default), `WARNING`, or `ERROR`

The default (`playwright`) is recommended for Baeldung because it can pass Cloudflare JS challenges that often block plain HTTP clients.

When using `playwright`, install browser binaries once on the runtime image/host:

```bash
playwright install chromium
```

### Storage backend selection

- `SCRAPER_STORAGE_BACKEND` - `s3` (default) or `local`
- `SCRAPER_LOCAL_BASE_DIRECTORY` - required when `SCRAPER_STORAGE_BACKEND=local`; base directory for artifact files

### S3 (Railway bucket) configuration

- `SCRAPER_S3_ENDPOINT` - S3-compatible endpoint URL, for example `https://<service>.up.railway.app`
- `SCRAPER_S3_REGION` - S3 region string (for Railway commonly `us-east-1`)
- `SCRAPER_S3_BUCKET` - destination bucket/container name
- `SCRAPER_S3_ACCESS_KEY_ID` - access key id
- `SCRAPER_S3_SECRET_ACCESS_KEY` - secret access key
- `SCRAPER_S3_FORCE_PATH_STYLE` - `true` or `false` (default `true`, recommended for Railway)

Rules enforced by validation:

- destination and manifest paths must be relative and must not include `.` or `..` segments
- manifest path must end with `.json`
- all S3 settings are required when `SCRAPER_STORAGE_BACKEND=s3` (`ENDPOINT`, `REGION`, `BUCKET`, access keys)
- `SCRAPER_LOCAL_BASE_DIRECTORY` is required when `SCRAPER_STORAGE_BACKEND=local`

### Example: Railway S3

```bash
export SCRAPER_DESTINATION_FOLDER_PATH=team/baeldung-java-daily
export SCRAPER_STORAGE_MANIFEST_PATH=manifests/latest.json
export SCRAPER_STORAGE_BACKEND=s3
export SCRAPER_S3_ENDPOINT=https://your-service.up.railway.app
export SCRAPER_S3_REGION=us-east-1
export SCRAPER_S3_BUCKET=baeldung-artifacts
export SCRAPER_S3_ACCESS_KEY_ID=your-access-key-id
export SCRAPER_S3_SECRET_ACCESS_KEY=your-secret-access-key
export SCRAPER_S3_FORCE_PATH_STYLE=true
```

### Example: Local filesystem mode

```bash
export SCRAPER_DESTINATION_FOLDER_PATH=team/baeldung-java-daily
export SCRAPER_STORAGE_MANIFEST_PATH=manifests/latest.json
export SCRAPER_STORAGE_BACKEND=local
export SCRAPER_LOCAL_BASE_DIRECTORY=.tmp/baeldung-artifacts
```

In local mode, files are written under:

- `.tmp/baeldung-artifacts/team/baeldung-java-daily/articles/...`
- `.tmp/baeldung-artifacts/team/baeldung-java-daily/indexes/...`
- `.tmp/baeldung-artifacts/team/baeldung-java-daily/manifests/latest.json`

The adapter-level unit tests do not require credentials because they use a fake S3 client.

### Discovery URL scope

Discovery intentionally excludes known non-editorial routes, including:

- `/courses/*`
- `/category/*`, `/tag/*`, `/author/*`
- feeds/search/wp-json and static assets (`.xml`, images, css, js, pdf)

This keeps processing focused on editorial article pages.

### Local test strategy for cloud adapter

- unit tests mock/fake provider clients and do not make network calls
- adapter tests cover create/update/no-op upsert paths plus exists/read behavior
- run locally with `pytest`

## Run One Scrape Cycle

This project intentionally exposes a single run command and expects scheduling to be handled externally.
No in-app scheduler or overlap lock is implemented.

### Required environment variables

- `SCRAPER_STORAGE_BACKEND` (`s3` or `local`, default `s3`)
- `SCRAPER_FETCH_BACKEND` (`playwright` or `httpx`, default `playwright`)
- if backend is `s3`: `SCRAPER_S3_ENDPOINT`, `SCRAPER_S3_REGION`, `SCRAPER_S3_BUCKET`, `SCRAPER_S3_ACCESS_KEY_ID`, `SCRAPER_S3_SECRET_ACCESS_KEY`
- if backend is `local`: `SCRAPER_LOCAL_BASE_DIRECTORY`

Optional overrides:

- `SCRAPER_DESTINATION_FOLDER_PATH` (default `baeldung-java-daily`)
- `SCRAPER_STORAGE_MANIFEST_PATH` (default `manifests/latest.json`)
- `SCRAPER_SOURCE_BASE_URL` (default `https://www.baeldung.com`)
- `SCRAPER_SOURCE_TIMEOUT_SECONDS` (default `15`)
- `SCRAPER_S3_FORCE_PATH_STYLE` (default `true`)

If you keep the default `SCRAPER_FETCH_BACKEND=playwright`, ensure `playwright install chromium` has been executed in the same environment before running `baeldung-scrapper-run`.

### Command

```bash
baeldung-scrapper-run
```

The command emits concise progress logs to stderr and prints a JSON run report to stdout.

Typical scheduler-visible log events include:

- `run_start`
- `run_discovery_start` / `run_discovery_complete`
- `discovery_source_scanned` / `discovery_milestone`
- `article_processing_start` / `article_processing_succeeded` / `article_processing_failed`
- `run_complete`

Example log output:

```text
2026-03-12 20:30:00,101 INFO baeldung_scrapper.pipeline.daily_run run_start fetch_backend='playwright' source_base_url='https://www.baeldung.com' storage_backend='s3'
2026-03-12 20:30:01,112 INFO baeldung_scrapper.fetching.baeldung_discovery discovery_milestone dedup_total=124 found=47 source='/java-tutorial'
2026-03-12 20:30:08,435 INFO baeldung_scrapper.pipeline.daily_run article_processing_start article_url='https://www.baeldung.com/java-streams' position=18 total=124
2026-03-12 20:30:10,009 INFO baeldung_scrapper.pipeline.daily_run run_complete discovered_count=124 failed_count=3 manifest_path='team/baeldung-java-daily/manifests/latest.json' succeeded_count=121
```

The JSON report still includes discovered, succeeded, failed counts, manifest path, and failure details.

## Article Artifact Structure

Each scraped article is stored as one JSON artifact at:

- `<destination_folder>/articles/java/<source-id>.json`

Current schema (`schema_version: 1.0`) looks like:

```json
{
  "schema_version": "1.0",
  "source_url": "https://www.baeldung.com/java-streams",
  "title": "Guide to Java Streams",
  "published_at": "2026-03-12T19:00:00+00:00",
  "updated_at": null,
  "author": "John Doe",
  "reviewer": "Jane Doe",
  "tags": ["java", "streams"],
  "body_blocks": [
    {
      "kind": "heading",
      "text": "Introduction",
      "level": 2,
      "items": [],
      "html": null,
      "language": null,
      "src": null,
      "alt": null,
      "caption": null
    },
    {
      "kind": "code",
      "text": "List<String> names = ...",
      "level": null,
      "items": [],
      "html": null,
      "language": "java",
      "src": null,
      "alt": null,
      "caption": null
    },
    {
      "kind": "image",
      "text": null,
      "level": null,
      "items": [],
      "html": null,
      "language": null,
      "src": "https://www.baeldung.com/wp-content/...png",
      "alt": "Stream pipeline diagram",
      "caption": "Example pipeline"
    }
  ],
  "validation_payload": {
    "source_code_blocks": 6,
    "extracted_code_blocks": 6,
    "source_images": 2,
    "extracted_images": 2
  }
}
```

`body_blocks.kind` supports: `paragraph`, `heading`, `list`, `blockquote`, `table`, `code`, `image`.
