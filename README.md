# Zendesk → BigQuery Support Analytics Pipeline

A production-style data pipeline that extracts customer support data
(tickets, users, CSAT ratings, and SLA/response-time metrics) from the
**Zendesk API** and loads it into **Google BigQuery**, turning day-to-day
helpdesk activity into a queryable analytics layer.

---

## What problem this solves

Zendesk is built for running support day-to-day - working tickets,
tracking CSAT per interaction, watching SLA timers - but it isn't built
for longitudinal analysis: trend lines across quarters, cross-referencing
CSAT against reply time, or building an executive dashboard that isn't
locked into Zendesk's own reporting UI. This pipeline lands that data in
BigQuery so those questions become a SQL query and a Looker Studio /
Tableau dashboard instead of manual exports.

**Corporate value, concretely:**
- **SLA / response-time reporting** — reply time, resolution time, and
  wait time (calendar vs. business hours) per agent, team, or ticket
  form, with a pre-computed SLA-met flag.
- **CSAT trend analysis** — satisfaction rate over time, by agent or by
  group, joinable directly against the tickets that drove each rating.
- **Ticket volume & channel mix** — how tickets are arriving (`channel`)
  and how that's shifted over the last 6 months.
- **A historical record Zendesk itself doesn't keep in query-able form**
  — the source of truth for "what did support performance look like last
  quarter," independent of Zendesk's own data retention/reporting limits.

---

## Features

- **Four independent pipelines** — users, tickets, CSAT, ticket metrics
  (KPIs/SLA) — each runnable on its own or together.
- **One shared API client** replacing four separately-implemented fetch
  loops, with consistent retry/backoff and two pagination styles handled
  correctly: cursor-based (`links.next`) for most endpoints, and
  Zendesk's purpose-built incremental export cursor for tickets.
- **Explicit BigQuery schemas** per table (kept from the original
  scripts, which got this right) instead of relying on autodetected
  types — column types stay stable across runs regardless of which
  fields happen to be null in a given batch.
- **Consistent `MERGE`-based upserts** everywhere, with built-in
  deduplication (`ROW_NUMBER() OVER (PARTITION BY id ORDER BY ... DESC)`)
  in case the same record appears twice in a single sync.
- **Chunked staging for large exports** — the tickets pipeline can stream
  many thousands of rows into a staging table across multiple chunks
  before a single final merge, keeping memory use flat.
- **Environment-based configuration** — zero secrets in source code.
- **Structured logging** throughout.

### Issues fixed during the rebuild

- **`Zendesk_kpi.py` did not compile.** The uploaded file had its entire
  contents — every function and `main()` — duplicated end-to-end in a
  single file, followed by a stray, non-Python fragment of a pasted
  BigQuery schema definition after the final `if __name__ == "__main__"`
  block. Running `python -m py_compile` on it fails with a `SyntaxError`.
  This rebuild consolidates it into one working `kpi_pipeline.py`,
  keeping the more correct of the two duplicated SLA calculations (the
  second copy explicitly handles values at the 24-hour boundary; the
  first left that case ambiguous).

- **`ticket_id` was formatted inconsistently across tables** — an API URL
  (`.../api/v2/tickets/{id}.json`) in the tickets script, but an agent-UI
  deep link (`.../agent/tickets/{id}`) in the CSAT script — which meant
  the two tables couldn't be joined on that column as-is. Unified to the
  agent-UI link everywhere (see `docs/schema.md`).
- **Assignee/group/form/brand name lookups silently missed** whenever
  Zendesk returned an ID as a float (`2.0`) rather than an int-like string
  (`"2"`) — `assignees.get(str(x), str(x))` looked up the key `"2.0"`
  against a map keyed by `"2"`, missed, and fell back to the raw numeric
  ID instead of the resolved name. Only the CSAT script handled this
  correctly (`str(int(x))`); the tickets script didn't. Fixed by
  normalizing every ID lookup the same way in `tickets_pipeline.py`.

---

## Architecture

```
                ┌────────────────────┐
                │     Zendesk API     │
                │ (users, tickets,    │
                │  satisfaction_      │
                │  ratings,           │
                │  ticket_metrics)    │
                └─────────┬───────────┘
                          │  REST (cursor & incremental-export pagination,
                          │  retried with backoff)
                          ▼
                ┌────────────────────┐
                │   ZendeskClient     │  src/zendesk_client.py
                │   (extract layer)   │
                └─────────┬───────────┘
                          │  raw JSON
                          ▼
                ┌────────────────────┐
                │  Pipeline modules   │  src/pipelines/*.py
                │  (transform layer): │
                │  clean, flatten,    │
                │  resolve names,     │
                │  derive SLA flags   │
                └─────────┬───────────┘
                          │  pandas DataFrame (typed to an explicit schema)
                          ▼
                ┌────────────────────┐
                │   BigQueryLoader    │  src/bigquery_loader.py
                │   (load layer):     │
                │   stage (chunked) → │
                │   dedupe → MERGE    │
                └─────────┬───────────┘
                          │
                          ▼
                ┌────────────────────┐
                │   Google BigQuery   │
                │   zendesk dataset   │
                └─────────────────────┘
```

### Why BigQuery?

Serverless, cheap at this scale, and connects natively to the BI tools
(Looker Studio, Tableau) a support/operations team would already use to
build dashboards on this data.

### How the Zendesk API works in this project

Zendesk uses two different pagination styles depending on the endpoint,
and this pipeline handles both:
- **Cursor pagination** (`links.next` in the response body) — used for
  `users`, `satisfaction_ratings`, and `ticket_metrics`.
- **Incremental export cursor** (`start_time` / `end_time` /
  `end_of_stream`) — used specifically for `tickets`, via
  `/api/v2/incremental/tickets.json`. This endpoint is purpose-built by
  Zendesk for reliable incremental syncing and guarantees no ticket is
  skipped even if it's updated mid-export, which a plain `updated_at`
  filter can't guarantee.

Authentication uses HTTP Basic Auth with `{email}/token` as the username
and an API token as the password, per
[Zendesk's authentication docs](https://developer.zendesk.com/api-reference/introduction/security-and-auth/).

---

## Technologies Used

| Layer | Tool |
|---|---|
| Language | Python 3.10+ |
| Data manipulation | pandas, numpy |
| Data warehouse | Google BigQuery |
| Source API | Zendesk REST API v2 |
| HTTP client | requests (custom retry/backoff logic) |
| Configuration | python-dotenv + environment variables |
| Auth | GCP Service Account (BigQuery), Zendesk API token |

---

## Installation

```bash
git clone https://github.com/<your-username>/zendesk-bigquery-pipeline.git
cd zendesk-bigquery-pipeline

python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

## Configuration

1. Copy the example environment file:

   ```bash
   cp .env.example .env
   ```

2. Populate `.env`:

   | Variable | Description |
   |---|---|
   | `ZENDESK_SUBDOMAIN` | Your Zendesk subdomain, e.g. `acme` for `acme.zendesk.com` |
   | `ZENDESK_EMAIL` | Email of the agent account the API token belongs to |
   | `ZENDESK_API_TOKEN` | API token (Admin Center → Apps and integrations → APIs → Zendesk API) |
   | `BQ_PROJECT_ID` | Your GCP project ID |
   | `BQ_DATASET_ID` | BigQuery dataset name (defaults to `zendesk`) |
   | `BQ_*_TABLE` | Per-table name overrides (optional) |
   | `GOOGLE_APPLICATION_CREDENTIALS` | Path to a GCP service account JSON key with BigQuery Data Editor + Job User roles |
   | `TICKETS_LOOKBACK_DAYS` | How many days of ticket history to sync (default `180`) |
   | `UPLOAD_CHUNK_SIZE` | Rows per staging chunk on large exports (default `1000`) |

3. Create the target BigQuery dataset if it doesn't exist yet:

   ```bash
   bq mk --dataset "$BQ_PROJECT_ID:$BQ_DATASET_ID"
   ```

   Tables are created automatically on first load, using each pipeline's
   explicit schema.

## Running the Project

```bash
# Run every pipeline
python main.py

# Run a specific subset
python main.py users tickets
python main.py kpi
```

### Example workflow

```bash
# crontab: sync users/CSAT daily, tickets and KPIs a few times a day
0 2 * * *    cd /path/to/project && venv/bin/python main.py users csat        >> logs/daily.log 2>&1
0 */4 * * *  cd /path/to/project && venv/bin/python main.py tickets kpi        >> logs/refresh.log 2>&1
```

An example (corrected) Supervisor config is included at
[`deploy/supervisor.conf.example`](deploy/supervisor.conf.example) for
environments that prefer Supervisor over cron.

---

## Project Structure

```
.
├── main.py                       # CLI entrypoint - runs all or specific pipelines
├── config.py                     # Environment-driven configuration
├── requirements.txt
├── .env.example
├── deploy/
│   └── supervisor.conf.example    # Corrected example scheduler config
├── src/
│   ├── zendesk_client.py          # Extract layer: pagination, retries, reference lookups
│   ├── bigquery_loader.py         # Load layer: chunked staging + dedupe + MERGE
│   ├── pipelines/
│   │   ├── users_pipeline.py
│   │   ├── tickets_pipeline.py
│   │   ├── csat_pipeline.py
│   │   └── kpi_pipeline.py
│   └── utils/
│       └── logger.py
└── docs/
    └── schema.md                  # Column-level BigQuery schema + entity relationships
```

---

## BigQuery Schema

See [`docs/schema.md`](docs/schema.md) for the full column-level schema
of every table, and how the tables relate to each other (`tickets` as the
central fact table, joined by `csat` and `ticket_metrics`, referencing
`users`).

## API Resources Used

| Zendesk Endpoint | Pipeline |
|---|---|
| `GET /api/v2/users.json` | `users_pipeline.py` |
| `GET /api/v2/incremental/tickets.json` | `tickets_pipeline.py` |
| `GET /api/v2/ticket_forms.json`, `/brands.json`, `/groups/assignable`, `/capacity/rules/assignees` | `tickets_pipeline.py` (reference data) |
| `GET /api/v2/satisfaction_ratings.json` | `csat_pipeline.py` |
| `GET /api/v2/ticket_metrics` | `kpi_pipeline.py` |
| `GET /api/v2/users/show_many` | `csat_pipeline.py`, `kpi_pipeline.py` (name resolution) |

---

## Future Improvements

- **Data validation** (e.g. `pandera`) before load, to catch schema drift
  or unexpected nulls in fields like `sla_met_business` or `score`.
- **dbt models** on top of the raw tables for SLA compliance and CSAT
  trend reporting, keeping transformation SQL out of Python.
- **Unit tests** for each pipeline's `transform()` function — already
  isolated from I/O to make this straightforward.
- **Orchestration** (Airflow/Dagster) for dependency-aware scheduling,
  retries, and alerting in place of cron/Supervisor.
- **Incremental CSAT/KPI syncs** — both currently do a full re-pull each
  run; Zendesk's search/incremental endpoints could narrow this to
  "changed since last run" as data volume grows.
- **Containerization** (Dockerfile + Cloud Run job) for portable,
  infrastructure-agnostic scheduling.

---

## License

Released under the [MIT License](LICENSE).
