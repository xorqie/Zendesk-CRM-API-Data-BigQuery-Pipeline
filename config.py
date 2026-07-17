"""
config.py
---------
Centralized, environment-driven configuration for the Zendesk -> BigQuery
pipeline. No secrets live in source code.

The original scripts hardcoded a live Zendesk API token and inconsistent
service-account paths (`/home/wassim_bendrimia/auth.json` in some files,
`/root/auth.json` in others - almost certainly a leftover from testing on
different machines) directly in source. Both now come from the
environment. Copy `.env.example` to `.env` and fill in your own values.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Missing required environment variable: '{name}'. "
            f"See .env.example for the full list of required variables."
        )
    return value


@dataclass(frozen=True)
class BigQueryConfig:
    project_id: str = field(default_factory=lambda: _require_env("BQ_PROJECT_ID"))
    dataset_id: str = field(default_factory=lambda: os.getenv("BQ_DATASET_ID", "zendesk"))
    credentials_path: str = field(default_factory=lambda: _require_env("GOOGLE_APPLICATION_CREDENTIALS"))

    users_table: str = field(default_factory=lambda: os.getenv("BQ_USERS_TABLE", "users"))
    tickets_table: str = field(default_factory=lambda: os.getenv("BQ_TICKETS_TABLE", "tickets"))
    csat_table: str = field(default_factory=lambda: os.getenv("BQ_CSAT_TABLE", "csat"))
    kpi_table: str = field(default_factory=lambda: os.getenv("BQ_KPI_TABLE", "ticket_metrics"))


@dataclass(frozen=True)
class ZendeskConfig:
    subdomain: str = field(default_factory=lambda: _require_env("ZENDESK_SUBDOMAIN"))
    email: str = field(default_factory=lambda: _require_env("ZENDESK_EMAIL"))
    api_token: str = field(default_factory=lambda: _require_env("ZENDESK_API_TOKEN"))

    @property
    def base_url(self) -> str:
        return f"https://{self.subdomain}.zendesk.com/api/v2"

    @property
    def auth(self) -> tuple:
        """Zendesk token auth: username is 'email/token', password is the API token."""
        return f"{self.email}/token", self.api_token

    def ticket_deep_link(self, ticket_id) -> str:
        return f"https://{self.subdomain}.zendesk.com/agent/tickets/{ticket_id}"


# How many days of ticket history to backfill on an incremental sync.
TICKETS_LOOKBACK_DAYS = int(os.getenv("TICKETS_LOOKBACK_DAYS", "180"))

# Records are uploaded to BigQuery in chunks to keep memory use flat on
# large exports (ticket history in particular can run into the tens of
# thousands of rows).
UPLOAD_CHUNK_SIZE = int(os.getenv("UPLOAD_CHUNK_SIZE", "1000"))

bigquery_config = BigQueryConfig
zendesk_config = ZendeskConfig
