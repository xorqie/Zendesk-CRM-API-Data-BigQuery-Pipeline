"""
users_pipeline.py
--------------------
Extracts all Zendesk users (agents, admins, and end-users/customers) and
upserts them into BigQuery.

This is a full extraction, not incremental: the users list is Zendesk's
full account roster, which is small enough (relative to tickets) that a
full re-pull on every run is simple and keeps the table trivially
consistent, at negligible extra cost.
"""

from typing import Any, Dict, List

import pandas as pd
from google.cloud import bigquery

from config import ZendeskConfig, BigQueryConfig
from src.zendesk_client import ZendeskClient
from src.bigquery_loader import BigQueryLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)

SCHEMA = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("organization_id", "STRING"),
    bigquery.SchemaField("role", "STRING"),
    bigquery.SchemaField("alias", "STRING"),
    bigquery.SchemaField("active", "BOOLEAN"),
    bigquery.SchemaField("shared", "BOOLEAN"),
    bigquery.SchemaField("shared_agent", "BOOLEAN"),
    bigquery.SchemaField("last_login_at", "STRING"),
    bigquery.SchemaField("two_factor_auth_enabled", "BOOLEAN"),
    bigquery.SchemaField("signature", "STRING"),
    bigquery.SchemaField("custom_role_id", "STRING"),
    bigquery.SchemaField("moderator", "BOOLEAN"),
    bigquery.SchemaField("restricted_agent", "BOOLEAN"),
    bigquery.SchemaField("suspended", "BOOLEAN"),
    bigquery.SchemaField("default_group_id", "STRING"),
]

STRING_COLUMNS = [f.name for f in SCHEMA if f.field_type == "STRING"]
BOOLEAN_COLUMNS = [f.name for f in SCHEMA if f.field_type == "BOOLEAN"]


def transform(records: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    for col in [f.name for f in SCHEMA]:
        if col not in df.columns:
            df[col] = None

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    for col in BOOLEAN_COLUMNS:
        df[col] = df[col].fillna(False).astype(bool)

    for col in STRING_COLUMNS:
        df[col] = df[col].astype(str).replace({"None": None, "nan": None})

    return df[[f.name for f in SCHEMA]]


def run() -> None:
    zendesk_cfg = ZendeskConfig()
    bq_cfg = BigQueryConfig()

    client = ZendeskClient(auth=zendesk_cfg.auth)
    loader = BigQueryLoader(bq_cfg.credentials_path, bq_cfg.project_id, bq_cfg.dataset_id)

    logger.info("Fetching users from Zendesk...")
    url = f"{zendesk_cfg.base_url}/users.json?page[size]=100"
    records = list(client.fetch_paginated(url, result_key="users"))

    if not records:
        logger.info("No user data returned from Zendesk. Nothing to load.")
        return

    df = transform(records)
    rows_loaded = loader.load_and_merge(df, bq_cfg.users_table, SCHEMA, merge_key="id", order_by="updated_at")
    logger.info(f"Users pipeline complete. {rows_loaded} rows merged into {bq_cfg.users_table}.")


if __name__ == "__main__":
    run()
