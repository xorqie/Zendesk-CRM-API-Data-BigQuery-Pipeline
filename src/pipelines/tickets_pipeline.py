"""
tickets_pipeline.py
--------------------
Extracts tickets via Zendesk's incremental export endpoint and upserts
them into BigQuery, resolving group/assignee/ticket-form/brand IDs to
human-readable names.

Why the incremental export endpoint (not a plain `updated_at` filter)?
Zendesk explicitly designed `/incremental/tickets.json` for reliable
syncing: it returns a `start_time`/`end_time` cursor and an
`end_of_stream` flag, and guarantees no records are skipped even if a
ticket is updated mid-export. This was already the right choice in the
original script and is preserved here - it's a stronger pattern than the
naive date-window filtering used elsewhere in this codebase's sibling
projects (Freshdesk, Playvox).

Reference data (ticket forms, brands, groups, assignees) is fetched once
per run and used to enrich every ticket, rather than being looked up
per-row.
"""

from typing import Any, Dict, List

import numpy as np
import pandas as pd
from google.cloud import bigquery

from config import ZendeskConfig, BigQueryConfig, TICKETS_LOOKBACK_DAYS
from src.zendesk_client import ZendeskClient
from src.bigquery_loader import BigQueryLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)

SCHEMA = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("channel", "STRING"),
    bigquery.SchemaField("ticket_id", "STRING"),
    bigquery.SchemaField("subject", "STRING"),
    bigquery.SchemaField("rel", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("created_at", "STRING"),
    bigquery.SchemaField("updated_at", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("raw_subject", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("priority", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("recipient", "STRING"),
    bigquery.SchemaField("requester_id", "STRING"),
    bigquery.SchemaField("submitter_id", "STRING"),
    bigquery.SchemaField("assignee_id", "STRING"),
    bigquery.SchemaField("organization_id", "STRING"),
    bigquery.SchemaField("group_id", "STRING"),
    bigquery.SchemaField("collaborator_ids", "STRING"),
    bigquery.SchemaField("follower_ids", "STRING"),
    bigquery.SchemaField("problem_id", "STRING"),
    bigquery.SchemaField("has_incidents", "BOOLEAN"),
    bigquery.SchemaField("is_public", "BOOLEAN"),
    bigquery.SchemaField("due_at", "STRING"),
    bigquery.SchemaField("tags", "STRING"),
    bigquery.SchemaField("score", "STRING"),
    bigquery.SchemaField("comment", "STRING"),
    bigquery.SchemaField("reason", "STRING"),
    bigquery.SchemaField("ticket_form_id", "STRING"),
    bigquery.SchemaField("brand_id", "STRING"),
    bigquery.SchemaField("from_messaging_channel", "BOOLEAN"),
]


def _join_ids(value) -> Any:
    if value is None or (not isinstance(value, (list, tuple, np.ndarray)) and pd.isna(value)):
        return None
    if isinstance(value, (list, tuple, np.ndarray)):
        if len(value) == 0:
            return None
        try:
            return ",".join(str(int(i)) for i in value if pd.notnull(i))
        except (ValueError, TypeError):
            return None
    return None


def transform(
    records: List[Dict[str, Any]],
    zendesk_cfg: ZendeskConfig,
    ticket_forms: Dict[str, str],
    brands: Dict[str, str],
    groups: Dict[str, str],
    assignees: Dict[str, str],
) -> pd.DataFrame:
    df = pd.DataFrame(records)
    for col in [f.name for f in SCHEMA]:
        if col not in df.columns:
            df[col] = None

    df["id"] = df["id"].astype(str)
    df["ticket_id"] = df["id"].apply(zendesk_cfg.ticket_deep_link)
    df["channel"] = df["via"].apply(lambda x: x.get("channel") if isinstance(x, dict) else None)
    df["rel"] = df["via"].apply(lambda x: (x.get("source") or {}).get("rel") if isinstance(x, dict) else None)

    for col in ["url", "created_at", "updated_at", "type", "raw_subject", "description", "priority", "status", "recipient", "due_at"]:
        df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else None)

    for col in ["requester_id", "submitter_id", "organization_id"]:
        df[col] = df[col].apply(lambda x: str(int(x)) if pd.notnull(x) and str(x).replace(".0", "").isdigit() else None)

    def _lookup(id_value, mapping: Dict[str, str]) -> Any:
        """Resolve an ID to its name, normalizing float-formatted IDs (e.g. 2.0 -> '2')
        so they actually match the string keys in the reference maps."""
        if pd.isnull(id_value):
            return None
        key = str(int(id_value)) if str(id_value).replace(".0", "").isdigit() else str(id_value)
        return mapping.get(key, key)

    df["assignee_id"] = df["assignee_id"].apply(lambda x: _lookup(x, assignees))
    df["group_id"] = df["group_id"].apply(lambda x: _lookup(x, groups))
    df["ticket_form_id"] = df["ticket_form_id"].apply(lambda x: _lookup(x, ticket_forms))
    df["brand_id"] = df["brand_id"].apply(lambda x: _lookup(x, brands))

    df["collaborator_ids"] = df["collaborator_ids"].apply(_join_ids)
    df["follower_ids"] = df["follower_ids"].apply(_join_ids)

    df["problem_id"] = df["problem_id"].apply(lambda x: str(x) if pd.notnull(x) else None)
    df["has_incidents"] = df["has_incidents"].fillna(False).astype(bool)
    df["is_public"] = df["is_public"].fillna(False).astype(bool)
    df["tags"] = df["tags"].apply(lambda x: ",".join(x) if isinstance(x, list) and len(x) > 0 else None)

    df["score"] = df["satisfaction_rating"].apply(lambda x: x.get("score") if isinstance(x, dict) else None)
    df["comment"] = df["satisfaction_rating"].apply(lambda x: x.get("comment") if isinstance(x, dict) else None)
    df["reason"] = df["satisfaction_rating"].apply(lambda x: x.get("reason") if isinstance(x, dict) else None)
    df["from_messaging_channel"] = df["from_messaging_channel"].fillna(False).astype(bool)

    return df[[f.name for f in SCHEMA]]


def run() -> None:
    zendesk_cfg = ZendeskConfig()
    bq_cfg = BigQueryConfig()

    client = ZendeskClient(auth=zendesk_cfg.auth)
    loader = BigQueryLoader(bq_cfg.credentials_path, bq_cfg.project_id, bq_cfg.dataset_id)

    logger.info("Fetching reference data (ticket forms, brands, groups, assignees)...")
    ticket_forms = client.fetch_reference_map(f"{zendesk_cfg.base_url}/ticket_forms.json", "ticket_forms")
    brands = client.fetch_reference_map(f"{zendesk_cfg.base_url}/brands.json", "brands")
    groups = client.fetch_reference_map(f"{zendesk_cfg.base_url}/groups/assignable", "groups")
    assignees = client.fetch_reference_map(f"{zendesk_cfg.base_url}/capacity/rules/assignees?page[size]=100", "assignees")

    start_time = int(pd.Timestamp.utcnow().timestamp()) - TICKETS_LOOKBACK_DAYS * 86400
    logger.info(f"Fetching tickets updated in the last {TICKETS_LOOKBACK_DAYS} days from Zendesk...")

    staging_table_id = f"staging_{bq_cfg.tickets_table}"
    total_records = 0
    first_chunk = True

    for chunk in client.fetch_incremental_tickets(f"{zendesk_cfg.base_url}/incremental/tickets.json", start_time):
        df = transform(chunk, zendesk_cfg, ticket_forms, brands, groups, assignees)
        if df.empty:
            continue
        loader.stage_chunk(df, staging_table_id, SCHEMA, first_chunk=first_chunk)
        first_chunk = False
        total_records += len(df)
        logger.info(f"Staged {len(df)} tickets ({total_records} total so far)...")

    if total_records == 0:
        logger.info("No ticket data returned from Zendesk. Nothing to load.")
        return

    loader.merge_staging(staging_table_id, bq_cfg.tickets_table, SCHEMA, merge_key="id", order_by="updated_at")
    logger.info(f"Tickets pipeline complete. {total_records} rows merged into {bq_cfg.tickets_table}.")


if __name__ == "__main__":
    run()
