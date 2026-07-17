"""
csat_pipeline.py
--------------------
Extracts Customer Satisfaction (CSAT) ratings and upserts them into
BigQuery, resolving the assignee ID to a human-readable agent name.

`score_percentage` converts Zendesk's categorical rating ('good'/'bad'/
'offered') into a numeric 1.0/0.0/NULL field, which is what actually makes
this table useful for a "CSAT % over time" chart without re-deriving the
mapping in every downstream query.
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
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("assignee_id", "STRING"),
    bigquery.SchemaField("group_id", "STRING"),
    bigquery.SchemaField("requester_id", "STRING"),
    bigquery.SchemaField("ticket_id", "STRING"),
    bigquery.SchemaField("score", "STRING"),
    bigquery.SchemaField("score_percentage", "FLOAT64"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("comment", "STRING"),
    bigquery.SchemaField("reason", "STRING"),
    bigquery.SchemaField("reason_id", "STRING"),
]

SCORE_TO_PERCENTAGE = {"offered": None, "good": 1.0, "bad": 0.0}
PASSTHROUGH_STRING_COLUMNS = ["url", "id", "group_id", "requester_id", "score", "comment", "reason", "reason_id"]


def transform(records: List[Dict[str, Any]], zendesk_cfg: ZendeskConfig, agent_names: Dict[str, str]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    for col in [f.name for f in SCHEMA]:
        if col not in df.columns:
            df[col] = None

    df["ticket_id"] = df["ticket_id"].apply(lambda x: zendesk_cfg.ticket_deep_link(x) if pd.notnull(x) else None)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    df["assignee_id"] = df["assignee_id"].apply(lambda x: str(int(x)) if pd.notnull(x) else None)
    df["assignee_id"] = df["assignee_id"].map(agent_names).fillna(df["assignee_id"])

    df["score_percentage"] = df["score"].map(SCORE_TO_PERCENTAGE).astype(float)

    for col in PASSTHROUGH_STRING_COLUMNS:
        df[col] = df[col].astype(str).replace({"None": None, "nan": None})

    # De-duplicate defensively: keep the most recent record per rating ID
    df = df.sort_values("created_at", ascending=False).drop_duplicates("id", keep="first")

    return df[[f.name for f in SCHEMA]]


def run() -> None:
    zendesk_cfg = ZendeskConfig()
    bq_cfg = BigQueryConfig()

    client = ZendeskClient(auth=zendesk_cfg.auth)
    loader = BigQueryLoader(bq_cfg.credentials_path, bq_cfg.project_id, bq_cfg.dataset_id)

    logger.info("Fetching CSAT ratings from Zendesk...")
    url = f"{zendesk_cfg.base_url}/satisfaction_ratings.json?page[size]=100"
    records = list(client.fetch_paginated(url, result_key="satisfaction_ratings"))

    if not records:
        logger.info("No CSAT data returned from Zendesk. Nothing to load.")
        return

    assignee_ids = list({str(int(r["assignee_id"])) for r in records if r.get("assignee_id") is not None})
    logger.info(f"Resolving names for {len(assignee_ids)} assignees...")
    agent_names = client.fetch_names_by_id(f"{zendesk_cfg.base_url}/users/show_many", assignee_ids)

    df = transform(records, zendesk_cfg, agent_names)
    rows_loaded = loader.load_and_merge(df, bq_cfg.csat_table, SCHEMA, merge_key="id", order_by="created_at")
    logger.info(f"CSAT pipeline complete. {rows_loaded} rows merged into {bq_cfg.csat_table}.")


if __name__ == "__main__":
    run()
