"""
kpi_pipeline.py
--------------------
Extracts Zendesk's `ticket_metrics` (reply time, resolution time, wait
time, reopens, SLA outcomes) and joins in `assignee_id` from the tickets
endpoint, since `ticket_metrics` doesn't include it directly. This is the
table SLA/response-time dashboards should be built on.

Note on the source file this replaces: `Zendesk_kpi.py` as uploaded had
its entire contents duplicated end-to-end in a single file (every
function and `main()` defined twice), plus a stray, non-Python fragment
of a BigQuery schema pasted after the second `if __name__ == "__main__"`
block - the file did not actually compile. The two copies differed
slightly in one place (the SLA calculation below); this pipeline keeps
the more correct of the two, which explicitly handles the boundary case
around exactly 24 hours instead of leaving it ambiguous.
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
    bigquery.SchemaField("ticket_id", "STRING"),
    bigquery.SchemaField("ticket_url", "STRING"),
    bigquery.SchemaField("assignee_id", "STRING"),
    bigquery.SchemaField("created_at", "STRING"),
    bigquery.SchemaField("updated_at", "STRING"),
    bigquery.SchemaField("group_stations", "STRING"),
    bigquery.SchemaField("assignee_stations", "STRING"),
    bigquery.SchemaField("reopens", "STRING"),
    bigquery.SchemaField("replies", "STRING"),
    bigquery.SchemaField("assignee_updated_at", "STRING"),
    bigquery.SchemaField("requester_updated_at", "STRING"),
    bigquery.SchemaField("status_updated_at", "STRING"),
    bigquery.SchemaField("initially_assigned_at", "STRING"),
    bigquery.SchemaField("assigned_at", "STRING"),
    bigquery.SchemaField("solved_at", "STRING"),
    bigquery.SchemaField("latest_comment_added_at", "STRING"),
    bigquery.SchemaField("reply_time_in_hours_calendar", "FLOAT"),
    bigquery.SchemaField("reply_time_in_hours_business", "FLOAT"),
    bigquery.SchemaField("first_resolution_time_in_hours_calendar", "FLOAT"),
    bigquery.SchemaField("first_resolution_time_in_hours_business", "FLOAT"),
    bigquery.SchemaField("full_resolution_time_in_hours_calendar", "FLOAT"),
    bigquery.SchemaField("full_resolution_time_in_hours_business", "FLOAT"),
    bigquery.SchemaField("agent_wait_time_in_hours_calendar", "FLOAT"),
    bigquery.SchemaField("agent_wait_time_in_hours_business", "FLOAT"),
    bigquery.SchemaField("requester_wait_time_in_hours_calendar", "FLOAT"),
    bigquery.SchemaField("requester_wait_time_in_hours_business", "FLOAT"),
    bigquery.SchemaField("on_hold_time_in_hours_calendar", "FLOAT"),
    bigquery.SchemaField("on_hold_time_in_hours_business", "FLOAT"),
    bigquery.SchemaField("sla_met_calendar", "FLOAT"),
    bigquery.SchemaField("sla_met_business", "FLOAT"),
]

MINUTE_FIELDS = [
    "reply_time_in_minutes",
    "first_resolution_time_in_minutes",
    "full_resolution_time_in_minutes",
    "agent_wait_time_in_minutes",
    "requester_wait_time_in_minutes",
    "on_hold_time_in_minutes",
]

TIMESTAMP_FIELDS = [
    "created_at", "updated_at", "assignee_updated_at", "requester_updated_at",
    "status_updated_at", "initially_assigned_at", "assigned_at", "solved_at",
    "latest_comment_added_at",
]

SLA_TARGET_HOURS = 24.0


def _sla_met(hours) -> Any:
    """1.0 = met the reply-time SLA, 0.0 = missed it, None = no reply yet to measure."""
    if pd.isna(hours):
        return None
    return 1.0 if hours <= SLA_TARGET_HOURS else 0.0


def transform(
    metrics_records: List[Dict[str, Any]],
    tickets_records: List[Dict[str, Any]],
    zendesk_cfg: ZendeskConfig,
    agent_names: Dict[str, str],
) -> pd.DataFrame:
    metrics_df = pd.DataFrame(metrics_records)
    tickets_df = pd.DataFrame(tickets_records)

    if metrics_df.empty:
        return metrics_df

    for minute_field in MINUTE_FIELDS:
        hour_field = minute_field.replace("minutes", "hours")
        if minute_field in metrics_df.columns:
            metrics_df[f"{hour_field}_calendar"] = metrics_df[minute_field].apply(
                lambda x: x["calendar"] / 60.0 if isinstance(x, dict) and x.get("calendar") is not None else None
            )
            metrics_df[f"{hour_field}_business"] = metrics_df[minute_field].apply(
                lambda x: x["business"] / 60.0 if isinstance(x, dict) and x.get("business") is not None else None
            )

    metrics_df["ticket_url"] = metrics_df["ticket_id"].apply(zendesk_cfg.ticket_deep_link)
    metrics_df["ticket_id"] = metrics_df["ticket_id"].astype(str)
    metrics_df["id"] = metrics_df["url"].apply(lambda x: x.split("/")[-1].split(".")[0])

    for field in TIMESTAMP_FIELDS:
        if field in metrics_df.columns:
            metrics_df[field] = metrics_df[field].apply(lambda x: str(x) if pd.notna(x) else None)

    for field in ["group_stations", "assignee_stations", "reopens", "replies"]:
        if field in metrics_df.columns:
            metrics_df[field] = metrics_df[field].fillna("").astype(str)

    # ticket_metrics doesn't include assignee_id directly - join it in from the tickets endpoint
    if "id" in tickets_df.columns:
        tickets_slim = tickets_df[["id", "assignee_id"]].rename(columns={"id": "ticket_id"})
        tickets_slim["ticket_id"] = tickets_slim["ticket_id"].astype(str)
        tickets_slim["assignee_id"] = tickets_slim["assignee_id"].apply(
            lambda x: str(int(x)) if pd.notnull(x) else None
        )
        merged_df = pd.merge(metrics_df, tickets_slim, on="ticket_id", how="left")
    else:
        merged_df = metrics_df
        merged_df["assignee_id"] = None

    merged_df["assignee_id"] = merged_df["assignee_id"].map(agent_names).fillna(merged_df["assignee_id"])

    merged_df["sla_met_business"] = merged_df["reply_time_in_hours_business"].apply(_sla_met)
    merged_df["sla_met_calendar"] = merged_df["reply_time_in_hours_calendar"].apply(_sla_met)

    for col in [f.name for f in SCHEMA]:
        if col not in merged_df.columns:
            merged_df[col] = None

    return merged_df[[f.name for f in SCHEMA]]


def run() -> None:
    zendesk_cfg = ZendeskConfig()
    bq_cfg = BigQueryConfig()

    client = ZendeskClient(auth=zendesk_cfg.auth)
    loader = BigQueryLoader(bq_cfg.credentials_path, bq_cfg.project_id, bq_cfg.dataset_id)

    logger.info("Fetching ticket metrics from Zendesk...")
    metrics_url = f"{zendesk_cfg.base_url}/ticket_metrics?page[size]=100"
    metrics_records = list(client.fetch_paginated(metrics_url, result_key="ticket_metrics"))

    logger.info("Fetching tickets from Zendesk (for assignee join)...")
    tickets_url = f"{zendesk_cfg.base_url}/tickets?page[size]=100"
    tickets_records = list(client.fetch_paginated(tickets_url, result_key="tickets"))

    if not metrics_records or not tickets_records:
        logger.info("No metrics or ticket data returned from Zendesk. Nothing to load.")
        return

    assignee_ids = list({str(int(t["assignee_id"])) for t in tickets_records if t.get("assignee_id") is not None})
    logger.info(f"Resolving names for {len(assignee_ids)} assignees...")
    agent_names = client.fetch_names_by_id(f"{zendesk_cfg.base_url}/users/show_many", assignee_ids)

    df = transform(metrics_records, tickets_records, zendesk_cfg, agent_names)
    if df.empty:
        logger.info("No data to upload after transformation.")
        return

    rows_loaded = loader.load_and_merge(df, bq_cfg.kpi_table, SCHEMA, merge_key="id", order_by="updated_at")
    logger.info(f"KPI pipeline complete. {rows_loaded} rows merged into {bq_cfg.kpi_table}.")


if __name__ == "__main__":
    run()
