"""
bigquery_loader.py
--------------------
Reusable BigQuery load helpers shared by every pipeline.

One thing the original scripts got right that's worth keeping: each
defined an explicit `bigquery.SchemaField` list instead of relying on
pandas/BigQuery type inference. That avoids autodetect surprises (e.g. a
column that's all-null in one run getting inferred as a different type
than a run where it has data) and is genuinely good practice - this
loader keeps that pattern and makes it a first-class parameter instead of
being re-declared inline in every script.

All four pipelines follow the same shape: stage into a temp table, then
`MERGE` into the target on `id`. Two of the four (tickets, and originally
csat/kpi with multi-thousand-row exports) need to stage across *multiple*
chunks before merging once at the end, so staging and merging are split
into two explicit steps rather than one combined call.
"""

from typing import List

import pandas as pd
from google.cloud import bigquery

from src.utils.logger import get_logger

logger = get_logger(__name__)


class BigQueryLoader:
    def __init__(self, credentials_path: str, project_id: str, dataset_id: str):
        self.client = bigquery.Client.from_service_account_json(credentials_path)
        self.project_id = project_id
        self.dataset_id = dataset_id

    def _table_ref(self, table_id: str) -> str:
        return f"{self.project_id}.{self.dataset_id}.{table_id}"

    def stage_chunk(
        self,
        df: pd.DataFrame,
        staging_table_id: str,
        schema: List[bigquery.SchemaField],
        first_chunk: bool,
    ) -> None:
        """
        Load one chunk of data into the staging table. `first_chunk=True`
        truncates the staging table first (start of a fresh sync);
        subsequent chunks append, so a multi-page fetch accumulates into a
        single staging table before one final MERGE.
        """
        if df.empty:
            return

        write_disposition = "WRITE_TRUNCATE" if first_chunk else "WRITE_APPEND"
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition=write_disposition)
        job = self.client.load_table_from_dataframe(df, self._table_ref(staging_table_id), job_config=job_config)
        job.result()
        logger.info(f"Staged {len(df)} rows into {self._table_ref(staging_table_id)} ({write_disposition}).")

    def merge_staging(
        self,
        staging_table_id: str,
        target_table_id: str,
        schema: List[bigquery.SchemaField],
        merge_key: str = "id",
        order_by: str = "created_at",
    ) -> None:
        """
        Deduplicate the staging table (keeping the most recent row per
        `merge_key`, by `order_by` descending) and MERGE it into the target
        table. Always cleans up the staging table afterward, even on failure.
        """
        staging_ref = self._table_ref(staging_table_id)
        target_ref = self._table_ref(target_table_id)
        columns = [f.name for f in schema]

        try:
            update_clause = ", ".join(f"T.{c} = S.{c}" for c in columns if c != merge_key)
            insert_columns = ", ".join(columns)
            insert_values = ", ".join(f"S.{c}" for c in columns)

            merge_query = f"""
                MERGE `{target_ref}` T
                USING (
                    SELECT * EXCEPT(rn) FROM (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY {merge_key} ORDER BY {order_by} DESC
                        ) AS rn
                        FROM `{staging_ref}`
                    )
                    WHERE rn = 1
                ) S
                ON T.{merge_key} = S.{merge_key}
                WHEN MATCHED THEN
                    UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
            """
            self.client.query(merge_query).result()
            logger.info(f"Merged {staging_ref} into {target_ref}.")
        finally:
            self.client.delete_table(staging_ref, not_found_ok=True)
            logger.info(f"Cleaned up staging table {staging_ref}.")

    def load_and_merge(
        self,
        df: pd.DataFrame,
        target_table_id: str,
        schema: List[bigquery.SchemaField],
        merge_key: str = "id",
        order_by: str = "created_at",
    ) -> int:
        """Convenience wrapper for single-shot (non-chunked) pipelines: stage once, merge once."""
        if df.empty:
            logger.info("No data to upload; skipping load.")
            return 0

        staging_table_id = f"staging_{target_table_id}"
        self.stage_chunk(df, staging_table_id, schema, first_chunk=True)
        self.merge_staging(staging_table_id, target_table_id, schema, merge_key, order_by)
        return len(df)
