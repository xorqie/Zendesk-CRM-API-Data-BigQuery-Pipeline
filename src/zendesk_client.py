"""
zendesk_client.py
--------------------
A thin, reusable client around the Zendesk REST API (v2).

Consolidates logic that was duplicated, with small inconsistent variants,
across all four original scripts:
- Cursor-based pagination via `links.next` (used by most list endpoints).
- Zendesk's incremental export cursor (`start_time` / `end_time` /
  `end_of_stream`), used specifically by the tickets endpoint to support
  reliable, resumable incremental syncs.
- Retry with exponential backoff for transient network/server errors
  (only `Zendesk_tickets.py` had this originally; the other three scripts
  had no retry logic at all and would crash on a single dropped
  connection).
- Bulk reference-data lookups (`users/show_many`, ticket forms, brands,
  groups) via one `fetch_reference_data` / `fetch_names_by_id` pair
  instead of three near-identical copies.

Zendesk API docs: https://developer.zendesk.com/api-reference/
"""

import random
import time
from typing import Any, Callable, Dict, Iterator, List, Optional

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3
BASE_DELAY = 5


class ZendeskClient:
    def __init__(self, auth: tuple):
        self.auth = auth

    def _retry_with_backoff(self, func: Callable[[], requests.Response]) -> requests.Response:
        attempt = 0
        while True:
            try:
                response = func()
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                attempt += 1
                logger.error(f"Request failed ({e}). Attempt {attempt}/{MAX_RETRIES}.")
                if attempt >= MAX_RETRIES:
                    raise
                wait_time = BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait_time)

    def fetch_paginated(self, url: str, result_key: str) -> Iterator[Dict[str, Any]]:
        """
        Follow Zendesk's cursor-based `links.next` pagination, yielding one
        record at a time so callers can stream/chunk without holding the
        whole result set in memory.
        """
        while url:
            response = self._retry_with_backoff(lambda: requests.get(url, auth=self.auth, timeout=DEFAULT_TIMEOUT))
            data = response.json()
            for record in data.get(result_key, []):
                yield record
            url = data.get("links", {}).get("next")

    def fetch_incremental_tickets(self, url: str, start_time: int) -> Iterator[List[Dict[str, Any]]]:
        """
        Follow Zendesk's incremental ticket export cursor. Yields batches of
        raw ticket dicts (one batch per API page) until `end_of_stream` is
        reached. This endpoint is specifically designed for reliable
        incremental syncs (it's safe to resume from the last `end_time`),
        unlike a plain `updated_at` filter on the regular tickets endpoint.
        """
        while True:
            params = {"start_time": start_time}

            def fetch_page():
                return requests.get(url, params=params, auth=self.auth, timeout=DEFAULT_TIMEOUT)

            response = self._retry_with_backoff(fetch_page)
            data = response.json()
            tickets = data.get("tickets", [])
            if tickets:
                yield tickets

            if data.get("end_of_stream", False):
                logger.info("Reached end of incremental ticket stream.")
                return

            next_start_time = data.get("end_time")
            if not next_start_time:
                logger.warning("No next start_time in response; stopping incremental fetch.")
                return
            start_time = next_start_time

    def fetch_names_by_id(self, show_many_url: str, ids: List[str], chunk_size: int = 100) -> Dict[str, str]:
        """Bulk-resolve IDs to display names via an endpoint like `/users/show_many`."""
        ids = [i for i in ids if i]
        if not ids:
            return {}

        name_map: Dict[str, str] = {}
        for i in range(0, len(ids), chunk_size):
            chunk = ids[i : i + chunk_size]
            response = self._retry_with_backoff(
                lambda: requests.get(show_many_url, params={"ids": ",".join(chunk)}, auth=self.auth, timeout=DEFAULT_TIMEOUT)
            )
            payload = response.json()
            # `users/show_many` returns {"users": [...]}
            for user in payload.get("users", []):
                name_map[str(user["id"])] = user.get("name", "")

        return name_map

    def fetch_reference_map(self, url: str, result_key: str) -> Dict[str, str]:
        """
        Fetch a small reference/lookup dataset (ticket forms, brands, groups,
        assignees) and reduce it to an {id: display_name} dict. Follows
        either `links.next` or `next_page` pagination, since Zendesk uses
        both styles across different endpoints.
        """
        ref_map: Dict[str, str] = {}
        while url:
            response = self._retry_with_backoff(lambda: requests.get(url, auth=self.auth, timeout=DEFAULT_TIMEOUT))
            data = response.json()
            for item in data.get(result_key, []):
                ref_map[str(item["id"])] = item.get("name") or item.get("raw_name") or str(item["id"])
            url = data.get("links", {}).get("next") or data.get("next_page")
        logger.info(f"Fetched {len(ref_map)} records for '{result_key}' reference map.")
        return ref_map
