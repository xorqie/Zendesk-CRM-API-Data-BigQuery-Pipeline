# BigQuery Schema

All tables live in a single dataset (default name: `zendesk`). Every
pipeline defines an explicit `bigquery.SchemaField` list rather than
relying on autodetected types, so column types are guaranteed stable
across runs regardless of which fields happen to be null in a given
batch.

## Load strategy

Every table uses the same pattern: stage into a temporary table, then
`MERGE` into the target on `id` (deduplicating by keeping the most recent
row per `id`, ordered by `updated_at` or `created_at`). `tickets` stages
across multiple chunks (large incremental exports) before a single merge;
`users`, `csat`, and `ticket_metrics` stage once and merge once.

## `users`

| Column | Description |
|---|---|
| `id` | Zendesk user ID (merge key) |
| `name` / `email` | User identity |
| `role` | `end-user`, `agent`, or `admin` |
| `organization_id` | Linked organization |
| `active` / `suspended` | Account status |
| `custom_role_id` / `default_group_id` | Agent-specific configuration |
| `created_at` / `updated_at` | Lifecycle timestamps |

## `tickets`

| Column | Description |
|---|---|
| `id` | Zendesk ticket ID (merge key) |
| `ticket_id` | Deep link to the ticket in the Zendesk agent UI (shared format with `csat.ticket_id`, so the two tables join cleanly) |
| `subject` / `description` | Ticket content |
| `status` / `priority` / `type` | Ticket classification |
| `channel` / `rel` | How the ticket came in (derived from the `via` object) |
| `requester_id` / `submitter_id` / `assignee_id` | People involved (assignee resolved to a name) |
| `group_id` | Resolved to a group name |
| `ticket_form_id` / `brand_id` | Resolved to names via reference-data lookups |
| `collaborator_ids` / `follower_ids` | Comma-separated ID lists |
| `tags` | Comma-separated tags |
| `score` / `comment` / `reason` | Embedded satisfaction rating, if any |
| `has_incidents` / `is_public` / `from_messaging_channel` | Flags |
| `created_at` / `updated_at` / `due_at` | Lifecycle timestamps |

Synced via Zendesk's incremental export endpoint
(`/api/v2/incremental/tickets.json`), which guarantees no ticket is
skipped even if it's updated mid-export - a stronger guarantee than a
plain `updated_at >` filter on the regular tickets endpoint.

## `csat`

| Column | Description |
|---|---|
| `id` | Satisfaction rating ID (merge key) |
| `ticket_id` | Deep link to the related ticket (joins to `tickets.ticket_id`) |
| `assignee_id` | Resolved agent name |
| `score` | Raw Zendesk rating (`good` / `bad` / `offered`) |
| `score_percentage` | `1.0` / `0.0` / `NULL`, derived from `score` - use this column directly for a CSAT % calculation rather than re-deriving it from `score` in every query |
| `comment` / `reason` | Customer's written feedback, if any |
| `created_at` / `updated_at` | Rating timestamps |

## `ticket_metrics`

The SLA/response-time fact table.

| Column | Description |
|---|---|
| `id` | Ticket metrics ID (merge key) |
| `ticket_id` / `ticket_url` | Joins to `tickets.id` |
| `assignee_id` | Resolved agent name (joined in from the tickets endpoint - `ticket_metrics` doesn't include it directly) |
| `reply_time_in_hours_*` | First reply time, calendar vs. business hours |
| `first_resolution_time_in_hours_*` / `full_resolution_time_in_hours_*` | Resolution time |
| `agent_wait_time_in_hours_*` / `requester_wait_time_in_hours_*` | Wait time by party |
| `on_hold_time_in_hours_*` | Time spent on hold |
| `reopens` / `replies` | Counts |
| `sla_met_calendar` / `sla_met_business` | `1.0` if the reply-time SLA (24h) was met, `0.0` if missed, `NULL` if no reply yet to measure |

> The SLA threshold (24 hours) is defined once, as `SLA_TARGET_HOURS` in
> `kpi_pipeline.py`, rather than repeated as a magic number - change it
> there if your team's actual reply-time SLA differs.

## Entity relationships

```
tickets.requester_id / submitter_id  --> users.id
tickets.assignee_id                   --> users.id (resolved to name at transform time)
csat.ticket_id                        --> tickets.ticket_id
csat.assignee_id                      --> users.id (resolved to name at transform time)
ticket_metrics.ticket_id              --> tickets.id
ticket_metrics.assignee_id            --> users.id (resolved to name at transform time)
```

> Note: `assignee_id` is resolved to a display **name** at transform time
> across `tickets`, `csat`, and `ticket_metrics`, for readability in BI
> tools without an extra join. If you need a strict foreign key back to
> `users.id`, extend the relevant pipeline to keep the raw ID alongside
> the resolved name.
