# Sports Analytics Pipeline

A portfolio project demonstrating range across Data Analyst, Analytics Engineering, and Data Engineering — built as a single pipeline rather than three disconnected exercises, with an AI component that solves an actual problem in the data rather than being added for its own sake.

## What this is

Structured sports data (box scores, player stats, team/roster data) and unstructured data (game recaps, commentary) flow through the same warehouse, get modeled through a dbt medallion architecture, and land in dashboards — with an LLM-based extraction step in the middle that turns recap text into structured fields the stats alone don't capture (injury context, momentum, key events).

Full technical design: see [architecture.md](./architecture.md).
Why specific tradeoffs were made (BigQuery vs. Snowflake, Airflow vs. Dagster, etc.): see [decisions.md](./decisions.md).

## Tech stack

| Layer | Choice |
|---|---|
| Warehouse | BigQuery |
| Transformation | dbt Core |
| Orchestration | Airflow |
| BI | Superset |
| AI | LLM-based structured extraction (silver → gold); vector search (v2) |

## Role coverage

- **Data Engineering** — API ingestion, schema drift handling, idempotent loads, Airflow orchestration, LLM-step reliability (retries, caching, validation).
- **Analytics Engineering** — dbt medallion modeling (bronze/silver/gold), testing, documentation, incremental models, SCD Type 2 on roster changes.
- **Data Analyst** — Superset dashboards built on both raw stats and AI-extracted fields.

## Status

Design complete. Build in progress.

## Setup

TBD — filled in once ingestion is scaffolded.
