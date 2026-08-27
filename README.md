# Sports Analytics Pipeline

NFL, NCAAF, NHL, NBA, and MLB data flowing through Databricks, modeled with dbt, and surfaced in Superset. Recap text runs through an LLM extraction step that pulls out fields the box score doesn't have: injury status, key events, momentum.

Full design: [architecture.md](./architecture.md).

## Stack

| Layer | Tool |
|---|---|
| Warehouse | Databricks |
| Transformation | dbt Core |
| Orchestration | Airflow |
| BI | Superset |
| AI | LLM extraction (recap text to structured fields), vector search planned for v2 |

## Data sources

All free, no paid vendor APIs.

| Sport | Source |
|---|---|
| NFL | nflverse |
| NCAAF | CollegeFootballData |
| NHL | NHL public API |
| NBA | nba_api |
| MLB | MLB Stats API |
| Recaps (all sports) | ESPN news endpoints |

## What this covers

| Role | Where |
|---|---|
| Data Engineering | Five-source ingestion, Airflow orchestration, LLM-step reliability (retries, caching, validation) |
| Analytics Engineering | dbt medallion modeling, testing, incremental models, SCD Type 2 |
| Data Analyst | Superset dashboards built on raw stats and AI-extracted fields |

## Status

Build in progress (v1). Bronze ingestion is complete for all five sports (NFL, NCAAF, NHL, NBA, MLB). dbt modeling (silver/gold) and the LLM extraction step are next.

## Setup

TBD, filled in once the full pipeline is scaffolded.
