# Sports Analytics Pipeline

NFL and college football data flowing through Databricks, modeled with dbt, and surfaced in Superset. Recap text runs through an LLM extraction step that pulls out fields the box score doesn't have: injury status, key events, momentum.

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
| Recaps (both sports) | ESPN game summary endpoints |

## What this covers

| Role | Where |
|---|---|
| Data Engineering | Multi-source ingestion, Airflow orchestration, LLM-step reliability (retries, caching, validation) |
| Analytics Engineering | dbt medallion modeling, testing, incremental models, SCD Type 2 |
| Data Analyst | Superset dashboards built on raw stats and AI-extracted fields |

## Status

Build in progress (v1). Bronze schedules and recaps are loaded for NFL (1999-2025) and NCAAF (2000-2024). Silver staging models are built and tested. Team and player stats ingestion and the gold star schema are next.

## Setup

TBD, filled in once the full pipeline is scaffolded.
