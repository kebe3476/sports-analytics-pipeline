# Architecture

**Status:** Design finalized, ready for build
**Last updated:** 2026-07-16 (warehouse switched to Databricks, see D020)

---

## 1. Objective

A sports analytics pipeline covering ingestion, transformation, orchestration, an AI extraction step, and BI, end to end, for structured stats and unstructured recap text across five sports. The AI component is scoped to solve an actual problem in the pipeline: unstructured text has no structured equivalent, and the recap text contains information (injury context, coaching decisions, momentum) the stats alone don't capture.

---

## 2. Domain & Data Sources

**Domain:** Sports data - chosen specifically because it produces two genuinely different data types that need different treatment:

- **Structured:** box scores, player stats, team/roster data - API-native, ideal for a conventional medallion build.
- **Unstructured:** game recaps and commentary - not queryable in SQL, and contains information (injury context, coaching decisions, momentum) that the structured stats don't capture.

**Sport coverage:** NBA, NFL, NHL, MLB, and NCAAF (college football).

**Confirmed sources:**

- **Structured stats - open-source, per-sport, zero recurring cost:**
  - NFL: [nflverse](https://github.com/nflverse) (CC-BY 4.0, flat files on GitHub releases)
  - NCAAF: [CollegeFootballData](https://collegefootballdata.com/) (documented REST API, free tier)
  - NHL: [api-web.nhle.com](https://github.com/Zmalski/NHL-API-Reference) (NHL's own public API)
  - NBA: [nba_api](https://github.com/swar/nba_api) (MIT, wraps stats.nba.com)
  - MLB: [MLB Stats API](https://statsapi.mlb.com/) (official source powering MLB.com/Statcast)
- **Recap/commentary text: ESPN's unofficial `site.api.espn.com` news endpoints** - undocumented, no formal ToS grant, accepted as a real risk given it's genuine journalist-written content (independent of the box score) rather than AI-generated stats summaries. Covers all five sports under the same API pattern. Keep polling light, cache aggressively, and build resilient error handling since the endpoint could change without notice.

**Build sequencing:** NFL → NCAAF → NHL → NBA → MLB. Prove the pipeline end-to-end on NFL first, then replicate the ingestion pattern sport by sport in that order.

---

## 3. Architecture Overview

```
                    ┌─────────────┐
   Sports API  ───► │   Bronze    │  raw structured data (games, players, teams)
   Recap source ───►│ (Databricks)│  raw unstructured text (recaps)
                    └──────┬──────┘
                           │  dbt
                           ▼
                    ┌─────────────┐
                    │   Silver    │  cleaned, typed, tested, conformed
                    └──────┬──────┘
                           │  LLM extraction step (structured fields
                           │  pulled from recap text: injury flags,
                           │  momentum tags, key events)
                           ▼
                    ┌─────────────┐
                    │    Gold     │  star schema marts (fact_games,
                    │             │  dim_teams, dim_players, dim_date)
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │  Superset   │  dashboards, using AI-extracted
                    │  (BI layer) │  fields alongside raw stats
                    └─────────────┘

   Orchestrated end-to-end by Airflow: ingest → dbt run → LLM
   extraction → BI refresh.
```

*(See diagrams.md for the Whimsical and Mermaid versions of this diagram.)*

---

## 4. Data Model

### 4.1 Star schema (core)

The real fact table - the one with actual additive measures - is `fact_games`, at one-row-per-game grain: score, team stats, player stats. Standard dimensions: `dim_teams`, `dim_players`, `dim_date`. This is untouched by the AI/RAG extension below; nothing about adding unstructured-data support changes its grain.

### 4.2 AI/RAG extension (bridge structure - scoped for v2, reserved now)

RAG Q&A over recaps is explicitly a later iteration, not v1. But the schema for it is designed in now, so v1 doesn't have to be restructured to support it later.

**Why not just add a vector column to `fact_games`:** effective RAG retrieval requires chunking recap text into paragraphs rather than embedding a whole article as one vector, which means multiple embeddings per game. Storing that in the existing fact table would either denormalize multiple vectors into one row (violates 1NF - repeating group in a cell) or force a grain change on a table everything else depends on. This holds regardless of warehouse platform - it's a normalization problem, not a BigQuery-specific one.

**Design instead - a bridge table pattern** (Kimball's standard technique for attaching a multi-valued, non-additive attribute to a fact without changing its grain):

recaps
recap_id (PK) | game_id (FK → fact_games) | source | published_at

recap_chunks
chunk_id (PK) | recap_id (FK → recaps) | chunk_index | chunk_text | embedding ARRAY<FLOAT64>

- `recaps` bridges `fact_games` (one game → many recaps, e.g. beat writer + wire service) without touching the fact table's grain.
- `recap_chunks` is a child/detail table one level below the bridge, at chunk grain. It has no additive measures, so in strict terms it's a **factless fact table** - it records that a chunk exists in association with a recap, nothing more.
- This is a two-level bridge-then-child chain, *not* a hierarchy bridge (a different Kimball pattern reserved for recursive, ragged self-referencing relationships like org charts or bills of materials - doesn't apply here since nothing is self-referencing or variable-depth).

**Referential integrity caveat:** Databricks (Unity Catalog) allows declaring PK/FK constraints but does not enforce them - they're informational only. Integrity between `recap_chunks → recaps → fact_games` has to be enforced with dbt `relationships` tests, not the database.

**v1 vs. v2 scope:**
- **v1:** land raw recap text at silver as a plain string column, one row per recap. No embeddings yet.
- **v2 (RAG iteration):** add a pipeline step - chunk, embed, load - that populates `recaps` and `recap_chunks`, then build a vector index on `recap_chunks.embedding`. Databricks Free Edition includes one native AI Search (vector search) endpoint, worth evaluating against a manually-managed index when this phase starts. No changes required to `fact_games` or any existing gold model.

---

## 5. AI Component - Rationale & Design

The AI's job is not a chatbot bolted on top of dashboards. It's an LLM-based extraction step that runs between silver and gold: recap text contains information the structured stats don't (why a player was pulled, coaching decisions, momentum shifts), and manually parsing that doesn't scale. The extraction step pulls structured fields out of the text - injury flags, sentiment/momentum tags, key events - and lands them as new columns that dbt models can test and Superset can chart directly.

This comes with real engineering problems to solve:

- **Non-determinism:** LLM output isn't guaranteed consistent between runs; needs validation before it's trusted into gold.
- **Cost:** re-running extraction on unchanged recaps burns money for no benefit; needs caching keyed on recap ID so only new/changed text gets processed.
- **Reliability:** the pipeline needs to handle LLM call failures/retries like any other unreliable upstream dependency, not treat the LLM as a black box that always succeeds.

The optional RAG Q&A layer (v2) sits on top of this, once the bridge tables above are populated - natural-language questions answered against the vectorized recap corpus.

---

## 6. Orchestration

**Airflow**, scheduling: ingest (structured + recap) → dbt run (bronze → silver → gold) → LLM extraction step → Superset refresh.

Ingestion runs in Airflow's own environment, not as native Databricks notebooks or jobs: Databricks Free Edition's serverless compute restricts outbound access to a fixed allowlist that doesn't cover the sports APIs. Airflow fetches from the sources and loads into Databricks over its SQL connection.

---

## 7. BI Layer

**Superset**. Dashboards should visibly use the AI-extracted fields (e.g., injury impact on team performance), so the AI layer's output is seen paying off downstream, not just sitting in a table unused.

---

## 8. Tech Stack Summary

| Layer | Choice |
|---|---|
| Warehouse | Databricks Free Edition (no-cost, no billing account required) |
| Transformation | dbt Core |
| Orchestration | Airflow |
| BI | Superset |
| AI / extraction | LLM-based structured extraction (silver → gold step) |
| AI / retrieval (v2) | Vector search via Databricks AI Search endpoint or a managed index on `recap_chunks.embedding` |

---

## 9. Role Coverage Map

- **Data Engineering:** API ingestion, schema drift handling, idempotent loads, Airflow orchestration, LLM-step reliability engineering (retries, caching, validation).
- **Analytics Engineering:** dbt medallion modeling (bronze/silver/gold), testing, documentation, incremental models, SCD Type 2 on team roster/player-team assignment changes.
- **Data Analyst:** Superset dashboards, insight generation from both raw stats and AI-extracted fields (e.g., injury impact analysis).

---

## 10. Phased Roadmap

**v1 (core build):**
1. Confirm data source(s) for structured stats + recap text.
2. Ingestion → bronze in Databricks.
3. dbt medallion: silver (cleaned/tested) → gold (star schema, SCD Type 2 on roster).
4. LLM extraction step: recap text → structured fields into gold.
5. Airflow DAG tying it together.
6. Superset dashboards on gold, incorporating AI-extracted fields.

**v2 (iteration, not blocking v1 launch):**
7. Populate `recaps` / `recap_chunks` bridge tables (chunk, embed, load).
8. Build `CREATE VECTOR INDEX` on `recap_chunks.embedding`.
9. RAG Q&A layer over the vectorized recap corpus.

---

## 11. Open Decisions & Risks

- **Data source confirmation:** resolved, see section 2. Residual risk: ESPN's unofficial endpoint could change or get rate-limited without notice; StoryStats is the documented fallback if it does.
- **Warehouse:** resolved, see D020. Residual risk: Databricks Free Edition accounts may be deleted after prolonged inactivity (not a fixed window like BigQuery Sandbox's 60 days, but undocumented exactly how long). Revisit if the project goes dormant for an extended stretch.
