from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from databricks import sql as databricks_sql

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator, get_current_context

CATALOG = "workspace"
SCHEMA = "bronze"
DEFAULT_SEASON = 2024
CFBD_BASE = "https://api.collegefootballdata.com"


def _db_conn():
    return databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def _cfbd_headers() -> dict:
    return {"Authorization": f"Bearer {os.environ['CFBD_API_KEY']}"}


def _coerce_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(
            lambda x: None if (x is None or (isinstance(x, float) and pd.isna(x))) else str(x)
        )
    return df


def _col_defs(df: pd.DataFrame) -> str:
    type_map = {
        "int64": "BIGINT",
        "float64": "DOUBLE",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
    }
    return ", ".join(
        f"`{c}` {type_map.get(str(df[c].dtype), 'STRING')}" for c in df.columns
    )


def _insert_rows(cursor, full_table: str, df: pd.DataFrame, batch_size: int) -> None:
    rows = [
        tuple(None if (v is None or (isinstance(v, float) and pd.isna(v))) else v for v in row)
        for row in df.itertuples(index=False)
    ]
    row_template = "(" + ", ".join(["?"] * len(df.columns)) + ")"
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        placeholders = ", ".join([row_template] * len(batch))
        params = [v for row in batch for v in row]
        cursor.execute(f"INSERT INTO {full_table} VALUES {placeholders}", params)


def _write_season(cursor, df: pd.DataFrame, table: str, season: int, batch_size: int = 100) -> None:
    main = f"`{CATALOG}`.`{SCHEMA}`.`{table}`"
    staging = f"`{CATALOG}`.`{SCHEMA}`.`{table}_staging_{season}`"
    col_defs = _col_defs(df)
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {main} ({col_defs}) USING DELTA")
    cursor.execute(f"CREATE OR REPLACE TABLE {staging} ({col_defs}) USING DELTA")
    _insert_rows(cursor, staging, df, batch_size)
    cursor.execute(
        f"INSERT INTO {main} REPLACE WHERE season = {season} SELECT * FROM {staging}"
    )
    cursor.execute(f"DROP TABLE IF EXISTS {staging}")


def _run_schedules(season: int) -> None:
    headers = _cfbd_headers()
    games = []
    for season_type in ("regular", "postseason"):
        resp = requests.get(
            f"{CFBD_BASE}/games",
            headers=headers,
            params={"year": season, "seasonType": season_type},
            timeout=30,
        )
        resp.raise_for_status()
        games.extend(resp.json())
    if not games:
        print(f"No CFBD games returned for season {season}.")
        return
    df = pd.DataFrame(games)
    # rename to avoid SQL reserved word collision
    df = df.rename(columns={"id": "cfbd_game_id"})
    df = _coerce_df(df)
    df["_ingested_at"] = datetime.utcnow()
    with _db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
            _write_season(cursor, df, "ncaaf_games_raw", season)
    print(f"Loaded {len(df)} NCAAF games for season {season}")


def _run_recaps(season: int) -> None:
    with _db_conn() as conn:
        with conn.cursor() as cursor:
            result = cursor.execute(
                f"SELECT cfbd_game_id FROM `{CATALOG}`.`{SCHEMA}`.`ncaaf_games_raw` "
                f"WHERE season = {season} AND completed = true"
            )
            game_rows = result.fetchall()
    records = []
    for (cfbd_game_id,) in game_rows:
        # CFBD game IDs are ESPN event IDs for college football
        url = (
            "https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary"
            f"?event={cfbd_game_id}"
        )
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            records.append({
                "cfbd_game_id": cfbd_game_id,
                "espn_game_id": str(cfbd_game_id),
                "season": season,
                "raw_json": resp.text,
                "_ingested_at": datetime.utcnow(),
            })
        except requests.HTTPError:
            if resp.status_code == 404:
                print(f"No ESPN data for game {cfbd_game_id}: 404")
            elif resp.status_code == 429:
                raise
            else:
                print(f"WARNING: ESPN {resp.status_code} for game {cfbd_game_id}, skipping")
        except Exception as exc:
            print(f"Skipped ESPN game {cfbd_game_id}: {exc}")
        time.sleep(0.5)
    if not records:
        print("No ESPN recap records to load.")
        return
    df = pd.DataFrame(records)
    with _db_conn() as conn:
        with conn.cursor() as cursor:
            _write_season(cursor, df, "ncaaf_recaps_raw", season, batch_size=1)
    print(f"Loaded {len(records)} NCAAF recaps for season {season}")


# ── Weekly DAG ───────────────────────────────────────────────────────────────

def ingest_ncaaf_schedules(**context):
    season = context["params"].get("season", DEFAULT_SEASON)
    _run_schedules(season)


def ingest_espn_recaps(**context):
    season = context["params"].get("season", DEFAULT_SEASON)
    _run_recaps(season)


with DAG(
    dag_id="ncaaf_bronze_ingest",
    start_date=datetime(2024, 9, 1),
    schedule="@weekly",
    catchup=False,
    params={"season": DEFAULT_SEASON},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "ncaaf"],
) as weekly_dag:
    t_schedules = PythonOperator(
        task_id="ingest_ncaaf_schedules",
        python_callable=ingest_ncaaf_schedules,
    )
    t_recaps = PythonOperator(
        task_id="ingest_espn_recaps",
        python_callable=ingest_espn_recaps,
    )
    t_schedules >> t_recaps


# ── Backfill DAG ─────────────────────────────────────────────────────────────

@dag(
    dag_id="ncaaf_bronze_backfill",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={"start_season": 2000, "end_season": 2023},
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "ncaaf", "backfill"],
)
def ncaaf_bronze_backfill():
    @task
    def season_range() -> list[int]:
        ctx = get_current_context()
        p = ctx["params"]
        return list(range(p["start_season"], p["end_season"] + 1))

    @task(max_active_tis_per_dagrun=4)
    def run_schedules(season: int) -> None:
        _run_schedules(season)

    @task(max_active_tis_per_dagrun=2)
    def run_recaps(season: int) -> None:
        _run_recaps(season)

    seasons = season_range()
    s = run_schedules.expand(season=seasons)
    r = run_recaps.expand(season=seasons)
    s >> r


ncaaf_bronze_backfill()
