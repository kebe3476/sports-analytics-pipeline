from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

import nfl_data_py as nfl
import pandas as pd
import requests
from databricks import sql as databricks_sql

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator, get_current_context

CATALOG = "workspace"
SCHEMA = "bronze"
DEFAULT_SEASON = 2024


def _db_conn():
    return databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


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
    """
    Atomic season-level write using a staging table and Delta's REPLACE WHERE.
    Writes all rows for the season to a staging table first, then swaps into the
    main table in a single Delta transaction. The main table is never left with
    zero rows for the season due to a failure between delete and insert.
    """
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
    df = nfl.import_schedules([season])
    df = _coerce_df(df)
    df["_ingested_at"] = datetime.utcnow()
    with _db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
            _write_season(cursor, df, "nfl_games_raw", season)
    print(f"Loaded {len(df)} games for season {season}")


def _run_recaps(season: int) -> None:
    with _db_conn() as conn:
        with conn.cursor() as cursor:
            result = cursor.execute(
                f"SELECT game_id, espn FROM `{CATALOG}`.`{SCHEMA}`.`nfl_games_raw` "
                f"WHERE season = {season} AND espn IS NOT NULL"
            )
            game_rows = result.fetchall()

    records = []
    for game_id, espn_id in game_rows:
        url = (
            "https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary"
            f"?event={int(float(espn_id))}"
        )
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            records.append({
                "game_id": game_id,
                "espn_game_id": str(int(float(espn_id))),
                "season": season,
                "raw_json": resp.text,
                "_ingested_at": datetime.utcnow(),
            })
        except requests.HTTPError:
            if resp.status_code == 404:
                print(f"No ESPN data for game {espn_id}: 404")
            elif resp.status_code == 429:
                raise  # rate limited: fail task so Airflow retry backs off
            else:
                print(f"WARNING: ESPN {resp.status_code} for game {espn_id}, skipping")
        except Exception as exc:
            print(f"Skipped ESPN game {espn_id}: {exc}")
        time.sleep(0.5)

    if not records:
        print("No ESPN records to load.")
        return

    df = pd.DataFrame(records)
    with _db_conn() as conn:
        with conn.cursor() as cursor:
            _write_season(cursor, df, "nfl_recaps_raw", season, batch_size=1)
    print(f"Loaded {len(records)} recaps for season {season}")


# ── Weekly DAG ───────────────────────────────────────────────────────────────

def ingest_nfl_schedules(**context):
    season = context["params"].get("season", DEFAULT_SEASON)
    _run_schedules(season)


def ingest_espn_recaps(**context):
    season = context["params"].get("season", DEFAULT_SEASON)
    _run_recaps(season)


with DAG(
    dag_id="nfl_bronze_ingest",
    start_date=datetime(2024, 9, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    params={"season": DEFAULT_SEASON},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "nfl"],
) as weekly_dag:
    t_schedules = PythonOperator(
        task_id="ingest_nfl_schedules",
        python_callable=ingest_nfl_schedules,
    )
    t_recaps = PythonOperator(
        task_id="ingest_espn_recaps",
        python_callable=ingest_espn_recaps,
    )
    t_schedules >> t_recaps


# ── Backfill DAG ─────────────────────────────────────────────────────────────

@dag(
    dag_id="nfl_bronze_backfill",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={"start_season": 1999, "end_season": 2023},
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "nfl", "backfill"],
)
def nfl_bronze_backfill():
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


nfl_bronze_backfill()
