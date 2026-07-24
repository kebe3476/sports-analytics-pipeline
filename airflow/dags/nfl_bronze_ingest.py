from __future__ import annotations

import os
from datetime import datetime, timedelta

import nfl_data_py as nfl
import pandas as pd
import requests
from databricks import sql as databricks_sql

from airflow import DAG
from airflow.operators.python import PythonOperator

CATALOG = "main"
SCHEMA = "bronze"
DEFAULT_SEASON = 2024


def _db_conn():
    return databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def _coerce_df(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten any non-primitive object columns to strings so every value maps cleanly to a Delta type."""
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(
            lambda x: None if (x is None or (isinstance(x, float) and pd.isna(x))) else str(x)
        )
    return df


def _write_df(cursor, df: pd.DataFrame, table: str) -> None:
    full = f"`{CATALOG}`.`{SCHEMA}`.`{table}`"
    type_map = {
        "int64": "BIGINT",
        "float64": "DOUBLE",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
    }
    col_defs = ", ".join(
        f"`{c}` {type_map.get(str(df[c].dtype), 'STRING')}" for c in df.columns
    )

    cursor.execute(f"DROP TABLE IF EXISTS {full}")
    cursor.execute(f"CREATE TABLE {full} ({col_defs}) USING DELTA")

    rows = [
        tuple(None if (v is None or (isinstance(v, float) and pd.isna(v))) else v for v in row)
        for row in df.itertuples(index=False)
    ]

    # Multi-row parameterized INSERT in batches of 100
    row_template = "(" + ", ".join(["%s"] * len(df.columns)) + ")"
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        placeholders = ", ".join([row_template] * len(batch))
        params = [v for row in batch for v in row]
        cursor.execute(f"INSERT INTO {full} VALUES {placeholders}", params)


def ingest_nfl_schedules(**context):
    season = context["params"].get("season", DEFAULT_SEASON)

    df = nfl.import_schedules([season])
    df = _coerce_df(df)
    df["_ingested_at"] = datetime.utcnow()

    with _db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
            _write_df(cursor, df, "nfl_games_raw")

    print(f"Loaded {len(df)} games for season {season}")


def ingest_espn_recaps(**context):
    season = context["params"].get("season", DEFAULT_SEASON)

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
            records.append(
                {
                    "game_id": game_id,
                    "espn_game_id": str(int(float(espn_id))),
                    "season": season,
                    "raw_json": resp.text,
                    "_ingested_at": datetime.utcnow(),
                }
            )
        except Exception as exc:
            print(f"Skipped ESPN game {espn_id}: {exc}")

    if not records:
        print("No ESPN records to load.")
        return

    df = pd.DataFrame(records)
    with _db_conn() as conn:
        with conn.cursor() as cursor:
            _write_df(cursor, df, "nfl_recaps_raw")

    print(f"Loaded {len(df)} recaps for season {season}")


with DAG(
    dag_id="nfl_bronze_ingest",
    start_date=datetime(2024, 9, 1),
    schedule="@weekly",
    catchup=False,
    params={"season": DEFAULT_SEASON},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "nfl"],
) as dag:
    t_schedules = PythonOperator(
        task_id="ingest_nfl_schedules",
        python_callable=ingest_nfl_schedules,
    )
    t_recaps = PythonOperator(
        task_id="ingest_espn_recaps",
        python_callable=ingest_espn_recaps,
    )
    t_schedules >> t_recaps
