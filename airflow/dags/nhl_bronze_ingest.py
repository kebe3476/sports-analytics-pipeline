from __future__ import annotations

import os
import time
from collections import defaultdict
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from databricks import sql as databricks_sql

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator, get_current_context

CATALOG = "workspace"
SCHEMA = "bronze"
DEFAULT_SEASON = 2024
NHL_BASE = "https://api-web.nhle.com/v1"
ESPN_NHL_BASE = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl"


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
    main = f"`{CATALOG}`.`{SCHEMA}`.`{table}`"
    staging = f"`{CATALOG}`.`{SCHEMA}`.`{table}_staging_{season}`"
    col_defs = _col_defs(df)
    spark_type_map = {
        "int64": "BIGINT", "float64": "DOUBLE",
        "bool": "BOOLEAN", "datetime64[ns]": "TIMESTAMP",
    }
    df_col_set = set(df.columns)

    def _staging_type(col: str) -> str:
        return spark_type_map.get(str(df[col].dtype), "STRING")

    cursor.execute(f"CREATE TABLE IF NOT EXISTS {main} ({col_defs}) USING DELTA")

    # Get main's current schema in column order
    result = cursor.execute(f"DESCRIBE TABLE {main}")
    main_cols = [
        (row[0], row[1]) for row in result.fetchall()
        if row[0] and not row[0].startswith("#")
    ]
    main_schema = {name: typ for name, typ in main_cols}

    # Add columns in df that are missing from main
    for col in df.columns:
        if col not in main_schema:
            col_type = spark_type_map.get(str(df[col].dtype), "STRING")
            cursor.execute(f"ALTER TABLE {main} ADD COLUMN `{col}` {col_type}")
            main_cols.append((col, col_type))
            main_schema[col] = col_type

    cursor.execute(f"CREATE OR REPLACE TABLE {staging} ({col_defs}) USING DELTA")
    _insert_rows(cursor, staging, df, batch_size)

    # Build SELECT aligned to main's column order:
    # - column exists in staging with matching type: select as-is
    # - column exists in staging but type differs: try_cast (returns NULL on failure)
    # - column missing from staging: CAST(NULL AS type)
    select_parts = []
    for col_name, col_type in main_cols:
        if col_name in df_col_set:
            if _staging_type(col_name).lower() == col_type.lower():
                select_parts.append(f"`{col_name}`")
            else:
                select_parts.append(
                    f"try_cast(`{col_name}` AS {col_type.upper()}) AS `{col_name}`"
                )
        else:
            select_parts.append(f"CAST(NULL AS {col_type.upper()}) AS `{col_name}`")

    select_clause = ", ".join(select_parts)
    cursor.execute(
        f"INSERT INTO {main} REPLACE WHERE season = {season} SELECT {select_clause} FROM {staging}"
    )
    cursor.execute(f"DROP TABLE IF EXISTS {staging}")


def _run_schedules(season_year: int) -> None:
    current = date(season_year, 10, 1)
    end = date(season_year + 1, 7, 1)
    all_games: list[dict] = []
    seen_ids: set = set()
    while current <= end:
        for attempt in range(5):
            resp = requests.get(
                f"{NHL_BASE}/schedule/{current.strftime('%Y-%m-%d')}",
                timeout=30,
            )
            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"NHL API 429, waiting {wait}s (attempt {attempt + 1}/5)")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        for week in resp.json().get("gameWeek", []):
            for game in week.get("games", []):
                if game["id"] not in seen_ids:
                    seen_ids.add(game["id"])
                    all_games.append(game)
        current += timedelta(days=7)
        time.sleep(1.0)

    if not all_games:
        print(f"No NHL games found for season {season_year}.")
        return

    df = pd.json_normalize(all_games, sep="_")
    df = df.rename(columns={"id": "nhl_game_id"})
    # Overwrite 8-digit NHL season code with 4-digit start year for consistency with NFL/NCAAF
    df["season"] = season_year
    df = _coerce_df(df)
    df["_ingested_at"] = datetime.utcnow()

    with _db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
            _write_season(cursor, df, "nhl_games_raw", season_year)
    print(f"Loaded {len(df)} NHL games for season {season_year}")


def _run_recaps(season_year: int) -> None:
    # Step 1: load completed games from bronze
    with _db_conn() as conn:
        with conn.cursor() as cursor:
            result = cursor.execute(
                f"SELECT nhl_game_id, startTimeUTC, homeTeam_abbrev "
                f"FROM `{CATALOG}`.`{SCHEMA}`.`nhl_games_raw` "
                f"WHERE season = {season_year} AND gameState = 'OFF'"
            )
            game_rows = result.fetchall()

    # Step 2: group by date, call ESPN scoreboard per date, map home abbrev -> ESPN event ID
    games_by_date: dict = defaultdict(list)
    for nhl_game_id, start_time_utc, home_abbrev in game_rows:
        game_date = start_time_utc[:10] if start_time_utc else None
        if game_date:
            games_by_date[game_date].append((nhl_game_id, home_abbrev))

    espn_id_map: dict = {}
    for game_date, games in sorted(games_by_date.items()):
        date_str = game_date.replace("-", "") if isinstance(game_date, str) else game_date.strftime("%Y%m%d")
        try:
            resp = requests.get(
                f"{ESPN_NHL_BASE}/scoreboard",
                params={"dates": date_str},
                timeout=15,
            )
            resp.raise_for_status()
            home_to_espn: dict = {}
            for event in resp.json().get("events", []):
                comps = event.get("competitions", [{}])[0]
                for competitor in comps.get("competitors", []):
                    if competitor.get("homeAway") == "home":
                        home_to_espn[competitor["team"]["abbreviation"]] = event["id"]
            for nhl_game_id, home_abbrev in games:
                espn_id = home_to_espn.get(home_abbrev)
                if espn_id:
                    espn_id_map[nhl_game_id] = espn_id
                else:
                    print(f"No ESPN match for NHL game {nhl_game_id} ({home_abbrev} on {game_date})")
        except Exception as exc:
            print(f"WARNING: ESPN scoreboard failed for {game_date}: {exc}")
        time.sleep(0.3)

    # Step 3: fetch ESPN recap for each matched game
    records = []
    for nhl_game_id, espn_event_id in espn_id_map.items():
        url = f"{ESPN_NHL_BASE}/summary?event={espn_event_id}"
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            records.append({
                "nhl_game_id": nhl_game_id,
                "espn_game_id": espn_event_id,
                "season": season_year,
                "raw_json": resp.text,
                "_ingested_at": datetime.utcnow(),
            })
        except requests.HTTPError:
            if resp.status_code == 404:
                print(f"No ESPN recap for event {espn_event_id}: 404")
            elif resp.status_code == 429:
                raise
            else:
                print(f"WARNING: ESPN {resp.status_code} for event {espn_event_id}, skipping")
        except Exception as exc:
            print(f"Skipped ESPN event {espn_event_id}: {exc}")
        time.sleep(0.5)

    if not records:
        print("No ESPN recap records to load.")
        return

    df = pd.DataFrame(records)
    with _db_conn() as conn:
        with conn.cursor() as cursor:
            _write_season(cursor, df, "nhl_recaps_raw", season_year, batch_size=1)
    print(f"Loaded {len(records)} NHL recaps for season {season_year}")


# ── Weekly DAG ───────────────────────────────────────────────────────────────

def ingest_nhl_schedules(**context):
    season = context["params"].get("season", DEFAULT_SEASON)
    _run_schedules(season)


def ingest_espn_recaps(**context):
    season = context["params"].get("season", DEFAULT_SEASON)
    _run_recaps(season)


with DAG(
    dag_id="nhl_bronze_ingest",
    start_date=datetime(2024, 10, 1),
    schedule="@weekly",
    catchup=False,
    params={"season": DEFAULT_SEASON},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "nhl"],
) as weekly_dag:
    t_schedules = PythonOperator(
        task_id="ingest_nhl_schedules",
        python_callable=ingest_nhl_schedules,
    )
    t_recaps = PythonOperator(
        task_id="ingest_espn_recaps",
        python_callable=ingest_espn_recaps,
    )
    t_schedules >> t_recaps


# ── Backfill DAG ─────────────────────────────────────────────────────────────

@dag(
    dag_id="nhl_bronze_backfill",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={"start_season": 2010, "end_season": 2023},
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "nhl", "backfill"],
)
def nhl_bronze_backfill():
    @task
    def season_range() -> list[int]:
        ctx = get_current_context()
        p = ctx["params"]
        return list(range(p["start_season"], p["end_season"] + 1))

    @task(max_active_tis_per_dagrun=1)
    def run_schedules(season: int) -> None:
        _run_schedules(season)

    @task(max_active_tis_per_dagrun=2)
    def run_recaps(season: int) -> None:
        _run_recaps(season)

    seasons = season_range()
    s = run_schedules.expand(season=seasons)
    r = run_recaps.expand(season=seasons)
    s >> r


nhl_bronze_backfill()
