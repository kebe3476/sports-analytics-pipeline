from __future__ import annotations

import os
import time
from collections import defaultdict
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
ESPN_NBA_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"

# NBA API uses different abbreviations than ESPN for some franchises
NBA_TO_ESPN_ABBREV = {
    "GSW": "GS",
    "NYK": "NY",
    "NOP": "NO",
    "SAS": "SA",
    "NOH": "NO",
    "NOK": "NO",
    "NJN": "NJ",
    "UTA": "UTAH",  # Utah Jazz: NBA API uses UTA, ESPN uses UTAH
    "WAS": "WSH",   # Washington Wizards: NBA API uses WAS, ESPN uses WSH
}


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

    result = cursor.execute(f"DESCRIBE TABLE {main}")
    main_cols = [
        (row[0], row[1]) for row in result.fetchall()
        if row[0] and not row[0].startswith("#")
    ]
    main_schema = {name: typ for name, typ in main_cols}

    for col in df.columns:
        if col not in main_schema:
            col_type = spark_type_map.get(str(df[col].dtype), "STRING")
            cursor.execute(f"ALTER TABLE {main} ADD COLUMN `{col}` {col_type}")
            main_cols.append((col, col_type))
            main_schema[col] = col_type

    cursor.execute(f"DROP TABLE IF EXISTS {staging}")
    cursor.execute(f"CREATE TABLE {staging} ({col_defs}) USING DELTA")
    _insert_rows(cursor, staging, df, batch_size)

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


def _upsert_recaps(cursor, df: pd.DataFrame, table: str, id_col: str, batch_size: int = 50) -> None:
    """
    Merge recap rows into the main table, matching on (id_col, season).
    Unlike _write_season, this never deletes existing rows, making it safe
    for incremental writes: call multiple times and already-written rows are
    updated in place while new rows are inserted.
    """
    import uuid as _uuid

    main = f"`{CATALOG}`.`{SCHEMA}`.`{table}`"
    staging = f"`{CATALOG}`.`{SCHEMA}`.`{table}_stg_{_uuid.uuid4().hex[:8]}`"
    col_defs = _col_defs(df)

    cursor.execute(f"CREATE TABLE IF NOT EXISTS {main} ({col_defs}) USING DELTA")
    cursor.execute(f"CREATE OR REPLACE TABLE {staging} ({col_defs}) USING DELTA")
    _insert_rows(cursor, staging, df, batch_size)

    set_clause = ", ".join(
        f"t.`{c}` = s.`{c}`" for c in df.columns if c not in (id_col, "season")
    )
    cursor.execute(
        f"MERGE INTO {main} AS t USING {staging} AS s "
        f"ON t.`{id_col}` = s.`{id_col}` AND t.`season` = s.`season` "
        f"WHEN MATCHED THEN UPDATE SET {set_clause} "
        f"WHEN NOT MATCHED THEN INSERT *"
    )
    cursor.execute(f"DROP TABLE IF EXISTS {staging}")


def _run_schedules(season_year: int) -> None:
    # Lazy import: nba_api is not installed until after Docker rebuild; importing
    # at module level would break DAG parsing before the image is rebuilt.
    from nba_api.stats.endpoints import LeagueGameLog  # noqa: PLC0415

    # nba_api season format: "2023-24"
    season_str = f"{season_year}-{str(season_year + 1)[-2:]}"
    dfs = []
    for season_type in ("Regular Season", "Playoffs"):
        try:
            log = LeagueGameLog(
                season=season_str,
                player_or_team_abbreviation="T",
                season_type_all_star=season_type,
                timeout=60,
            )
            df = log.get_data_frames()[0]
            if not df.empty:
                df["season_type"] = season_type
                dfs.append(df)
                print(f"Fetched {len(df)} rows for {season_str} {season_type}")
        except Exception as exc:
            print(f"WARNING: Failed to fetch {season_type} for {season_str}: {exc}")
        time.sleep(0.6)

    if not dfs:
        print(f"No NBA games found for season {season_year}.")
        return

    df = pd.concat(dfs, ignore_index=True)
    df["season"] = season_year
    df = _coerce_df(df)
    df["_ingested_at"] = datetime.utcnow()

    with _db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
            _write_season(cursor, df, "nba_games_raw", season_year)
    print(f"Loaded {len(df)} NBA team-game rows for season {season_year}")


def _run_recaps(season_year: int) -> None:
    # Pull home-team rows only (MATCHUP contains "vs.") for completed games.
    # Also check which game IDs already have recaps written so retries skip them.
    with _db_conn() as conn:
        with conn.cursor() as cursor:
            result = cursor.execute(
                f"SELECT GAME_ID, GAME_DATE, TEAM_ABBREVIATION "
                f"FROM `{CATALOG}`.`{SCHEMA}`.`nba_games_raw` "
                f"WHERE season = {season_year} "
                f"AND MATCHUP LIKE '%vs.%' "
                f"AND WL IS NOT NULL"
            )
            game_rows = result.fetchall()

            existing_ids: set = set()
            try:
                ex = cursor.execute(
                    f"SELECT nba_game_id FROM `{CATALOG}`.`{SCHEMA}`.`nba_recaps_raw` "
                    f"WHERE season = {season_year}"
                )
                existing_ids = {str(r[0]) for r in ex.fetchall()}
                if existing_ids:
                    print(f"Found {len(existing_ids)} existing recaps for season {season_year}, will skip them.")
            except Exception:
                pass  # Table doesn't exist yet; start fresh.

    if not game_rows:
        print(f"No completed NBA home-team rows found for season {season_year}.")
        return

    # Group by date for scoreboard lookup (one ESPN call per date, not per game).
    games_by_date: dict = defaultdict(list)
    for game_id, game_date, home_abbrev in game_rows:
        if game_date:
            date_key = game_date[:10] if isinstance(game_date, str) else game_date.strftime("%Y-%m-%d")
            games_by_date[date_key].append((game_id, home_abbrev))

    espn_id_map: dict = {}
    for game_date, games in sorted(games_by_date.items()):
        date_str = game_date.replace("-", "")
        try:
            resp = requests.get(
                f"{ESPN_NBA_BASE}/scoreboard",
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
            for game_id, home_abbrev in games:
                espn_abbrev = NBA_TO_ESPN_ABBREV.get(home_abbrev, home_abbrev)
                espn_id = home_to_espn.get(espn_abbrev)
                if espn_id:
                    espn_id_map[game_id] = espn_id
                else:
                    print(f"No ESPN match for NBA game {game_id} ({home_abbrev} -> {espn_abbrev} on {game_date})")
        except Exception as exc:
            print(f"WARNING: ESPN scoreboard failed for {game_date}: {exc}")
        time.sleep(0.3)

    if not espn_id_map:
        print(f"No ESPN IDs resolved for season {season_year}.")
        return

    # Skip games whose recaps are already written -- makes retries resume from
    # where the previous run left off instead of starting over.
    pending = {k: v for k, v in espn_id_map.items() if str(k) not in existing_ids}
    if not pending:
        print(f"All {len(espn_id_map)} matched recaps already written for season {season_year}.")
        return
    if existing_ids:
        print(
            f"Fetching recaps for {len(pending)} of {len(espn_id_map)} matched games "
            f"({len(existing_ids)} already written)."
        )

    # Fetch ESPN recap summaries and flush to Databricks in batches.
    # Flushing every FLUSH_EVERY records means a task kill only loses the
    # current batch; the next retry skips everything already committed.
    FLUSH_EVERY = 100
    records: list = []
    total_written = 0

    def _flush(batch: list) -> None:
        nonlocal total_written
        df_batch = _coerce_df(pd.DataFrame(batch))
        with _db_conn() as conn:
            with conn.cursor() as cursor:
                _upsert_recaps(cursor, df_batch, "nba_recaps_raw", "nba_game_id")
        total_written += len(batch)
        print(f"Flushed {len(batch)} recaps (total so far: {total_written})")

    for game_id, espn_event_id in pending.items():
        url = f"{ESPN_NBA_BASE}/summary?event={espn_event_id}"
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            records.append({
                "nba_game_id": game_id,
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

        if len(records) >= FLUSH_EVERY:
            _flush(records)
            records = []

    if records:
        _flush(records)

    print(f"Loaded {total_written} NBA recaps for season {season_year}")


# ── Weekly DAG ───────────────────────────────────────────────────────────────

def ingest_nba_schedules(**context):
    season = context["params"].get("season", DEFAULT_SEASON)
    _run_schedules(season)


def ingest_espn_recaps(**context):
    season = context["params"].get("season", DEFAULT_SEASON)
    _run_recaps(season)


with DAG(
    dag_id="nba_bronze_ingest",
    start_date=datetime(2024, 10, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    params={"season": DEFAULT_SEASON},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "nba"],
) as weekly_dag:
    t_schedules = PythonOperator(
        task_id="ingest_nba_schedules",
        python_callable=ingest_nba_schedules,
    )
    t_recaps = PythonOperator(
        task_id="ingest_espn_recaps",
        python_callable=ingest_espn_recaps,
    )
    t_schedules >> t_recaps


# ── Backfill DAG ─────────────────────────────────────────────────────────────

@dag(
    dag_id="nba_bronze_backfill",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={"start_season": 2010, "end_season": 2023},
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "nba", "backfill"],
)
def nba_bronze_backfill():
    @task
    def season_range() -> list[int]:
        ctx = get_current_context()
        p = ctx["params"]
        return list(range(p["start_season"], p["end_season"] + 1))

    @task(max_active_tis_per_dagrun=2)
    def run_schedules(season: int) -> None:
        _run_schedules(season)

    @task(max_active_tis_per_dagrun=2)
    def run_recaps(season: int) -> None:
        _run_recaps(season)

    seasons = season_range()
    s = run_schedules.expand(season=seasons)
    r = run_recaps.expand(season=seasons)
    s >> r


nba_bronze_backfill()
