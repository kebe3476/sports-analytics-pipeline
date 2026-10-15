with source as (

    select * from {{ source('bronze', 'mlb_games_raw') }}

)

select
    'mlb'                                   as sport,
    concat('mlb-', cast(mlb_game_id as string)) as game_id,
    cast(mlb_game_id as string)             as source_game_id,
    cast(season as int)                     as season,
    to_date(game_date)                      as game_date,
    case when game_type = 'R' then 'regular' else 'postseason' end as game_type,
    cast(home_team_id as string)            as home_team_id,
    home_team_abbrev                        as home_team_abbrev,
    home_team_name                          as home_team_name,
    cast(null as string)                    as home_team_conference,
    cast(away_team_id as string)            as away_team_id,
    away_team_abbrev                        as away_team_abbrev,
    away_team_name                          as away_team_name,
    cast(null as string)                    as away_team_conference,
    -- The bronze DAG does not capture scores yet; null until the ingest fix lands.
    cast(null as int)                       as home_score,
    cast(null as int)                       as away_score,
    cast(null as boolean)                   as overtime_flag,
    cast(null as boolean)                   as neutral_site,
    cast(null as int)                       as attendance
from source
where status in ('Final', 'Completed Early')
