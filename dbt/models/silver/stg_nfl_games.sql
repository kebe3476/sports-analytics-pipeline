with source as (

    select * from {{ source('bronze', 'nfl_games_raw') }}

)

select
    'nfl'                                   as sport,
    concat('nfl-', game_id)                 as game_id,
    game_id                                 as source_game_id,
    cast(season as int)                     as season,
    to_date(gameday)                        as game_date,
    case when game_type = 'REG' then 'regular' else 'postseason' end as game_type,
    -- nflverse has no team ids; the abbreviation is the only team key available.
    home_team                               as home_team_id,
    home_team                               as home_team_abbrev,
    cast(null as string)                    as home_team_name,
    cast(null as string)                    as home_team_conference,
    away_team                               as away_team_id,
    away_team                               as away_team_abbrev,
    cast(null as string)                    as away_team_name,
    cast(null as string)                    as away_team_conference,
    cast(home_score as int)                 as home_score,
    cast(away_score as int)                 as away_score,
    overtime = 1                            as overtime_flag,
    location = 'Neutral'                    as neutral_site,
    cast(null as int)                       as attendance
from source
where home_score is not null
