with source as (

    select * from {{ source('bronze', 'nba_games_raw') }}

),

ranked as (

    select
        *,
        -- Neutral-site games have "@" on both rows, so fall back to the lower TEAM_ID to pick a stable home side.
        row_number() over (
            partition by GAME_ID
            order by case when MATCHUP like '%vs.%' then 0 else 1 end, TEAM_ID
        ) as side_rank,
        max(case when MATCHUP like '%vs.%' then 1 else 0 end) over (partition by GAME_ID) = 0 as is_neutral
    from source

),

home as (select * from ranked where side_rank = 1),
away as (select * from ranked where side_rank = 2)

select
    'nba'                                   as sport,
    concat('nba-', home.GAME_ID)            as game_id,
    home.GAME_ID                            as source_game_id,
    cast(home.season as int)                as season,
    to_date(home.GAME_DATE)                 as game_date,
    case when home.season_type = 'Playoffs' then 'postseason' else 'regular' end as game_type,
    cast(home.TEAM_ID as string)            as home_team_id,
    home.TEAM_ABBREVIATION                  as home_team_abbrev,
    home.TEAM_NAME                          as home_team_name,
    cast(null as string)                    as home_team_conference,
    cast(away.TEAM_ID as string)            as away_team_id,
    away.TEAM_ABBREVIATION                  as away_team_abbrev,
    away.TEAM_NAME                          as away_team_name,
    cast(null as string)                    as away_team_conference,
    cast(home.PTS as int)                   as home_score,
    cast(away.PTS as int)                   as away_score,
    -- Regulation is 240 team minutes; each overtime adds 25.
    home.MIN > 240                          as overtime_flag,
    home.is_neutral                         as neutral_site,
    cast(null as int)                       as attendance
from home
inner join away
    on home.GAME_ID = away.GAME_ID
