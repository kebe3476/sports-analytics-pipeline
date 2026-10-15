with source as (

    select * from {{ source('bronze', 'ncaaf_games_raw') }}

)

select
    'ncaaf'                                 as sport,
    concat('ncaaf-', cast(cfbd_game_id as string)) as game_id,
    cast(cfbd_game_id as string)            as source_game_id,
    cast(season as int)                     as season,
    -- startDate is UTC; TBD kickoffs are stored as midnight Eastern, so convert before truncating.
    to_date(from_utc_timestamp(startDate, 'America/New_York')) as game_date,
    seasonType                              as game_type,
    cast(homeId as string)                  as home_team_id,
    cast(null as string)                    as home_team_abbrev,
    homeTeam                                as home_team_name,
    homeConference                          as home_team_conference,
    cast(awayId as string)                  as away_team_id,
    cast(null as string)                    as away_team_abbrev,
    awayTeam                                as away_team_name,
    awayConference                          as away_team_conference,
    cast(homePoints as int)                 as home_score,
    cast(awayPoints as int)                 as away_score,
    size(from_json(homeLineScores, 'array<int>')) > 4 as overtime_flag,
    neutralSite                             as neutral_site,
    cast(attendance as int)                 as attendance
from source
where completed
