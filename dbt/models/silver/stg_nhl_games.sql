with source as (

    select * from {{ source('bronze', 'nhl_games_raw') }}

)

select
    'nhl'                                   as sport,
    concat('nhl-', cast(nhl_game_id as string)) as game_id,
    cast(nhl_game_id as string)             as source_game_id,
    cast(season as int)                     as season,
    to_date(from_utc_timestamp(startTimeUTC, venueTimezone)) as game_date,
    case when gameType = 2 then 'regular' else 'postseason' end as game_type,
    cast(homeTeam_id as string)             as home_team_id,
    homeTeam_abbrev                         as home_team_abbrev,
    concat_ws(' ', homeTeam_placeName_default, homeTeam_commonName_default) as home_team_name,
    cast(null as string)                    as home_team_conference,
    cast(awayTeam_id as string)             as away_team_id,
    awayTeam_abbrev                         as away_team_abbrev,
    concat_ws(' ', awayTeam_placeName_default, awayTeam_commonName_default) as away_team_name,
    cast(null as string)                    as away_team_conference,
    cast(homeTeam_score as int)             as home_score,
    cast(awayTeam_score as int)             as away_score,
    gameOutcome_lastPeriodType in ('OT', 'SO') as overtime_flag,
    neutralSite                             as neutral_site,
    cast(null as int)                       as attendance
from source
-- 2 = regular season, 3 = playoffs; drops preseason, all-star, and international exhibition types.
where gameType in (2, 3)
  and gameState in ('OFF', 'FINAL')
