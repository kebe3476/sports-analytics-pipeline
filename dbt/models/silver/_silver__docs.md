{% docs stg_games_model %}
Cleaned, typed games for one sport, conformed to the shared silver game column set so the gold layer can union all sports without per-sport logic. Only completed games are kept, since fact_games records results.
{% enddocs %}

{% docs col_sport %}
Lowercase sport code: nfl, ncaaf, nhl, nba, or mlb.
{% enddocs %}

{% docs col_game_id %}
Sport-prefixed natural key (for example nfl-2024_01_GB_CHI). Prefixed because source ids are not unique across sports.
{% enddocs %}

{% docs col_source_game_id %}
Game id exactly as the source API issues it, kept for joining recaps and tracing back to bronze.
{% enddocs %}

{% docs col_season %}
Season start year. A 2023-24 NBA or NHL season is 2023.
{% enddocs %}

{% docs col_game_date %}
Local calendar date the game was played, used for dim_date lookups.
{% enddocs %}

{% docs col_game_type %}
Conformed game type: regular or postseason. Each source's own codes are mapped here.
{% enddocs %}

{% docs col_team_id %}
Source natural key for the team, unique only within a sport. Joins to dim_teams on sport plus this id.
{% enddocs %}

{% docs col_team_abbrev %}
Team abbreviation as the source issues it. Null for NCAAF, which has no abbreviations in the source.
{% enddocs %}

{% docs col_team_name %}
Team display name. Null for NFL, which has only abbreviations in the source.
{% enddocs %}

{% docs col_team_conference %}
Conference at the time of the game. Populated for NCAAF only.
{% enddocs %}

{% docs col_score %}
Final score. Null for MLB until the bronze ingest captures scores.
{% enddocs %}

{% docs col_overtime_flag %}
True if the game went past regulation (including NHL shootouts). Null where the source cannot tell.
{% enddocs %}

{% docs col_neutral_site %}
True if played at a neutral venue. Null where the source does not say.
{% enddocs %}

{% docs col_attendance %}
Announced attendance. Populated for NCAAF only.
{% enddocs %}
