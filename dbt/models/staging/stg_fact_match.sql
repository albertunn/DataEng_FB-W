-- models/staging/stg_fact_match.sql
{{ config(materialized='view') }}

WITH fixtures AS (
    SELECT
        CAST(eventId AS String) AS event_id,
        CAST(date AS DateTime) AS match_datetime, 
        CAST(venueId AS Int32) AS venue_id,
        CAST(homeTeamId AS Int32) AS home_team_id,
        CAST(awayTeamId AS Int32) AS away_team_id,
        
        CAST(NULLIF(attendance, '') AS Nullable(Int32)) AS attendance,
        CAST(NULLIF(homeTeamScore, '') AS Nullable(Int32)) AS home_goals,
        CAST(NULLIF(awayTeamScore, '') AS Nullable(Int32)) AS away_goals,
        CAST(NULLIF(homeTeamShootoutScore, '') AS Nullable(Int32)) AS home_shootout_score,
        CAST(NULLIF(awayTeamShootoutScore, '') AS Nullable(Int32)) AS away_shootout_score
    FROM {{ source('raw_data', 'bronze_fixtures') }}
    WHERE eventId IS NOT NULL
),

team_stats_agg AS (
    SELECT
        ts.eventId AS event_id, 
        
        SUM(CASE WHEN ts.teamId = f.homeTeamId THEN CAST(NULLIF(foulsCommitted, '') AS Nullable(Int32)) ELSE 0 END) AS home_fouls,
        SUM(CASE WHEN ts.teamId = f.homeTeamId THEN CAST(NULLIF(yellowCards, '') AS Nullable(Int32)) ELSE 0 END) AS home_yellow_cards,
        SUM(CASE WHEN ts.teamId = f.homeTeamId THEN CAST(NULLIF(redCards, '') AS Nullable(Int32)) ELSE 0 END) AS home_red_cards,
        SUM(CASE WHEN ts.teamId = f.homeTeamId THEN CAST(NULLIF(possessionPct, '') AS Nullable(Decimal(5,2))) ELSE 0 END) AS home_possession_pct,
        SUM(CASE WHEN ts.teamId = f.homeTeamId THEN CAST(NULLIF(wonCorners, '') AS Nullable(Int32)) ELSE 0 END) AS home_corners,

        SUM(CASE WHEN ts.teamId = f.awayTeamId THEN CAST(NULLIF(foulsCommitted, '') AS Nullable(Int32)) ELSE 0 END) AS away_fouls,
        SUM(CASE WHEN ts.teamId = f.awayTeamId THEN CAST(NULLIF(yellowCards, '') AS Nullable(Int32)) ELSE 0 END) AS away_yellow_cards,
        SUM(CASE WHEN ts.teamId = f.awayTeamId THEN CAST(NULLIF(redCards, '') AS Nullable(Int32)) ELSE 0 END) AS away_red_cards,
        SUM(CASE WHEN ts.teamId = f.awayTeamId THEN CAST(NULLIF(possessionPct, '') AS Nullable(Decimal(5,2))) ELSE 0 END) AS away_possession_pct,
        SUM(CASE WHEN ts.teamId = f.awayTeamId THEN CAST(NULLIF(wonCorners, '') AS Nullable(Int32)) ELSE 0 END) AS away_corners
        
    FROM {{ source('raw_data', 'bronze_teamStats') }} ts
    INNER JOIN {{ source('raw_data', 'bronze_fixtures') }} f 
        ON ts.eventId = f.eventId
    GROUP BY ts.eventId
)

SELECT
    halfMD5(toString(f.event_id)) AS match_id, 

    dd.date_key,
    dv.venue_key,
    dw.weather_key,

    th.team_key AS home_team_key,
    ta.team_key AS away_team_key,
    
    MAX(f.home_goals) AS home_goals,
    MAX(f.away_goals) AS away_goals,
    MAX(f.home_shootout_score) AS home_shootout_score,
    MAX(f.away_shootout_score) AS away_shootout_score,
    MAX(f.attendance) AS attendance,

    MAX(ts.home_fouls) AS home_fouls,
    MAX(ts.away_fouls) AS away_fouls,
    MAX(ts.home_yellow_cards) AS home_yellow_cards,
    MAX(ts.home_red_cards) AS home_red_cards,
    MAX(ts.away_yellow_cards) AS away_yellow_cards,
    MAX(ts.away_red_cards) AS away_red_cards,
    MAX(ts.home_possession_pct) AS home_possession_pct,
    MAX(ts.away_possession_pct) AS away_possession_pct,
    MAX(ts.home_corners) AS home_corners,
    MAX(ts.away_corners) AS away_corners

FROM fixtures AS f
LEFT JOIN team_stats_agg AS ts  
    ON f.event_id = ts.event_id  

LEFT JOIN {{ source('raw_data', 'bronze_weather') }} w
    ON f.event_id = CAST(w.eventId AS String)
    AND toStartOfHour(w.timestamp_utc) = toStartOfHour(f.match_datetime)

LEFT JOIN {{ ref('dim_date') }} AS dd 
    ON halfMD5(toString(f.match_datetime) || '|' || toString(f.venue_id)) = dd.date_key

LEFT JOIN {{ ref('dim_venue') }} AS dv 
    ON f.venue_id = dv.venue_id

LEFT JOIN {{ ref('dim_weather') }} AS dw 
    ON halfMD5(
        toString(w.latitude) || '|' || toString(w.longitude) || '|' || toString(w.timestamp_utc)
    ) = dw.weather_key 
    
LEFT JOIN {{ ref('dim_team') }} AS th 
    ON f.home_team_id = th.team_id
    
LEFT JOIN {{ ref('dim_team') }} AS ta 
    ON f.away_team_id = ta.team_id

GROUP BY
    match_id, 
    dd.date_key,
    dv.venue_key,
    dw.weather_key,
    home_team_key,
    away_team_key
    