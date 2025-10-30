-- models/staging/stg_fact_match.sql
{{ config(materialized='view') }}

WITH fixtures AS (
    SELECT
        CAST(eventId AS String) AS event_id,
        CAST(date AS DateTime) AS match_datetime, 
        CAST(venueId AS String) AS venue_id,
        CAST(homeTeamId AS String) AS home_team_id,
        CAST(awayTeamId AS String) AS away_team_id,
        

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

    halfMD5(toString(f.match_datetime) || '|' || toString(f.venue_id)) AS date_key, 
    halfMD5(toString(f.home_team_id)) AS team_key_home, 
    halfMD5(toString(f.away_team_id)) AS team_key_away, 
    halfMD5(toString(f.venue_id)) AS venue_key, 
    
    halfMD5(toString(w.latitude) || '|' || toString(w.longitude) || '|' || toString(w.timestamp_utc)) AS weather_key,

    
    f.home_goals,
    f.away_goals,
    f.home_shootout_score,
    f.away_shootout_score,
    f.attendance,

    ts.home_fouls,
    ts.away_fouls,
    ts.home_yellow_cards,
    ts.home_red_cards,
    ts.away_yellow_cards,
    ts.away_red_cards,
    ts.home_possession_pct,
    ts.away_possession_pct,
    ts.home_corners,
    ts.away_corners

FROM fixtures AS f
LEFT JOIN team_stats_agg AS ts  
    ON f.event_id = ts.event_id  

LEFT JOIN {{ source('raw_data', 'bronze_weather') }} w
    ON f.event_id = CAST(w.eventId AS String)
    AND w.timestamp_utc = toStartOfHour(f.match_datetime)