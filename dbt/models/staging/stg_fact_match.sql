WITH fixtures AS (
    SELECT
        CAST(eventId AS Int32) AS event_id,
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

raw_stats AS (
    SELECT
        CAST(eventId AS Int32) AS event_id,
        CAST(teamId AS Int32) AS team_id,

        -- FIX: Foul/Card/Corner data are raw strings containing decimals (e.g., '6.0').
        -- To cast them to an Integer (Int32) without error, we need a multi-step conversion:
        -- 1. NULLIF handles empty strings by converting them to NULL.
        -- 2. toFloat64 converts the string ('6.0') to a floating-point number (6.0).
        -- 3. toInt32 truncates the floating-point number (6.0) to an integer (6).
        -- 4. The outer CAST ensures the final Nullable(Int32) type.
        
        CAST(toInt32(toFloat64(NULLIF(foulsCommitted, ''))) AS Nullable(Int32)) AS fouls_committed,
        CAST(toInt32(toFloat64(NULLIF(yellowCards, ''))) AS Nullable(Int32)) AS yellow_cards,
        CAST(toInt32(toFloat64(NULLIF(redCards, ''))) AS Nullable(Int32)) AS red_cards,
        CAST(NULLIF(possessionPct, '') AS Nullable(Decimal(5,2))) AS possession_pct,
        CAST(toInt32(toFloat64(NULLIF(wonCorners, ''))) AS Nullable(Int32)) AS corners

    FROM {{ source('raw_data', 'bronze_teamStats') }}
),

team_stats_agg AS (
    SELECT
        f.event_id,
        -- Home team stats
        MAX(CASE WHEN rs.team_id = f.home_team_id THEN rs.fouls_committed END) AS home_fouls,
        MAX(CASE WHEN rs.team_id = f.home_team_id THEN rs.yellow_cards END) AS home_yellow_cards,
        MAX(CASE WHEN rs.team_id = f.home_team_id THEN rs.red_cards END) AS home_red_cards,
        MAX(CASE WHEN rs.team_id = f.home_team_id THEN rs.possession_pct END) AS home_possession_pct,
        MAX(CASE WHEN rs.team_id = f.home_team_id THEN rs.corners END) AS home_corners,
        -- Away team stats
        MAX(CASE WHEN rs.team_id = f.away_team_id THEN rs.fouls_committed END) AS away_fouls,
        MAX(CASE WHEN rs.team_id = f.away_team_id THEN rs.yellow_cards END) AS away_yellow_cards,
        MAX(CASE WHEN rs.team_id = f.away_team_id THEN rs.red_cards END) AS away_red_cards,
        MAX(CASE WHEN rs.team_id = f.away_team_id THEN rs.possession_pct END) AS away_possession_pct,
        MAX(CASE WHEN rs.team_id = f.away_team_id THEN rs.corners END) AS away_corners
    FROM fixtures AS f
    LEFT JOIN raw_stats AS rs
        ON f.event_id = rs.event_id
    GROUP BY f.event_id
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
LEFT JOIN {{ ref('dim_date') }} AS dd
    ON halfMD5(toString(f.match_datetime) || '|' || toString(f.venue_id)) = dd.date_key
LEFT JOIN {{ ref('dim_venue') }} AS dv
    ON f.venue_id = dv.venue_id
LEFT JOIN {{ ref('dim_weather') }} AS dw
    ON dw.weather_key = halfMD5(
        toString(f.event_id) || '|' || toString(toStartOfHour(f.match_datetime))
    )
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
