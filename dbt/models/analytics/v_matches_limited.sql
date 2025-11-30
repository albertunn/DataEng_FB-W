{{ config(
    materialized='view',
    schema='analytics'
) }}

WITH base AS (
    SELECT *
    FROM {{ ref('fact_match') }}
),

goal_ranges AS (
    SELECT
        match_id,
        date_key,
        venue_key,
        weather_key,
        home_team_key,
        away_team_key,

        -- Home goals: bucket into 2-goal ranges (0–1, 2–3, 4–5, ...)
        CASE
            WHEN home_goals IS NULL THEN NULL
            ELSE
                concat(
                    toString(intDiv(home_goals, 2) * 2),
                    '-',
                    toString(intDiv(home_goals, 2) * 2 + 1)
                )
        END AS home_goals_bucket,

        -- Away goals: bucket into 2-goal ranges
        CASE
            WHEN away_goals IS NULL THEN NULL
            ELSE
                concat(
                    toString(intDiv(away_goals, 2) * 2),
                    '-',
                    toString(intDiv(away_goals, 2) * 2 + 1)
                )
        END AS away_goals_bucket,

        -- Attendance: bucket into powers of 10
        CASE
            WHEN attendance IS NULL THEN NULL
            WHEN attendance < 10 THEN '0-10'
            WHEN attendance < 100 THEN '10-100'
            WHEN attendance < 1000 THEN '100-1000'
            WHEN attendance < 10000 THEN '1000-10 000'
            WHEN attendance < 100000 THEN '10 000-100 000'
            ELSE '100 000+'
        END AS attendance_bucket,

        home_shootout_score,
        away_shootout_score,

        home_fouls,
        away_fouls,
        home_yellow_cards,
        home_red_cards,
        away_yellow_cards,
        away_red_cards,
        home_possession_pct,
        away_possession_pct,
        home_corners,
        away_corners

    FROM base
)

SELECT
    match_id,
    date_key,
    venue_key,
    weather_key,
    home_team_key,
    away_team_key,
    home_goals_bucket AS home_goals,
    away_goals_bucket AS away_goals,
    attendance_bucket AS attendance,
    home_shootout_score,
    away_shootout_score,
    home_fouls,
    away_fouls,
    home_yellow_cards,
    home_red_cards,
    away_yellow_cards,
    away_red_cards,
    home_possession_pct,
    away_possession_pct,
    home_corners,
    away_corners

FROM goal_ranges
