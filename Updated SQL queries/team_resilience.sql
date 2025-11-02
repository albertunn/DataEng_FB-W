-- Are certain teams more resilient to difficult weather conditions?

-- This query was generated with the help of Gemini AI.

-- Classify each match based on temperature and determine the win/loss outcome.
WITH MatchOutcomes AS (
    SELECT
        fm.match_id,
        fm.home_team_key,
        fm.away_team_key,
        CASE 
            WHEN w.avg_temperature < 5 THEN '1. Cold (< 5°C)'
            WHEN w.avg_temperature >= 30 THEN '3. Hot (> 30°C)'
            ELSE '2. Normal (5°C - 30°C)'
        END AS weather_temp_category,
        
        CASE WHEN fm.home_goals > fm.away_goals THEN 1 ELSE 0 END AS home_win, -- 1 if the home team won, 0 otherwise.
        CASE WHEN fm.away_goals > fm.home_goals THEN 1 ELSE 0 END AS away_win
    FROM
        default_marts.fact_match AS fm
    JOIN
        default_marts.dim_weather AS w 
        ON fm.weather_key = w.weather_key
),

-- Normalize the data by pivoting home and away match results into a single list of team performances.
TeamPerformance AS (
    SELECT -- Home Team Results
        home_team_key AS team_key,
        weather_temp_category,
        home_win AS is_win,
        1 AS total_played
    FROM MatchOutcomes
    UNION ALL
    SELECT -- Away Team Results
        away_team_key AS team_key,
        weather_temp_category,
        away_win AS is_win,
        1 AS total_played
    FROM MatchOutcomes
)

-- Final SELECT: Aggregate the normalized performance data by team and weather category.
SELECT
    t.team_name,
    tp.weather_temp_category,
    SUM(tp.total_played) AS total_games_played, -- Sum the total games played in this specific weather category.
    
    (SUM(tp.is_win) * 100.0 / SUM(tp.total_played)) AS win_percentage -- Calculate the Win Percentage for each team/category.
FROM
    TeamPerformance AS tp
JOIN
    default_marts.dim_team AS t
    ON tp.team_key = t.team_key
GROUP BY
    t.team_name,
    tp.weather_temp_category
ORDER BY
    t.team_name,
    tp.weather_temp_category DESC;