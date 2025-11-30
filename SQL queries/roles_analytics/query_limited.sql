--SET ROLE analyst_limited; -- Line not needed if logged in as user_limited

SELECT 
    match_id,
    home_goals,
    away_goals,
    attendance
FROM default_analytics.v_matches_limited
ORDER BY match_id
LIMIT 20;