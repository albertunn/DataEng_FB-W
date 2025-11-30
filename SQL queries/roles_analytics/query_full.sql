--SET ROLE analyst_full; -- Line not needed if logged in as user_full

SELECT 
    match_id,
    home_goals,
    away_goals,
    attendance
FROM default_analytics.v_matches_full
ORDER BY match_id
LIMIT 20;