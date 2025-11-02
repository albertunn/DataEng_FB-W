--- Question: Is the variability of match outcomes higher in extreme weather conditions 
--- (wind speed)?  

SELECT

    CASE
        WHEN w.wind_speed >= 36 THEN '1. Strong (≥ 36 km/h)'
        WHEN w.wind_speed >= 20 AND w.wind_speed < 36 THEN '2. Moderate (20-36 km/h)'
        ELSE '3. Light (< 20 km/h)'
    END AS wind_category,
    
    COUNT(fm.match_id) AS total_matches, -- Number of games in each category
    AVG(ABS(fm.home_goals - fm.away_goals)) AS avg_goal_difference,  -- Average goal difference 

    stddevSamp(fm.home_goals - fm.away_goals) AS stddev_goal_difference -- Standard deviation of goal differences: higher value = more unpredictable outcomes
FROM 
    default_marts.fact_match AS fm  
JOIN 
    default_marts.dim_weather AS w  
    ON fm.weather_key = w.weather_key
GROUP BY 
    wind_category
ORDER BY 
    stddev_goal_difference DESC; 