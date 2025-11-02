--- Question: Is the variability of match outcomes higher in extreme weather conditions 
--- (very hot vs. very cold)?  

SELECT

    CASE
        WHEN w.temperature < 5 THEN '1. Cold (< 5°C)'
        WHEN w.temperature >= 30 THEN '3. Hot (> 30°C)'
        ELSE '2. Normal (5°C - 30°C)'
    END AS temperature_category,
    
    COUNT(fm.match_id) AS total_matches, -- Number of games in each category

    AVG(ABS(fm.home_goals - fm.away_goals)) AS avg_goal_difference,  -- Average goal difference 

    stddevSamp(fm.home_goals - fm.away_goals) AS stddev_goal_difference -- Standard deviation of goal differences: higher value = more unpredictable outcomes
FROM 
    default_marts.fact_match AS fm  
JOIN 
    default_marts.dim_weather AS w  
    ON fm.weather_key = w.weather_key
GROUP BY 
    temperature_category
ORDER BY 
    stddev_goal_difference DESC; 