--- Question: Is the variability of match outcomes higher in extreme weather conditions 
--- (rain)?  

SELECT

    CASE
        WHEN w.avg_precipitation >= 7.6 THEN '4. Heavy Rain (≥ 7.6 mm/h)'
        WHEN w.avg_precipitation >= 2.5 AND w.avg_precipitation < 7.6 THEN '3. Moderate Rain (2.5-7.6 mm/h)'
        WHEN w.avg_precipitation >= 0.2 AND w.avg_precipitation < 2.5 THEN '2. Light Rain (0.2-2.5 mm/h)'
        ELSE '1. Dry/Normal (< 0.2 mm/h)'
    END AS rain_category,
    
    COUNT(fm.match_id) AS total_matches, -- Number of games in each rain category

    AVG(ABS(fm.home_goals - fm.away_goals)) AS avg_goal_difference,  -- Average goal difference 

    stddevSamp(fm.home_goals - fm.away_goals) AS stddev_goal_difference -- Standard deviation of goal differences: higher value = more unpredictable outcomes
FROM 
    default_marts.fact_match AS fm  
JOIN 
    default_marts.dim_weather AS w  
    ON fm.weather_key = w.weather_key
GROUP BY 
    rain_category
ORDER BY 
    stddev_goal_difference DESC; 