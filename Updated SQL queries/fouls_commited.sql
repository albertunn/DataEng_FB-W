--  Does the number of fouls committed depend on weather conditions?  

SELECT
    CASE
        WHEN w.avg_precipitation >= 7.6 THEN '4. Heavy Rain (≥ 7.6 mm/h)'
        WHEN w.avg_precipitation >= 2.5 AND w.avg_precipitation < 7.6 THEN '3. Moderate Rain (2.5-7.6 mm/h)'
        WHEN w.avg_precipitation >= 0.2 AND w.avg_precipitation < 2.5 THEN '2. Light Rain (0.2-2.5 mm/h)'
        ELSE '1. Dry/Normal (< 0.2 mm/h)'
    END AS rain_category,
    
    COUNT(fm.match_id) AS total_matches, -- Total number of matches played under each weather condition
    AVG(fm.home_fouls + fm.away_fouls) AS average_total_fouls, -- Average total fouls per match (home + away combined)
    AVG(fm.home_fouls)AS average_home_fouls, -- Average fouls committed by home teams
    AVG(fm.away_fouls) AS average_away_fouls -- Average fouls committed by away teams
    
FROM 
    default_marts.fact_match AS fm  
INNER JOIN 
    default_marts.dim_weather AS w  
    ON fm.weather_key = w.weather_key
WHERE
    fm.home_fouls IS NOT NULL AND fm.away_fouls IS NOT NULL
GROUP BY 
    rain_category
ORDER BY 
    rain_category;