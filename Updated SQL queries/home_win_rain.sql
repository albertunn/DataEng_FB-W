-- Are home teams more/less affected by extreme weather?

SELECT
    CASE
        WHEN w.precipitation >= 7.6 THEN '4. Heavy Rain (≥ 7.6 mm/h)'
        WHEN w.precipitation >= 2.5 AND w.precipitation < 7.6 THEN '3. Moderate Rain (2.5-7.6 mm/h)'
        WHEN w.precipitation >= 0.2 AND w.precipitation < 2.5 THEN '2. Light Rain (0.2-2.5 mm/h)'
        ELSE '1. Dry/Normal (< 0.2 mm/h)'
    END AS rain_category,
    
    COUNT(fm.match_id) AS total_matches,
    
    ROUND(
        -- Percentage of matches where the home team scored more goals than the away team
        (SUM(CASE WHEN fm.home_goals > fm.away_goals THEN 1 ELSE 0 END) * 100.0) / COUNT(fm.match_id)
    , 2) AS home_win_percentage,
    
    ROUND(
        -- Percentage of matches where the home team scored fewer goals than the away team
        (SUM(CASE WHEN fm.home_goals < fm.away_goals THEN 1 ELSE 0 END) * 100.0) / COUNT(fm.match_id)
    , 2) AS home_loss_percentage
FROM 
    default_marts.fact_match AS fm  
INNER JOIN 
    default_marts.dim_weather AS w  
    ON fm.weather_key = w.weather_key
GROUP BY 
    rain_category
ORDER BY 
    rain_category;
