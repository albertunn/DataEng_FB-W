-- Does bad weather impact penalty shootouts?

SELECT
    CASE
        -- Extreme Weather: Heavy Rain (> 7.6 mm/h) OR Very Cold (< 5°C) OR Very Hot (> 25°C)
        WHEN w.precipitation > 7.6 OR w.temperature < 5 OR w.temperature >= 25 THEN '1. Extreme Weather (Heavy Rain/Extreme Temp)'
        ELSE '2. Normal Weather'
    END AS weather_category,

    COUNT(fm.match_id) AS total_shootouts, -- Count the number of penalty shootouts in each weather category.

    -- Average total goals scored in the shootout (Home + Away)
    ROUND(
        AVG(fm.home_shootout_score + fm.away_shootout_score) -- Calculate the average total goals scored during the shootout phase for each category.
    , 2) AS average_shootout_goals_per_match

FROM 
    default_marts.fact_match AS fm  
INNER JOIN 
    default_marts.dim_weather AS w  
    ON fm.weather_key = w.weather_key
WHERE
    (fm.home_shootout_score IS NOT NULL OR fm.away_shootout_score IS NOT NULL)
    AND (fm.home_shootout_score > 0 OR fm.away_shootout_score > 0)
GROUP BY 
    weather_category
ORDER BY 
    weather_category;