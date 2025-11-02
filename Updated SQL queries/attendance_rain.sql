-- How much does attendance depend on the weather conditions?

SELECT
    v.venue_name,
    v.city,
    CASE
        WHEN w.precipitation >= 7.6 THEN '4. Heavy Rain (≥ 7.6 mm/h)'
        WHEN w.precipitation >= 2.5 AND w.precipitation < 7.6 THEN '3. Moderate Rain (2.5-7.6 mm/h)'
        WHEN w.precipitation >= 0.2 AND w.precipitation < 2.5 THEN '2. Light Rain (0.2-2.5 mm/h)'
        ELSE '1. Dry/Normal (< 0.2 mm/h)'
    END AS rain_category,
    
    COUNT(fm.match_id) AS total_matches, -- Number of matches played in each rain category per venue
    
    ROUND(AVG(fm.attendance), 0) AS average_attendance -- Average attendance (rounded to nearest person)
    
FROM 
    default_marts.fact_match AS fm  
INNER JOIN 
    default_marts.dim_weather AS w  
    ON fm.weather_key = w.weather_key
INNER JOIN
    default_marts.dim_venue AS v
    ON fm.venue_key = v.venue_key
WHERE
    fm.attendance IS NOT NULL AND fm.attendance > 0 
GROUP BY 
    v.venue_name,
    v.city,
    rain_category
ORDER BY 
    v.venue_name,
    rain_category DESC;