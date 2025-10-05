--Query to see venues where rain deteriorates the playing conditions the most
SELECT 
    v.VenueName,
    v.City,
    COUNT(m.MatchID) AS TotalMatches,
    AVG(CASE 
            WHEN w.Precipitation > 2 THEN (m.HomeFouls + m.AwayFouls)
        END) AS AvgFouls_Rainy,
    AVG(CASE 
            WHEN w.Precipitation <= 2 THEN (m.HomeFouls + m.AwayFouls)
        END) AS AvgFouls_Otherwise
FROM 
    MatchFact as m
JOIN 
    DimWeather as w ON m.WeatherID = w.WeatherID
JOIN 
    DimVenue as v ON m.VenuneID = v.VenueID
GROUP BY 
    v.VenueName, v.City
ORDER BY 
    AvgFouls_Rainy DESC;
