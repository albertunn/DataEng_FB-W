--See teams performances in 5 hottest and 5 coldest matches (Team where TeamID=100 in this case)
SELECT 
    m.MatchID,
    w.temperature,
    CASE 
        WHEN m.TeamID_home = 100 THEN m.HomeGoals
        ELSE m.AwayGoals
    END AS GoalsFor,
    CASE 
        WHEN m.TeamID_home = 100 THEN m.AwayGoals
        ELSE m.HomeGoals
    END AS GoalsAgainst
FROM MatchFact AS m
JOIN DimWeather AS w ON m.WeatherID = w.WeatherID
WHERE m.TeamID_home = 100 OR m.TeamID_away = 100
ORDER BY w.temperature DESC -- (ASC in the case of coldest)
LIMIT 5;
