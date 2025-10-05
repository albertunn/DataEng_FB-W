--See matches between 2 teams (with specific ID’s) and order based on temperature
SELECT
    m.MatchID,
    d.Date,
    w.Temperature,
    t1.TeamName AS Team1_Name,
    t2.TeamName AS Team2_Name,
    CASE 
        WHEN m.TeamID_home = 1 THEN m.HomeGoals
        ELSE m.AwayGoals
    END AS Team1_Goals,
    CASE 
        WHEN m.TeamID_home = 2 THEN m.HomeGoals
        ELSE m.AwayGoals
    END AS Team2_Goals,
    (CASE 
        WHEN m.TeamID_home = 1 THEN m.HomeGoals - m.AwayGoals
        ELSE m.AwayGoals - m.HomeGoals
    END) AS GoalDifference_Team1
FROM 
    MatchFact m
JOIN 
    DimTeam as t1 ON t1.TeamHomeID = 1
JOIN 
    DimTeam as t2 ON t2.TeamHomeID = 2
JOIN 
    DimWeather as w ON m.WeatherID = w.WeatherID
JOIN 
    DimDate as d ON m.DateID = d.DateID
WHERE 
    (m.TeamID_home = 1 AND m.TeamID_away = 2)
    OR
    (m.TeamID_home = 2 AND m.TeamID_away = 1)
ORDER BY 
    w.Temperature ASC;