--Team performance based on precipitation (2 rows for each match and ordered by teamID and then percipitaion)
SELECT 
    m.MatchID,
    d.Date,
    t.TeamHomeID AS TeamID,
    t.TeamName AS TeamName,
    opp.TeamHomeID AS OpponentID,
    opp.TeamName AS OpponentName,
    CASE 
        WHEN m.TeamID_home = t.TeamHomeID THEN m.HomeGoals
        ELSE m.AwayGoals
    END AS TeamGoals,
    CASE 
        WHEN m.TeamID_home = t.TeamHomeID THEN m.AwayGoals
        ELSE m.HomeGoals
    END AS OpponentGoals,
    (CASE 
        WHEN m.TeamID_home = t.TeamHomeID THEN m.HomeGoals - m.AwayGoals
        ELSE m.AwayGoals - m.HomeGoals
    END) AS GoalDifference,
    w.Precipitation
FROM 
    MatchFact as m
JOIN 
    DimTeam as t 
        ON t.TeamHomeID IN (m.TeamID_home, m.TeamID_away)
JOIN 
    DimTeam as opp 
        ON ( (m.TeamID_home = t.TeamHomeID AND m.TeamID_away = opp.TeamHomeID)
          OR  (m.TeamID_away = t.TeamHomeID AND m.TeamID_home = opp.TeamHomeID) )
JOIN 
    DimWeather as w ON m.WeatherID = w.WeatherID
JOIN 
    DimDate as d ON m.DateID = d.DateID
ORDER BY 
    t.TeamHomeID ASC,
    w.Precipitation ASC;