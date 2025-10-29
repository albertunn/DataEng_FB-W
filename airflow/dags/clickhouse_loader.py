from clickhouse_driver import Client
import pandas as pd
import os

def load_to_clickhouse(data_dir, run_date):
    print(f"Loading data for {run_date} from {data_dir}")
    client = Client(host="clickhouse-server")
    client.execute("CREATE DATABASE IF NOT EXISTS matchData")
    client.execute("USE matchData")
    
    try:    
        # Create tables
        client.execute("""
        CREATE TABLE IF NOT EXISTS DimDate (
            date_key Date
        ) ENGINE = MergeTree() ORDER BY date_key
        """)
        
        client.execute("""
        CREATE TABLE IF NOT EXISTS DimTeam (
            team_id UInt32,
            team_name String,
            team_location String
        ) ENGINE = MergeTree() ORDER BY team_id
        """)

        client.execute("""
        CREATE TABLE IF NOT EXISTS DimVenue (
            venue_id UInt32,
            venue_name String,
            city String,
            country String,
            latitude Nullable(Float32),
            longitude Nullable(Float32)
        ) ENGINE = MergeTree() ORDER BY venue_id
        """)
        
        client.execute("""
        CREATE TABLE IF NOT EXISTS MatchFact (
            match_id UInt32,
            date_key Date,
            home_team_id UInt32,
            away_team_id UInt32,
            venue_id Int32,
            weather_id UInt32,
            home_score Nullable(UInt8),
            away_score Nullable(UInt8),
            home_fouls Nullable(UInt8),
            away_fouls Nullable(UInt8),
            home_yellow_cards Nullable(UInt8),
            away_yellow_cards Nullable(UInt8),
            home_red_cards Nullable(UInt8),
            away_red_cards Nullable(UInt8),
            home_possession Nullable(Float32),
            away_possession Nullable(Float32),
            home_corners Nullable(UInt8),
            away_corners Nullable(UInt8),
            home_shootout_score Nullable(UInt8),
            away_shootout_score Nullable(UInt8),
            attendance Nullable(UInt32)
        ) ENGINE = MergeTree() ORDER BY (match_id, date_key)
        """)
        
        # Read CSVs
        fixtures = pd.read_csv(os.path.join(data_dir, "fixtures.csv"))
        teams = pd.read_csv(os.path.join(data_dir, "teams.csv"))
        venues = pd.read_csv(os.path.join(data_dir, "venues.csv"))
        teamStats = pd.read_csv(os.path.join(data_dir, "teamStats.csv"))
        
        # Get existing IDs
        existing_dates = {row[0] for row in client.execute("SELECT date_key FROM DimDate")} if client.execute("EXISTS TABLE DimDate")[0][0] else set()
        existing_teams = {row[0] for row in client.execute("SELECT team_id FROM DimTeam")} if client.execute("EXISTS TABLE DimTeam")[0][0] else set()
        existing_venues = {row[0] for row in client.execute("SELECT venue_id FROM DimVenue")} if client.execute("EXISTS TABLE DimVenue")[0][0] else set()
        existing_matches = {row[0] for row in client.execute("SELECT match_id FROM MatchFact")} if client.execute("EXISTS TABLE MatchFact")[0][0] else set()
        
        # Load DimDate
        dimdate = fixtures[["date"]].drop_duplicates().copy()
        dimdate["date_key"] = pd.to_datetime(dimdate["date"]).dt.date
        dimdate = dimdate[["date_key"]]
        dimdate = dimdate[~dimdate["date_key"].isin(existing_dates)]
        if len(dimdate) > 0:
            dimdate_data = [tuple(x) for x in dimdate.values] 
            client.execute("INSERT INTO DimDate VALUES", dimdate_data)
        
        # Load DimTeam
        dimteam = teams[["teamId", "name", "location"]].copy()
        dimteam.columns = ["team_id", "team_name", "team_location"]
        dimteam = dimteam[~dimteam["team_id"].isin(existing_teams)]
        if len(dimteam) > 0:
            dimteam_data = [tuple(x) for x in dimteam.values]
            client.execute("INSERT INTO DimTeam VALUES", dimteam_data)
        
        # Load DimVenue
        dimvenue = venues[["venueId", "fullName", "city", "country"]].copy()
        dimvenue.columns = ["venue_id", "venue_name", "city", "country"]
        dimvenue["latitude"] = None
        dimvenue["longitude"] = None
        dimvenue = dimvenue[~dimvenue["venue_id"].isin(existing_venues)]
        if len(dimvenue) > 0:
            dimvenue_data = [tuple(x) for x in dimvenue.values]
            client.execute("INSERT INTO DimVenue VALUES", dimvenue_data)
        
        # Load MatchFact
        stat_cols_map = {
            "eventId": "eventId",
            "teamId": "teamId",
            "foulsCommitted": "fouls",
            "yellowCards": "yellow_cards",
            "redCards": "red_cards",
            "possessionPct": "possession",
            "wonCorners": "corners",
        }
        stats_subset = teamStats[list(stat_cols_map.keys())].rename(columns=stat_cols_map)

        home_stats = pd.merge(
            fixtures[["eventId", "homeTeamId"]],
            stats_subset,
            left_on=["eventId", "homeTeamId"],
            right_on=["eventId", "teamId"],
            how="left"
        ).drop(columns=["teamId", "homeTeamId"])
        home_stats.columns = ["eventId", "home_fouls", "home_yellow_cards", "home_red_cards", "home_possession", "home_corners"]
        
        away_stats = pd.merge(
            fixtures[["eventId", "awayTeamId"]],
            stats_subset,
            left_on=["eventId", "awayTeamId"],
            right_on=["eventId", "teamId"],
            how="left"
        ).drop(columns=["teamId", "awayTeamId"])
        away_stats.columns = ["eventId", "away_fouls", "away_yellow_cards", "away_red_cards", "away_possession", "away_corners"]

        matchfact = fixtures[["eventId", "date", "homeTeamId", "awayTeamId", "venueId", "attendance",
                              "homeTeamScore", "awayTeamScore", "homeTeamShootoutScore", "awayTeamShootoutScore"]].copy()

        matchfact = pd.merge(matchfact, home_stats, on="eventId", how="left")
        matchfact = pd.merge(matchfact, away_stats, on="eventId", how="left")

        matchfact.columns = ["match_id", "date_key_tmp", "home_team_id", "away_team_id", "venue_id", "attendance",
                             "home_score", "away_score", "home_shootout_score", "away_shootout_score", 
                             "home_fouls", "home_yellow_cards", "home_red_cards", "home_possession", "home_corners",
                             "away_fouls", "away_yellow_cards", "away_red_cards", "away_possession", "away_corners"]
                             
        matchfact["date_key"] = pd.to_datetime(matchfact["date_key_tmp"]).dt.date
        matchfact = matchfact.drop(columns=["date_key_tmp"])
        matchfact["weather_id"] = 0
          
        matchfact = matchfact[["match_id", "date_key", "home_team_id", "away_team_id", "venue_id", "weather_id", 
                               "home_score", "away_score", 
                               "home_fouls", "away_fouls", "home_yellow_cards", "away_yellow_cards", 
                               "home_red_cards", "away_red_cards", 
                               "home_possession", "away_possession", 
                               "home_corners", "away_corners", 
                               "home_shootout_score", "away_shootout_score", 
                               "attendance"]]
                               
        matchfact = matchfact[~matchfact["match_id"].isin(existing_matches)]
        
        if len(matchfact) > 0:
            matchfact_data = []
            for _, row in matchfact.iterrows():
                matchfact_data.append((
                    int(row['match_id']),
                    row['date_key'],
                    int(row['home_team_id']),
                    int(row['away_team_id']),
                    int(row['venue_id']),
                    int(row['weather_id']),
                    int(row['home_score']) if pd.notna(row['home_score']) else None,
                    int(row['away_score']) if pd.notna(row['away_score']) else None,
                    int(row['home_fouls']) if pd.notna(row['home_fouls']) else None,
                    int(row['away_fouls']) if pd.notna(row['away_fouls']) else None,
                    int(row['home_yellow_cards']) if pd.notna(row['home_yellow_cards']) else None,
                    int(row['away_yellow_cards']) if pd.notna(row['away_yellow_cards']) else None,
                    int(row['home_red_cards']) if pd.notna(row['home_red_cards']) else None,
                    int(row['away_red_cards']) if pd.notna(row['away_red_cards']) else None,
                    float(row['home_possession']) if pd.notna(row['home_possession']) else None,
                    float(row['away_possession']) if pd.notna(row['away_possession']) else None,
                    int(row['home_corners']) if pd.notna(row['home_corners']) else None,
                    int(row['away_corners']) if pd.notna(row['away_corners']) else None,
                    int(row['home_shootout_score']) if pd.notna(row['home_shootout_score']) else None,
                    int(row['away_shootout_score']) if pd.notna(row['away_shootout_score']) else None,
                    int(row['attendance']) if pd.notna(row['attendance']) else None,
                ))
            client.execute("INSERT INTO MatchFact VALUES", matchfact_data)
        
    finally:
        client.disconnect()
    
    return True