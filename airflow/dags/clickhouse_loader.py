from clickhouse_driver import Client
import pandas as pd
import os
from datetime import datetime

def load_to_clickhouse(data_dir, run_date):
    """
    Bronze Layer: Load raw CSV data exactly as-is into ClickHouse.
    No transformations, no deduplication - just raw ingestion.
    """
    print(f"🔹 Loading Bronze Layer data for {run_date} from {data_dir}")
    client = Client(host="clickhouse-server")

    try:
        client.execute("CREATE DATABASE IF NOT EXISTS matchData")
        client.execute("USE matchData")

        table_definitions = {
            "fixtures": """
                CREATE TABLE IF NOT EXISTS matchData.bronze_fixtures
                (
                  `Rn`                         Nullable(String),
                  `seasonType`                 Nullable(String),
                  `leagueId`                   Nullable(String),
                  `eventId`                    String,
                  `date`                       Nullable(String),
                  `venueId`                    Nullable(String),
                  `attendance`                 Nullable(String),
                  `homeTeamId`                 Nullable(String),
                  `awayTeamId`                 Nullable(String),
                  `homeTeamWinner`             Nullable(String),
                  `awayTeamWinner`             Nullable(String),
                  `homeTeamScore`              Nullable(String),
                  `awayTeamScore`              Nullable(String),
                  `homeTeamShootoutScore`      Nullable(String),
                  `awayTeamShootoutScore`      Nullable(String),
                  `statusId`                   Nullable(String),
                  `updateTime`                 Nullable(String),
                  `hasWeather`                 Nullable(String),
                  `_ingestion_time`            DateTime DEFAULT now()
                )
                ENGINE = ReplacingMergeTree(_ingestion_time)
                ORDER BY (`eventId`);
            """,
            "teams": """
                CREATE TABLE IF NOT EXISTS matchData.bronze_teams
                (
                  `teamId`           String,
                  `location`         Nullable(String),
                  `name`             Nullable(String),
                  `abbreviation`     Nullable(String),
                  `displayName`      Nullable(String),
                  `shortDisplayName` Nullable(String),
                  `color`            Nullable(String),
                  `alternateColor`   Nullable(String),
                  `logoURL`          Nullable(String),
                  `venueId`          Nullable(String),
                  `slug`             Nullable(String),
                  `_ingestion_time`  DateTime DEFAULT now()
                )
                ENGINE = ReplacingMergeTree(_ingestion_time)
                ORDER BY (`teamId`);
            """,
            "venues": """
                CREATE TABLE IF NOT EXISTS matchData.bronze_venues
                (
                  `venueId`          String,
                  `fullName`         Nullable(String),
                  `shortName`        Nullable(String),
                  `capacity`         Nullable(String),
                  `city`             Nullable(String),
                  `country`          Nullable(String),
                  `latitude`         Nullable(String),
                  `longitude`        Nullable(String),
                  `_ingestion_time`  DateTime DEFAULT now()
                )
                ENGINE = ReplacingMergeTree(_ingestion_time)
                ORDER BY (`venueId`);
            """,
            "teamStats": """
                CREATE TABLE IF NOT EXISTS matchData.bronze_teamStats
                (
                  `seasonType`         Nullable(String),
                  `eventId`            String,
                  `teamId`             String,
                  `teamOrder`          Nullable(String),
                  `possessionPct`      Nullable(String),
                  `foulsCommitted`     Nullable(String),
                  `yellowCards`        Nullable(String),
                  `redCards`           Nullable(String),
                  `offsides`           Nullable(String),
                  `wonCorners`         Nullable(String),
                  `saves`              Nullable(String),
                  `totalShots`         Nullable(String),
                  `shotsOnTarget`      Nullable(String),
                  `shotPct`            Nullable(String),
                  `penaltyKickGoals`   Nullable(String),
                  `penaltyKickShots`   Nullable(String),
                  `accuratePasses`     Nullable(String),
                  `totalPasses`        Nullable(String),
                  `passPct`            Nullable(String),
                  `accurateCrosses`    Nullable(String),
                  `totalCrosses`       Nullable(String),
                  `crossPct`           Nullable(String),
                  `totalLongBalls`     Nullable(String),
                  `accurateLongBalls`  Nullable(String),
                  `longballPct`        Nullable(String),
                  `blockedShots`       Nullable(String),
                  `effectiveTackles`   Nullable(String),
                  `totalTackles`       Nullable(String),
                  `tacklePct`          Nullable(String),
                  `interceptions`      Nullable(String),
                  `effectiveClearance` Nullable(String),
                  `totalClearance`     Nullable(String),
                  `updateTime`         Nullable(String),
                  `_ingestion_time`    DateTime DEFAULT now()
                )
                ENGINE = ReplacingMergeTree(_ingestion_time)
                ORDER BY (`eventId`, `teamId`);
            """
        }


        for ddl in table_definitions.values():
            client.execute(ddl)

        files = {
            "fixtures": "fixtures.csv",
            "teams": "teams.csv",
            "venues": "venues.csv",
            "teamStats": "teamStats.csv"
        }

        for table_name, filename in files.items():
            file_path = os.path.join(data_dir, filename)
            if not os.path.exists(file_path):
                print(f"Skipping {filename} (not found)")
                continue

            print(f"\nLoading {filename} into bronze_{table_name}...")
            df = pd.read_csv(file_path, dtype=str)
            df = df.astype(object).where(pd.notnull(df), None)
            df = df.applymap(lambda x: str(x) if x is not None else None)

            print(f"Read {len(df)} rows from CSV")
            
            fqtn = f"matchData.bronze_{table_name}"
            cols = ', '.join(f'`{c}`' for c in df.columns)

            data = [tuple(row) for row in df.values]
            
            client.execute(f"INSERT INTO {fqtn} ({cols}) VALUES", data)
            print(f"Inserted {len(data)} rows (ReplacingMergeTree will deduplicate)")

        print("\n" + "="*60)
        print("BRONZE LAYER SUMMARY")
        print("="*60)
        for table_name in files.keys():
            count = client.execute(f"SELECT COUNT(*) FROM matchData.bronze_{table_name} FINAL")[0][0]
            print(f"  bronze_{table_name}: {count:,} total rows")
        print("="*60)

    finally:
        client.disconnect()

    print("\nBronze Layer load complete")
    return True