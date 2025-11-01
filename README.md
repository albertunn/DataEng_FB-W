# DataEng_FB-W

Repository for **Data Engineering 2025** course.  
Project investigates how **weather conditions influence football match outcomes**.

---

## Data Sources
The project uses the following data sources:

| Source | Description |
|--------|-------------|
| Kaggle football datasets | Match results, team info, scores, etc. |
| Open-Meteo API | Weather data including temperature, precipitation, wind speed, snowfall, etc. |

---

## Data Layers

### 1. Raw Data (Bronze)
- Stores **original unprocessed data** in the following tables:
  - `bronze_weather`
  - `bronze_fixtures`
  - `bronze_teams`
  - `bronze_venues`
  - `bronze_teamStats`
- Purpose: keep data as-is for reproducibility and traceability.

### 2. Staging Layer (Silver)
- Located in `dbt/models/staging/`
- Purpose: **clean and standardize raw data** before building marts.
- **Key tasks:**
  - Type casting (e.g., convert strings to `Int32` or `DateTime`)
  - Handling null values
  - Creating **surrogate keys** using `halfMD5`
- **Key objects:**  
  - `stg_dim_*` → staging dimension views  
  - `stg_fact_*` → staging fact views  

### 3. Mart Layer (Gold)
- Located in `dbt/models/marts/`
- Purpose: **business-level curated tables** ready for analysis
- **Key objects:**  
  - Fact tables: `fact_match`  
  - Dimension tables: `dim_team`, `dim_date`, `dim_weather`, `dim_venue`  
- Notes: Aggregates and joins staging views into analytical tables suitable for querying.

---

## Running the project

### Project Setup and Configuration

1. #### Kaggle credentials: For initial data ingestion, write your credentials into the .env file:
   
`KAGGLE_USERNAME={username}
KAGGLE_KEY={token}`

2. #### Start services: Build and run all necessary containers (Airflow, dbt, Clickhouse)
```
docker compose up -d --build
```
3. #### Airflow: 
Access at [http://localhost:8080](http://localhost:8080). Start the `update_kaggle_espn_soccer` DAG to begin the ingestion and transformation process.

4. #### Clickhouse Client:
To open interactive SQL session directly in the database, use:
```
docker compose exec clickhouse-server clickhouse-client
```
### Manual dbt Workflow

While Airflow handles orchestration, it is possible to use dbt manually as well.

| Step | Command (Run from Host) | Purpose |
|------|--------------------------|----------|
| **1. Load Dependencies** | `docker exec -it dbt dbt deps` | Must be run first to download external dbt packages defined in `packages.yml`. |
| **2. Run Models** | `docker exec -it dbt dbt run` | Builds all staging and mart models. |
| **3. Run Tests** | `docker exec -it dbt dbt test` | Executes data quality tests. 



