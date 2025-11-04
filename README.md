# DataEng_FB-W

Repository for **Data Engineering 2025** course.  
Project investigates how **weather conditions influence football match outcomes**.

---

## Data Sources
The project uses the following data sources:

| Source | Description | Link |
|--------|-------------|------|
| **Kaggle Football Dataset** | Match results, team info, scores, etc. | [Kaggle Dataset](https://www.kaggle.com/datasets/excel4soccer/espn-soccer-data/data) |
| **Open-Meteo API** | Weather data including temperature, precipitation, wind speed, snowfall, etc. | [Open-Meteo API](https://open-meteo.com/) |


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
Access at [http://localhost:8080](http://localhost:8080). Username and password are both: `airflow`. Start the `update_kaggle_espn_soccer` DAG to begin the ingestion and transformation process.

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

---

## Screenshot of Airflow and dbt DAGS

<img width="1867" height="324" alt="dags" src="https://github.com/user-attachments/assets/67bcc1a1-8109-4f48-8145-16d61516413c" />

<img width="814" height="144" alt="graph_1" src="https://github.com/user-attachments/assets/5579db34-1d38-4a6a-8c6e-0fb2c99c19bd" />

<img width="796" height="223" alt="graph_2" src="https://github.com/user-attachments/assets/67e78e36-636e-4327-91bf-f9de63a27433" />

<img width="420" height="169" alt="graph_3" src="https://github.com/user-attachments/assets/fac04502-9bee-4437-9a15-bb2dae71be61" />

---

## Results of the Example Analytical Queries from Project 1
Queries can be found from the Updated SQL Queries folder.

1. #### Is the variability of match outcomes higher in extreme weather conditions (e.g, very hot vs. very cold, strong wind, heavy rain)?
match_outcome_variability_rain.sql, match_outcome_variability_wind.sql, match_outcome_variability_temp.sql

<img width="953" height="294" alt="test_variability_rain" src="https://github.com/user-attachments/assets/898ebf81-ea36-4dc3-88f7-1d1c7312b448" />

<img width="954" height="271" alt="test_variability_wind" src="https://github.com/user-attachments/assets/0cf15263-b8e1-4b85-8330-381da51462f3" />

<img width="956" height="269" alt="test_variability_temp" src="https://github.com/user-attachments/assets/c211ace8-2dd7-4c63-b4d1-aed8872af155" />


2. #### Are certain teams more resilient to difficult weather conditions?
team.resilience.sql

<img width="713" height="482" alt="test_resilience" src="https://github.com/user-attachments/assets/fa991251-d6b1-4159-a24b-6994be124d41" />


3. #### How much does attendance depend on the weather conditions?
attendance_rain.sql

<img width="956" height="535" alt="test_attendance" src="https://github.com/user-attachments/assets/31571862-3c70-484b-b814-84859d01c113" />

4. #### Are home teams more/less affected by extreme weather?
home_win_rain.sql 

<img width="958" height="296" alt="test_homewin_rain" src="https://github.com/user-attachments/assets/21a6db88-5b72-450f-b513-27d09a211f68" />

   
6. #### Does the number of fouls committed depend on weather conditions?
fouls_commited.sql

<img width="956" height="323" alt="test_fouls_committed" src="https://github.com/user-attachments/assets/61e018d5-ee75-4d3d-ba06-020592af7551" />


8. #### Does bad weather impact penalty shootouts?
penalty.sql

<img width="1632" height="273" alt="575650483_1323332595663169_7199510167246905563_n" src="https://github.com/user-attachments/assets/b33a4ede-aae2-4587-8167-dbe9aaa5a05d" />

