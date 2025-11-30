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

5. #### Create roles for analysts with full and limited views.

Run `docker exec -it clickhouse-server clickhouse-client --multiquery --queries-file=/sql/roles_analytics/create_roles.sql` to create roles `analyst_full` and `analyst_limited`. Limited analyst gets pseudonymized data for rows `home_goals`, `away_goals` and `attendance` (chosen randomly, because this project doesn't contain any actually sensitive data).

To login to clickhouse as user `user_limited` or `user_full` use command `docker exec -it clickhouse-server clickhouse-client --disable_suggestion -u user_* --password *_pw` (Replace `*` with the desired role)

SQL files to see how roles with different permissions get different answers on the same data are located in `/SQL queries/roles_analytics` as `query_full.sql` and `query_limited.sql`.

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

---

## Project 3

---

### Apache Iceberg

TODO

### Clickhouse

TODO



### OpenMetaData

The following services must be running and configured before proceeding to the OpenMetadata setup:
1. **Docker** is up and running
2. **Airflow DAGs** initialized (Kaggle first, then the weather data)
3. **DBT Run** completed
   
#### 1. Clickhouse Security Setup

1.1 Create User and Grant Permissions

```
docker compose exec clickhouse-server clickhouse-client

CREATE ROLE role_openmetadata;

CREATE USER service_openmetadata IDENTIFIED WITH sha256_password BY 'omd_very_secret_password';

GRANT role_openmetadata TO service_openmetadata;

GRANT SELECT, SHOW ON system.* TO role_openmetadata;

GRANT SELECT ON default_marts.* TO role_openmetadata;

```
#### 2. OpenMetadata UI Configuration

Log into the OpenMetadata UI at: `http://localhost:8085`

- **Username:** `admin@open-metadata.org`
- **Password:** `admin`

##### 2.1 Add ClickHouse Database Service

This step registers the ClickHouse data source with OpenMetadata.

1. Go to Settings → Services → Databases.
2. Click + Add New Service.
3. Choose ClickHouse as the service type.
4. Fill in the Connection Details:
   * **Service Name**: clickhouse_matches (or similar)
   * **Host and Port**: clickhouse-server:8123
   * **Username**: service_openmetadata
   * **Password**: omd_very_secret_password
   * **Database / Schema**: Leave empty.
   * **Https / Secure**: Leave them off.
5. Click Test Connection. If successful, click Next and Save the service.

##### 2.2 Metadata Agent Deployment
Trigger the Metadata Agent (if it's not running automatically already). Once the Metadata Agent has successfully run, you should be able to see the tables and columns in OpenMetadata.  
 After triggering the Metadata Agent, trigger the other agents (**Usage**, **Lineage**, **Profiler**) as well.

##### 2.3 DBT Manifest Fix and Description Import

##### 2.3.1 Automated Fix for Manifest File

The manifest.json file generated by dbt docs generate may contain empty database paths ("database": ""), which OpenMetadata interprets as a different database than default. This causes errors during description import.

Run the following Python script to update the manifest.json file to explicitly use the expected database name (default) where paths are missing:
1. Navigate to your dbt project directory:
```
cd dbt
```
2. Generate the latest documentation (which updates manifest.json):

```
docker exec -it dbt dbt docs generate
```

3. Run your custom Python script (fix_manifest.py) to adjust the database names:

```
python fix_manifest.py
```

#### 2.3.2 Automated Fix for Manifest File

After fixing the manifest, set up the dbt agent in OpenMetadata to automatically import all model and column descriptions:

1. Go to the **ClickHouse service** you created.
2. Click **+ Add New Agent**.
3. Select the **dbt agent** type.
4. Configure the agent to point to the location of the fixed manifest file: ` /dbt/target/manifest.json`.

     
Validation: Check your tables in OpenMetadata. Column descriptions should now be automatically populated based on your dbt models.

**Evidence**
<p align="center">
  <img width="601" height="391" alt="tables" src="https://github.com/user-attachments/assets/b89c1601-57b2-4afc-a2d2-b92b19fa7acc" />
</p>

<p align="center">
  <img width="398" height="425" alt="dimdate" src="https://github.com/user-attachments/assets/0e503223-dd73-4eb3-97c4-060b50924436" />
</p>

<p align="center">
  <img width="407" height="464" alt="dimteam" src="https://github.com/user-attachments/assets/b769bba3-bf29-47d0-aa84-8eba2e948ecd" />
</p>

<p align="center">
  <img width="409" height="524" alt="dimvenue" src="https://github.com/user-attachments/assets/301fb928-e653-42fd-9eea-32fa9850f51f" />
</p>

<p align="center">
  <img width="401" height="551" alt="dimweather" src="https://github.com/user-attachments/assets/2e21a572-7e7c-48b9-a870-97b557823fb8" />
</p>

<p align="center">
  <img width="394" height="445" alt="factmatch" src="https://github.com/user-attachments/assets/874b8a68-774c-48d2-909c-796b0b4ab5cf" />
</p>


#### 2.4 Data Quality Tests

We wrote **3 data quality tests**:

1. Validate that `dim_date`'s surrogate key is **unique**.
2. Validate that `home_team_key` is **not null** in the `fact_match` table.
3. Validate that `away_goals` in the `fact_match` table is **not a negative number**.

<img width="548" height="429" alt="dq_tests" src="https://github.com/user-attachments/assets/91013aa6-100c-4f10-a321-42b12bdb77e3" />

#### 2.5 Register Superset Dashboard Service

Register the Superset connection to discover dashboards and link them to the underlying data tables:

1. Go to **Settings → Services → Dashboards**.
2. Click **+ Add New Service**.
3. Choose **Superset** as the service type.
4. Fill in the connection details:
   - **Host and Port:** `http://superset:8088`
   - **Provider:** `db`
   - **Username:** `admin`
   - **Password:** `admin`
5. Click **Test Connection** and **Save**.

**Evidence**
<br>
<img width="673" height="571" alt="dasboard-on-openmetadata" src="https://github.com/user-attachments/assets/92ec84ec-ba02-4b2b-a141-80c56009efed" />

### Apache Superset

NB! The superset-init service sometimes fails due to script line-ending issues (LF vs. CRLF). If superset-init throws errors, ensure all relevant Docker scripts (docker-init-with-deps.sh, jne.) are converted to the LF (Unix/Linux) format.

#### 1. Add ClickHouse Connection (for Visualization)

In Superset **(default login: username: `admin`, password: `admin`)**, you need to register the database connection:

1. Go to **Settings → Database Connections**.
2. Click **+ Database**.
3. Choose **ClickHouse Connect** as the database type.
4. Fill in the connection details:
   - **Host:** `clickhouse-server`
   - **Port:** `8123`
   - **Username:** `default`
5. **Test and Connect:** Verify that tables are available for visualization.

---

#### Business Question 1 — Home Team Performance vs Weather

We created three stacked bar charts to answer **Business Question 1: Are home teams more or less affected by extreme weather?**

- Categorised matches based on **temperature**, **rainfall**, and **wind speed**  
- Calculated the average **home win percentage** and **home loss percentage** for each weather category

---

#### Business Question 2 — Outcome Variability in Extreme Weather

To answer **Business Question 2: Is the variability of match outcomes higher in extreme weather conditions?** we created three summary tables showing:

- The **number of matches** in each weather category  
- The **average goal difference**  
- The **outcome variability** using **standard deviation**

---

#### Filters

We added a **Country filter** to analyse how weather impacts match outcomes across different countries.


**Evidence**
#### Business Question 1 – without filter
<p align="center">
  <img width="545" height="491" alt="bq_1_wo_filter" src="https://github.com/user-attachments/assets/c381dbd1-0254-403e-9a03-b05eb6d5a889" />
</p>

#### Business Question 1 – filtered for Argentina
<p align="center">
  <img width="671" height="482" alt="bq_1_w_filter" src="https://github.com/user-attachments/assets/ce287abf-f341-4009-8034-bb5c2eaaaf0a" />
</p>

#### Business Question 2 – filtered for Argentina
<p align="center">
  <img width="668" height="632" alt="bq_2_w_filter" src="https://github.com/user-attachments/assets/2f816282-9bcd-41e5-b43f-bd8ad941bc94" />
</p>


