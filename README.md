# DataEng_FB-W
Repository for Data Engineering 2025 course. Project to investigate how weather conditions influence football match outcomes


## Running the project

Run the container with *docker compose up -d --build*

For Kaggle downloads to work, you need to write kaggle credentials into the .env file:
KAGGLE_USERNAME={username}
KAGGLE_KEY={token}

Airflow is accedssible at http://localhost:8080

Access dbt from *docker compose exec dbt bash* or through Docker Desktop dbt container terminal
