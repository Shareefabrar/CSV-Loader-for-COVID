# CSV to PostgreSQL Data Loader with Docker

A small ETL loader that downloads COVID and countries CSVs, cleans them, and inserts into Postgres

## Features

- **Dockerized Environment**: Complete containerized setup with PostgreSQL and Python
- **Open Source Data**: Loads COVID-19 data from Our World in Data
- **Automated Pipeline**: Download, clean, transform, and load data automatically
- **Data Validation**: Includes data quality checks and summary reports
- **Sample Data**: Falls back to sample data if downloads fail
- **SQL Views**: Pre-configured analytical views for easy querying

## Prerequisites

- Docker (20.10+)
- Docker Compose (2.0+)
- Git

## Quick Start

1. **Clone and setup:**
   ```bash
   git clone <your-repo-url>
   cd csv-to-postgres-docker
   ```
2. **Add .env file with the parameters:**
    ```bash
    echo "POSTGRES_USER={postgres_user}
    POSTGRES_PASSWORD={postgres_pass}
    POSTGRES_DB={DB_name}

    # Open Source Data URLs (Example: COVID-19 data from Our World in Data)
    COVID_DATA_URL=https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv
    COUNTRIES_DATA_URL=https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv" >>.env
    ```

3. **Build and run:**
    ```bash
   docker-compose up --build
   ```

4. **Check logs:**
    ```bash
   docker-compose logs -f app
   ```
5. **Logs foler:**
    ```bash
    docker-compose logs -f app --no-color | tee logs/logs.log   
    ```