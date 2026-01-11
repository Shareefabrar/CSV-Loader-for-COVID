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

2. **Build and run:**
    ```bash
   docker-compose up --build
   ```

3. **Check logs:**
    ```bash
   docker-compose logs -f app
   ```
4. **Logs foler:**
    ```bash
    docker-compose logs -f app --no-color | tee logs/logs.log   
    ```