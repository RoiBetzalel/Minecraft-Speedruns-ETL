# Minecraft Speedruns ETL Pipeline

## Project Overview

This project automates the extraction, transformation, and loading (ETL) of Minecraft speedrun data.

The pipeline extracts raw data from MongoDB, cleans and transforms it using Python and Pandas, and loads the processed data into PostgreSQL.

Apache Airflow orchestrates the workflow.

---

## Tech Stack

- Python
- Apache Airflow
- MongoDB
- PostgreSQL
- Pandas
- Docker
- Power BI

---

## Architecture

MongoDB
↓
Extract
↓
Transform
↓
PostgreSQL
↓
Power BI

---

## Screenshots

## MongoDB

![MongoDB](screenshots/MongoDB.png)

## Airflow DAG

![Airflow](screenshots/Airflow.png)

## PostgreSQL

![PgAdmin](screenshots/Pgadmin.png)
