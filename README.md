# NYC Taxi Data Pipeline: Medallion Architecture

## Project Overview
An automated ELT data pipeline built to process NYC Taxi data. This project transforms raw data through a Medallion Architecture (Bronze, Silver, Gold) to engineer AI-ready features for downstream analytics.

## Architecture & Tech Stack
* **Data Engine:** PySpark, Databricks
* **Orchestration:** Databricks Workflows & Jobs
* **CI/CD & Version Control:** GitHub Actions
* **Target Data Warehouse:** Snowflake (Connection code established; full execution modeled for standard compute environments)

## Pipeline Stages
1. **Extract (Bronze):** Ingests raw NYC Taxi dataset directly into Databricks using Delta format.
2. **Transform (Silver):** Cleans anomalous data points, specifically filtering for valid passenger counts and trip distances.
3. **Load (Gold):** Aggregates data and engineers a new boolean machine learning feature (`is_premium_trip`) to flag high-value rides over $50.

## DevOps Integration (CI/CD)
This repository is configured with a continuous deployment pipeline using GitHub Actions. Any commits pushed to the `main` branch automatically trigger a workflow that authenticates and deploys the updated PySpark logic directly to the Databricks production environment via the Databricks REST API.

## Proof of Execution
Below are the results of the automated Databricks Job run and the final engineered Gold table output.

*(Pipeline Success)*
![Job Success](Screenshot%202026-04-25%20142834.jpg)

*(Gold Table Output)*
![Data Output](Screenshot%202026-04-25%20143631.png)
