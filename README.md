# 🚢 Red Sea Risk Radar: A Polyglot Data Pipeline

### Quantifying the "Lag Effect" of Geopolitical Conflict on Global Logistics

---

## 📖 Project Overview

Supply chains are vulnerable to geopolitical shocks, but the financial impact often **lags** behind the actual events.  
**Red Sea Risk Radar** is an end-to-end data engineering pipeline designed to **quantify this latency**.

By correlating **unstructured geopolitical news signals** (from [GDELT](https://www.gdeltproject.org/)) with **structured financial logistics data** (Shipping Rates), this project empirically demonstrates a **10–14 day lag** between high-intensity conflict events and subsequent spikes in global shipping costs.

---

## ⚙️ Key Features

- **Polyglot Persistence**: Harmonizes data across `MongoDB` (NoSQL) for unstructured news and `SQL Server` (RDBMS) for structured financial data.  
- **Hybrid Ingestion**: Combines automated API mining (GDELT) with manual ETL for financial data.  
- **Big Data Processing**: Utilizes `Apache Spark` for schema enforcement, temporal upsampling, and large-scale distributed joins.  
- **Containerized Architecture**: Completely encapsulated via `Docker`, ensuring reproducibility across any environment.  
- **Interactive Analytics**: A “Control Tower” dashboard built with `Streamlit` and `Plotly` for executive-level insight.

---

## 🏗️ Architecture

The pipeline follows a **Lakehouse-inspired architecture** orchestrated by **Dagster**.

```text
[ EXTERNAL SOURCES ]
       |
       v
[ DOCKER ENVIRONMENT ]
|
+---> 1. ORCHESTRATION (Dagster)
|     - Triggers Ingestion Scripts
|     - Triggers Spark Jobs
|
+---> 2. STORAGE LAYER
|     - MongoDB (Stores Raw News JSON)
|     - SQL Server (Stores Structured Rates)
|     - Postgres (Stores Dagster Metadata)
|
+---> 3. COMPUTE LAYER (Apache Spark)
|     - Reads from Mongo & SQL
|     - Joins Daily News with Hourly Rates
|     - Writes "Gold" Data to SQL Server
|
+---> 4. PRESENTATION LAYER (Streamlit)
      - Reads "Gold" Data from SQL Server
      - Displays Interactive Dashboard
The pipeline follows a **Lakehouse-inspired architecture** orchestrated by **Dagster**.
```

## 📂 File Structure
```text
RedSea_Project/
│
├── docker-compose.yaml # Orchestrates services (Dagster, SQL, Mongo, Dashboard)
├── Dockerfile # Defines main Python/Java environment
├── dagster.yaml # Dagster configuration (Postgres-backed)
├── requirements.txt # Python dependencies
│
├── jobs/ # [Pipeline Code]
│ ├── repo.py # Main pipeline logic (Assets & Spark)
│ ├── mine_gdelt_final.py # GDELT API mining script
│ ├── upsample_rates_only.py # Linear interpolation for rates
│ ├── rates.csv # Raw weekly rates
│ ├── historical_news_raw.csv # Output from GDELT miner
│ └── upsampled_rates.csv # Output from upsampler
│
└── dashboard/ # [Visualization App]
├── Dockerfile # Streamlit-specific environment
└── dashboard_app.py # Streamlit dashboard code
```

---

## 🚀 Reproducibility Guide (How to Run)

The project is fully **containerized** — no need to install Spark, SQL Server, or Mongo locally.

### Prerequisites

- Docker Desktop installed and running.

---

### Step 1: Initialize the Environment

Start the full infrastructure (databases, pipeline engine, dashboard):

docker-compose up -d


Wait ~2 minutes for SQL Server to fully initialize.

---

### Step 2: Data Acquisition (One-Time Setup)

Generate datasets inside the container.

**Mine real conflict news:**
docker exec -it redsea_processor python jobs/mine_gdelt_final.py

_Output: ~3,000 rows of conflict news from GDELT._

**Upsample weekly rate data:**
docker exec -it redsea_processor python jobs/upsample_rates_only.py

_Output: ~2,800 rows of hourly shipping rates._

---

### Step 3: Execute the Pipeline (Dagster)

Open Dagster UI:
[http://localhost:3000](http://localhost:3000)

- Click **Assets** → **Materialize All**  
- Dagster will ingest, transform, and load the data into the **"Gold" table**

---

### Step 4: Launch the Dashboard

When all assets have materialized, open:
[http://localhost:8501](http://localhost:8501)

---

## 📊 Results & Findings

| Metric | Result | Description |
|--------|---------|-------------|
| **Lag Correlation** | r = 0.52 | Correlation between Risk Score (News × Tone) and Shipping Prices |
| **Financial Impact** | Shipping peak: \$8,506 | 400%+ surge vs pre-crisis baseline |
| **Signal Lead** | 10–14 days | Conflict news preceded price surges |

> High-intensity “Red Bar” news clusters consistently appeared 10–14 days before the corresponding “Blue Line” price spikes — confirming news sentiment as a leading economic indicator.

---

## 🛠️ Technical Challenges & Solutions

### 1. Dependency Hell

- **Problem:** Spark 3.5 requires Java 17, but standard Python images bundle older versions.
- **Solution:** Built a **custom Debian 12 Docker image**, installed **OpenJDK 17**, and dynamically fetched `mongo-spark-connector:10.4.0` for version alignment.

### 2. Granularity Mismatch

- **Problem:** Joining **daily** news with **weekly** rates caused data loss.
- **Solution:** Implemented **linear interpolation** to upsample weekly rates into **hourly time-series data**, ensuring fidelity in temporal joins.

---

## 📜 License

- **Data:** GDELT open data terms

---


