# 🎬 StreamPulse — Real-Time Streaming Analytics Pipeline

![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoft-azure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=fastapi&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat&logoColor=white)

## 📌 Project Overview

StreamPulse is an end-to-end real-time data engineering 
pipeline that simulates a video streaming analytics platform 
similar to Netflix or Amazon Prime. 

The project processes live viewing events through a modern 
cloud data architecture — from event generation to 
analytics-ready Star Schema — using the Azure data stack.

---

## 🏗️ Architecture
```
┌─────────────────────────────────────────────────────┐
│                  DATA SOURCES                        │
│                                                      │
│  FastAPI App          GitHub JSON File               │
│  (Live Events)        (Historical Data)              │
└──────────┬───────────────────┬──────────────────────┘
           │                   │
           ▼                   ▼
┌──────────────────┐  ┌────────────────────┐
│  Azure Event     │  │  Azure Data Lake   │
│  Hubs (Kafka)    │  │  Gen2 Storage      │
└──────────┬───────┘  └────────┬───────────┘
           │                   │
           └─────────┬─────────┘
                     ▼
┌─────────────────────────────────────────────────────┐
│              AZURE DATABRICKS                        │
│                                                      │
│  🥉 Bronze Layer    Raw data ingestion               │
│     ├── bulk_history (1000 historical records)       │
│     └── stream_events (live watch events)            │
│                                                      │
│  🥈 Silver Layer    Cleaned and enriched OBT         │
│     └── watch_obt (cleaned + 5 derived columns)      │
│                                                      │
│  🥇 Gold Layer      Star Schema (Delta Live Tables)  │
│     ├── fact_watch_events                            │
│     ├── dim_content                                  │
│     ├── dim_users                                    │
│     ├── dim_devices                                  │
│     ├── dim_locations                                │
│     └── dim_date                                     │
└─────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────┐
│              ORCHESTRATION & AUTOMATION              │
│  Azure Data Factory    Pipeline orchestration        │
│  GitHub Actions        CI/CD auto deployment         │
│  Azure Key Vault       Secrets management            │
│  Jinja Templates       Dynamic SQL (9 reports)       │
└─────────────────────────────────────────────────────┘
```

---

## 🛠️ Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Web App | FastAPI + Uvicorn | Simulate streaming events |
| Messaging | Azure Event Hubs (Kafka) | Real-time event streaming |
| Storage | Azure Data Lake Gen2 | Raw and processed data |
| Compute | Azure Databricks | Data processing |
| Format | Delta Lake | ACID transactions |
| Pipeline | Delta Live Tables | Declarative pipelines |
| Orchestration | Azure Data Factory | Workflow automation |
| Security | Azure Key Vault | Secrets management |
| CI/CD | GitHub Actions | Auto deployment |
| Templates | Jinja2 | Dynamic SQL |
| Language | Python + PySpark + SQL | Data processing |

---

## 📁 Project Structure
```
StreamPulse-Data-Engineering/
│
├── .github/
│   └── workflows/
│       └── deploy.yml          CI/CD pipeline
│
├── data/
│   └── streaming_history.json  1000 historical records
│
├── screenshots/
│   ├── Databricks_Workspace_5.jpg
│   ├── The_Data_Lineage__Databricks_DLT_.jpg
│   └── The_Orchestration_Masterpiece__ADF_.jpg
│
├── 01_bronze_ingestion.py      Raw data ingestion
├── 02_silver_transformation.py Data cleaning
├── 03_gold_star_schema.py      Star Schema creation
├── 04_delta_live_tables.py     DLT pipeline
├── 05_data_quality.py          Quality checks
├── connection.py               Azure Event Hubs connection
├── data.py                     Watch event generator
├── generate_bulk_data.py       Bulk data generator
├── jinja_templates.py          Dynamic SQL templates
├── main.py                     FastAPI web application
└── requirements.txt            Python dependencies
```

---

## 🌊 Data Flow

### Source 1 — Bulk Historical Data
```
GitHub JSON (1000 records)
        ↓
pandas read_json
        ↓
Bronze Delta Table (bulk_history)
```

### Source 2 — Live Streaming Events
```
User opens StreamPulse app
        ↓
Clicks Watch Now button
        ↓
FastAPI generates watch event
        ↓
Azure Event Hubs (Kafka)
        ↓
Databricks Structured Streaming
        ↓
Bronze Delta Table (stream_events)
```

### Processing Pipeline
```
Bronze (raw)
        ↓
Silver (cleaned + enriched OBT)
        ↓
Gold (Star Schema via DLT)
        ↓
Analytics Views + Jinja Reports
```

---

## ⭐ Star Schema Design
```
                dim_content
                content_id
                title, genre
                     │
dim_users ───────────┤
user_id              │   fact_watch_events
age_group            ├── event_id (PK)
subscription         │   user_id (FK)
                     │   content_id (FK)
dim_devices ─────────┤   device_type (FK)
device_type          │   city (FK)
device_os            │   date (FK)
device_category      │   watch_duration_mins
                     │   completion_pct
dim_locations ───────┤   engagement_score
city                 │   revenue_per_view
state                │   is_completed
region               │   is_prime_time
                     │
dim_date ────────────┘
date, year, month
day_of_week
is_weekend
```

---

## ⚡ Delta Live Tables Pipeline

**Pipeline:** `StreamPulse_DLT`  
**Schema:** `streampulse_databricks.streampulse_dlt`  
**Status:** ✅ Completed in **38 seconds**

| Table | Rows | Type |
|-------|------|------|
| silver_watch_obt | 1,000 | Source |
| dim_content | 1,030 | Dimension |
| dim_users | 3,988 | Dimension |
| dim_devices | 1,000 | Dimension |
| dim_locations | 1,024 | Dimension |
| dim_date | 1,000 | Dimension |
| fact_watch_events | 4,000 | Fact |
| gold_content_performance | 1,010 | Gold Agg |
| gold_user_engagement | 2,004 | Gold Agg |
| gold_device_analytics | 2,001 | Gold Agg |
| gold_revenue_summary | 2,004 | Gold Agg |
| gold_hourly_pulse | 1,000 | Gold Agg |
| gold_trending_content | 10 | Gold Agg |
| **Total** | **13 tables** | |

---

## 🔄 ADF Pipeline — Orchestration

**Pipeline:** `StreamPulse_Pipeline`  
**Factory:** `StreamPulse-adf`  
**Status:** ✅ All activities Succeeded

```
Bronze_Ingestion ──► Silver_Transformation ──► Gold_Star_Schema
    ✅ 38s                  ✅ 36s                   ✅ 40s
```

---

## 🔄 CI/CD Pipeline

Every push to `master` branch automatically:
```
Push to GitHub
        ↓
GitHub Actions triggered
        ↓
Job 1: Test code quality
        ↓
Job 2: Deploy notebooks to Databricks
        ↓
Job 3: Verify deployment
        ↓
✅ Pipeline ready to run
```

---

## 🎯 Key Features

**Real-time Streaming**  
Live watch events flow from FastAPI app through
Azure Event Hubs into Databricks streaming pipeline.

**Dual Ingestion**  
Handles both bulk historical data and live streaming
events — combining them in Bronze layer.

**Medallion Architecture**  
Bronze → Silver → Gold with increasing data quality
and decreasing data volume at each layer.

**Star Schema Modeling**  
Production-grade dimensional model with 1 fact table
and 5 dimension tables built using Delta Live Tables.

**Data Quality**  
Automated quality checks across all layers including
referential integrity validation.

**Dynamic SQL**  
9 analytical reports generated from 4 Jinja templates
and metadata config — no SQL repetition.

**Security**  
All secrets managed through Azure Key Vault.
No credentials in code or GitHub.

**CI/CD**  
Notebooks automatically deployed to Databricks
on every GitHub push to master.

---

## 🚀 How To Run

### Prerequisites
- Azure subscription
- Azure Databricks workspace
- Azure Event Hubs namespace
- Azure Data Lake Gen2
- Azure Key Vault
- Python 3.12+

### Local Setup
```bash
# Clone repository
git clone https://github.com/khushboo-1991/StreamPulse-Data-Engineering.git

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure secrets
# Add your Azure credentials to .env file

# Generate bulk historical data
python generate_bulk_data.py

# Run StreamPulse app
python main.py
```

### Access StreamPulse App
```
Home page:    http://localhost:8000
Watch event:  http://localhost:8000/watch
API docs:     http://localhost:8000/docs
Health check: http://localhost:8000/health
```

### Run Databricks Notebooks
```
1. Upload notebooks/ folder to Databricks workspace
2. Run in order:
   01_bronze_ingestion.py
   02_silver_transformation.py
   03_gold_star_schema.py
   04_delta_live_tables.py
   05_data_quality.py
```

---

## 📊 Sample Analytics

**Top Content By Views:**
```sql
SELECT c.content_title, COUNT(*) as views
FROM fact_watch_events f
JOIN dim_content c ON f.content_id = c.content_id
GROUP BY c.content_title
ORDER BY views DESC
```

**Prime Time Analysis:**
```sql
SELECT time_of_day,
       COUNT(*) as views,
       AVG(completion_pct) as avg_completion
FROM fact_watch_events
GROUP BY time_of_day
ORDER BY views DESC
```

---

## 📸 Screenshots

| Component | Screenshot |
|-----------|-----------|
| Databricks Workspace | ![Workspace](screenshots/Databricks_Workspace_5.jpg) |
| DLT Pipeline — Data Lineage | ![DLT](screenshots/The_Data_Lineage__Databricks_DLT_.jpg) |
| ADF Pipeline — All Succeeded | ![ADF](screenshots/The_Orchestration_Masterpiece__ADF_.jpg) |

---

## 🔗 Related Projects

- [APAC Streaming Strategy Analytics](https://github.com/khushboo-1991/apac-streaming-strategy-analytics)  
  SQL and Python analysis of Netflix, Amazon Prime
  and JioHotstar competitive positioning in India.

---

## 👩‍💻 About

Built as a portfolio project demonstrating
end-to-end Azure data engineering skills including
real-time streaming, medallion architecture,
Star Schema modeling, and modern DevOps practices.

**Skills demonstrated:**  
PySpark • Delta Lake • Azure Databricks •
Azure Event Hubs • Azure Data Factory •
Delta Live Tables • GitHub Actions •
Jinja Templates • FastAPI • Star Schema

---

*Built with ❤️ by Khushboo Patel — April 2026*
