
# Amazon Sales End-to-End Data Lakehouse Pipeline (Medallion Architecture)

An automated, production-grade Data Engineering pipeline implementing the **Medallion Architecture** to ingest, transform, and load data into **data warehouse**.

---

## Tech Stack

- **Orchestration & Workflow:** Apache Airflow (Docker Compose)
- **Data Processing Engine:** Apache Spark / PySpark
- **Python Package Manager:** `uv`
- **Cloud Storage & Data Lake:** Google Cloud Storage (GCS)
- **Data Warehouse:** Google BigQuery
- **Connectors:** `spark-bigquery-connector`, `gcs-connector`

## System Architecture

This project implements a **Hybrid Execution Model** designed to optimize local machine resources while maintaining containerized orchestration:
*   **Orchestration Layer:** Apache Airflow runs inside an isolated **Docker** environment.
*   **Processing Layer:** Instead of bloating the Airflow container with JVM and Spark dependencies, Airflow securely connects to the local host machine via **SSH (`SSHOperator`)** and triggers the PySpark transformation jobs managed by **`uv`**.

```text
+------------------------+       SSH (Port 22)        +--------------------------+
|  Airflow (Docker)      | -------------------------> |  Local Host (Windows)    |
|  - Scheduler           |                            |  - PySpark Engine        |
|  - SSHOperator         |                            |  - Python Env (uv)       |
+------------------------+                            +--------------------------+
                                                                    |
                                                                    | Spark-BigQuery / GCS
                                                                    v
                                                      +--------------------------+
                                                      |  Google Cloud Platform   |
                                                      |  - GCS (Data Lake)       |
                                                      |  - BigQuery (Data Warehouse)        |
                                                      +--------------------------+
```
## Medallion Architecture (Data Layers)

| Layer | Storage / Service | Processing Logic & Responsibilities |
|---|---|---|
| **Bronze** | GCS (Raw) | Stores raw, immutable data ingested from source systems (CSV/JSON/Parquet) without any structural modifications. |
| **Silver** | GCS / BigQuery (Staging) | Data cleansing and schema enforcement via **PySpark**: handling missing values, filtering invalid records, deduplication, and type casting. |
| **Gold** | BigQuery (DWH) | Business-ready data modeled into Fact and Dimension tables, optimized for analytical queries and BI dashboards. |

## Project Structure

```text
Amazon_sales_project/
│
├── dags/
│   ├── sales_orchestrate.py          # Main Airflow DAG orchestrating Bronze -> Silver -> Gold
│   └── ...
│
├── spark/
│   ├── spark_gcs.py                  # PySpark script for Silver layer transformations & BigQuery sink
│   └── spark_gcs.ipynb
│
├── lib/                              # Spark external dependencies (Jars)
│   ├── gcs-connector-hadoop3-*.jar
│   └── spark-4.1-bigquery-*.jar
│
├── docker-compose.yaml               # Docker Compose configuration for Apache Airflow
└── README.md                         # Project documentation
```

## Setup & Installation

### 1. Prerequisites

- **Docker & Docker Desktop** installed and running.
- **OpenSSH Server** enabled and configured on the local host machine.
- **uv** and **Python** installed on the local machine.
- A **Google Cloud Platform (GCP)** Service Account with *BigQuery Admin* and *Storage Object Admin* roles.

### 2. Local Environment Setup (with `uv`)

Clone the repository and synchronize dependencies using `uv`:
```bash
git clone https://github.com/Dayladang/Amazon_sales_project.git
cd Amazon_sales_project

# Install project dependencies into a local .venv
uv sync
```

### 3. Airflow Configuration (Docker to Host SSH Connection)

1. Launch the Airflow cluster:

```bash
   docker compose up -d
```

2. Open the Airflow Web UI at `http://localhost:8080`.

3. Navigate to **Admin → Connections → Add a new record**:
   - **Connection ID:** `my_local_machine_ssh`
   - **Connection Type:** `SSH`
   - **Host:** `host.docker.internal` *(Allows Docker containers to reach the host network)*
   - **Username / Password:** SSH credentials of the local host machine.
   - **Port:** `22`

4. Running the Pipeline
    - In the Airflow UI, unpause the sales_orchestrate DAG and click Trigger DAG.
    - Airflow will connect to the local host via SSH, execute uv run python scripts/spark_gcs.py, process the staging data from GCS, and load the cleaned dataset directly into Google BigQuery.