# GCP Healthcare Data Pipelines 
This repository contains an end-to-end ELT (Extract, Load, Transform) pipeline designed to ingest, validate, and transform synthetic healthcare data (Synthea) on Google Cloud.

The architecture follows a Metadata-Driven approach, where a central control table tracks the lifecycle of every data file from ingestion to the final serving layer. It utilizes a 4-Layer Medallion Architecture (Bronze, Silver, Gold, Serving) to ensure data quality and governance.

## Repository Structure 
```
.
├── bigquery/               # BigQuery definitions and SQL
│   ├── clinical/           # Domain-specific logic (Synthea)
│   │   └── synthea/
│   │       ├── bronze/     # Bronze layer definitions
│   │       │   ├── contracts/  # JSON Schema definitions for validation
│   │       │   └── tables/     # DDL for Bronze tables
│   ├── ops/                # Operational metadata & audit logs
│   │   └── tables/         # Metadata table definitions (e.g., file_ingestion_audit)
│   └── scripts/            # Python scripts for schema validation
├── cloud_functions/        # Event-driven serverless functions
│   ├── csv_validator/      # Triggers on GCS upload to validate headers
│   └── json_validator/     # Validates JSON structure
├── dataflow/               # Apache Beam pipelines
│   └── file-counter/       # (Example) Processing pipeline logic
└── orchestration/          # Apache Airflow DAGs (Self-hosted)
```

```Mermaid
graph TD
    subgraph Ingestion ["Ingestion & Validation"]
        GCS["Cloud Storage<br/>(Landing Zone)"] -->|Finalize Event| CF["Cloud Functions<br/>(Schema Validator)"]
        CF -->|Update Status: VALIDATED| META[("Metadata Table")]
    end

    subgraph Processing ["Processing (Bronze)"]
        AF["Airflow (VM)"] -->|Trigger| DF["Dataflow<br/>(Apache Beam)"]
        DF -->|Read| GCS
        DF -->|Load| BQ_B[("BigQuery<br/>Bronze Layer")]
        DF -->|Update Status: LOADED| META
    end

    subgraph Transformation ["Transformation (Silver/Gold)"]
        AF -->|Trigger| DBT["dbt"]
        DBT -->|Read| BQ_B
        DBT -->|Transform| BQ_S[("BigQuery<br/>Silver Layer")]
        DBT -->|Transform| BQ_G[("BigQuery<br/>Gold Layer")]
        DBT -->|Create| BQ_V["Serving Layer<br/>(Views)"]
        DBT -->|Update Status: TRANSFORMED| META
    end
```

## The Metadata Framework
## The Metadata Framework (Shared Governance)
To ensure strict separation of duties and centralized governance, all operational metadata and data contracts are stored in a dedicated **Shared Governance Project** (separate from the compute/storage project).

* **Centralized Contracts:** The contracts are deployed in a central project.
   * Pre Dataflow Validation - The JSON schemas are used by cloud functions to do preliminary checks including Schema Drift. 
   * Dataflow Validation - YAML contracts for In depth validation / cleanup. 
* **Unified Audit Log:** The `file_ingestion_audit` table resides in the shared project's `ops` dataset. This allows for a single pane of glass to monitor data quality across multiple domain pipelines.

| Stage | Component | Action | Status Code |
| :--- | :--- | :--- | :--- |
| **1. Landing** | GCS | File arrives in bucket | `ARRIVED` |
| **2. Validation** | Cloud Function | Checks CSV vs **Shared Contract** | `VALIDATED` / `FAILED` |
| **3. Loading** | Dataflow | Ingests data to Bronze Table | `LOADED_BRONZE` |
| **4. Transform** | dbt | Cleans and aggregates to Silver/Gold | `PROCESSED` |

## Setup & Deployment

## Orchestration
The pipeline is orchestrated using **Apache Airflow**, self-hosted on a Google Compute Engine (GCE) VM to minimize overhead while maintaining full control over the environment. Terraform in the  landing-zone project. [GCP Landing Zone](https://github.com/aazad612/using-azure-devops-gcp-landing-zone)

* **Workflow Strategy:** We utilize a "Sensor-Based" pattern. Airflow DAGs continuously poll the **Metadata Table** (in the shared project) for files marked as `VALIDATED`.
* **Process Flow:**
    1.  **Bronze Loading:** Once a file is validated, Airflow triggers a **Dataflow** job to load the data into the Bronze layer.
    2.  **Transformation:** Upon successful load, Airflow triggers **dbt** models to process data into Silver and Gold layers.
* **Infrastructure:** The Airflow VM is deployed within a private VPC, utilizing Cloud NAT for secure external access when required.

### SQL Deployment (CI/CD)
SQL definitions in `bigquery/` are deployed via **GitHub Actions**. The pipeline detects changes in `.sql` or `.json` files and applies them to the respective datasets.

### Local Development
1.  **Ignore Artifacts:** This repo ignores `*.csv`, `*.sh`, and `archived_old/` to keep the source clean.
2.  **Validation Testing:**
    ```bash
    # Run schema validation script locally
    python bigquery/clinical/synthea/scripts/validate_schema.py --file data/sample.csv
    ```

## Future Roadmap
* [ ] Implement **Great Expectations** for deeper data quality checks in the Airflow DAG.
* [ ] Finalize Airflow DAGs in `orchestration/` folder.
* [ ] DBT for Silver and Bronze.
