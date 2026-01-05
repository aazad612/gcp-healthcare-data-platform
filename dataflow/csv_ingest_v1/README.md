# Bronze Layer Ingestion Engine: Metadata-Driven Framework

The primary goal of this bronge lawyer ingestion framework is to efficiency by making it metadata driven and putting guardrails that avoid rework. There are 2 guardrails in place before the dataflow comes into picture. 

While the development of this project is kicked off with CSV due to its popularily, Parquet and Avro would be added for other use cases. 

| Ingestion Method | Format | Throughput (MBps) | Throughput (Elements/s) |
| :--- | :--- | :--- | :--- |
| **Parquet Load** | Parquet | ~90 - 110 MBps | ~88,000 - 105,000 |
| **Avro Load** | Avro | ~78 MBps | ~77,000 |
| **CSV Load** | CSV | ~65 - 70 MBps | ~64,000 - 68,000 |
| **Storage Write API** | Stream | ~55 MBps | ~54,000 |
| **JSON Load** | JSON | ~54 MBps | ~53,000 |

## Pre-Dataflow guardrails 

### 1. Table creation
Table creation is handled by github actions which validate the schema for standards and partitioning/clustering + meta column existence. 

* [Table Creation Standards](../../bigquery/ops/tables/standards_definition.sql)
* [Table Creation Standards-seed](../../bigquery/ops/seeds/bronze_v1_standards.sql)
* [Validate Schema script](../../bigquery/scripts/validate_schema.py)

### 2. Cloud function
Triggered when a file arrives in the GCS bucket. The first 3 files are generic and would be used for other file formats. csv_validator is code specific to CSV files validation.
* [context.py](../../cloud_functions/csv_validator/context.py) - Common metadata
* [main.py](../../cloud_functions/csv_validator/main.py) - mainly just does the execution control
* [steps.py](../../cloud_functions/csv_validator/steps.py) - generic steps and are to be used elsewhere
* [csv_validator.py](../../cloud_functions/csv_validator/csv_validator.py)  is specific to CSV validation. 
* (To be added: Parquet, and AVRO).


#### Validation steps:
* **a. File path validation**
* **b. Sample row validation** - top N rows validated, specified in the [file_ingestion_mapping](/../../bigquery/ops/tables/file_ingestion_mapping.sql) to be dynamic.
    * Header Validation 
    * Unicode characters 
    * Delimiter check 
    * Binary / null bytes 
    * *To be added:* datatype validation 
* **c. Schema Drift** * Based on simple JSON contracts which include only business columns.
    * The JSON contracts are automatically generated from YAML contracts.
    * *To be added:* Acceptable vs. Unacceptable drift thresholds.
* **d. Audit entry** - Created to be used by notification channels, Dataflow, and Airflow. 
* **e. Dataflow Trigger** - (Mostly will be dropped off) - Directly kick off the Dataflow job upon successful validation.


## Data Flow Job for CSV files

### 🏗️ Architectural Guardrails (Metadata Roles)
The [metadata.py](./metadata.py) module aggregates four critical sources of truth that drive the pipeline's behavior:

### 1. File Ingestion Mapping Table (The Orchestrator)
This BigQuery table is the **Entry Guardrail**. If an entity is not registered here, the engine ignores it.
* **Role:** Maps the combination of `domain`, `unit`, and `table_name` to specific GCS configuration paths and DLQ policies.
* **Enforcement:** Controls environment-specific routing and ensures every load is tied to a specific version of a governance standard.

[Ingestion Mapping](../../bigquery/ops/tables/file_ingestion_mapping.sql)

### 2. Ingestion Audit / Target State (The Lineage Tracker)
The target BigQuery table itself acts as the **State Guardrail**. 
* **Role:** The pipeline performs a pre-flight `MAX(meta_batch_id)` query on this table to determine the next atomic batch. 
* **Enforcement:** Prevents lineage gaps and ensures that every ingestion job is incremented based on the actual successful state of the database.

[Ingestion Audit](../../bigquery/ops/tables/file_ingestion_audit.sql)

### 3. YAML Ingestion Contracts (The Physical Rulebook)
Stored in GCS, these files serve as the **Physical Guardrail**. 
* **Role:** They define the authoritative column order, data types, required modes (NULLABLE/REQUIRED) and list of valid values where applicable. 
* **Enforcement:** Used directly by the `ValidateAndParse` logic to perform schema drift detection and type-casting validation before data ever touches BigQuery.

[sample YAML contract](../../bigquery/clinical/synthea/bronze/ingestion_configs/conditions_v1.yaml)

### 4. JSON Data Contracts (The Integration Standard)
These are machine-readable definitions derived from the YAML contracts. Stored only in GCS now as they are automatically generated. 
* **Role:** They provide a single source of truth for downstream systems like Dataform and automated documentation tools.
* **Enforcement:** Ensures that the analytical layer and the ingestion layer are perfectly synchronized regarding the expected schema of the Bronze layer.

[sample JSON contract](../../bigquery/clinical/synthea/bronze/contract/conditions_sample.json)

---

## 🚀 Pipeline Execution Engine (`main.py`)
The [main.py](./main.py) script is the core executor that transforms these guardrails into a running Apache Beam graph.

### **1. Pre-Flight Validation & Schema Generation**
Before the pipeline begins, `main.py` executes `validate_target_compliance` to ensure the BigQuery target is structuraly ready. It then invokes `generate_bq_schema`, which creates a dynamic JSON schema hint. This hint is critical for the Storage Write API to handle temporal fields (Dates/Timestamps) as strings to avoid SDK serialization crashes.

### **2. ValidateAndParse (The Enforcement DoFn)**
This is the heart of the pipeline. It processes each file and performs:
* **Drift Check:** Reads the header and compares it to the contract. On failure, it tags the file as `SCHEMA_DRIFT` and diverts it.
* **Type Validation:** Each row value is passed through `validate_value` to ensure it matches the Contract Data Types (e.g., Integer, Float, ISO-Date).
* **Branching:** Uses side-outputs (`TAG_VALID`, `TAG_INVALID`) to ensure bad rows are separated without stopping the job.

### **3. Provider Registry (Decoupled Governance)**
`main.py` implements the `get_meta_provider_registry`. This pattern separates the **logic** of metadata generation (UUIDs, Batch IDs, Timestamps) from the **assignment** to columns. This allows the governance standards to add or rename `meta_` columns without requiring changes to the core processing logic.

### **4. Storage Write API & DLQ Sinks**
The pipeline uses the **Storage Write API** for all BigQuery sinks. For a more mature pipeline the free FILE_LOADS can be used to reduce costs. For reducing latency use STORAGE_API_AT_LEAST_ONCE (testing required). 
* **Success Sink:** Writes valid rows to the Bronze target.
* **Error Capture:** Uses the `FailedRows` attribute of the Storage Write API to catch rows that passed initial validation but failed BigQuery's internal constraints (e.g., partition violations).
* **Flattened DLQ:** Combines validation failures and BigQuery failures into a single stream for dual-output to **BigQuery `{table}_bad`** and **GCS JSON errors**.
* Tobe added - **Pubsub Sink**, Archival, error segregation, STORAGE_API_AT_LEAST_ONCE testing, scaling based on file_size and priority. 

### 5. Stateful Batch Management (Atomic Lineage)
Lineage and auditability are managed through stateful tracking, ensuring that every row can be traced back to a specific ingestion event.
* **Dynamic High-Water Mark:** Instead of hardcoding batch IDs, the engine queries the target table at runtime to calculate `MAX(meta_batch_id) + 1`.
* **Temporal Stability:** A consistent `job_timestamp` is established at the start of the job and used across all valid and invalid rows, ensuring an atomic timeline for the entire batch.

---

## Other Enhancements 
* **Stable Serialization:** Maps all temporal types to `STRING` in the schema hint to bypass the Beam Python SDK `.micros` bug.
* **VPC-SC Readiness:** Strictly targets `WorkerOptions` for networking (Subnetworks, Internal IPs) to ensure compliance in locked-down GCP environments.

---

## 📋 Execution Guide
```bash
python main.py \
  --domain clinical \
  --unit synthea \
  --table_name conditions \
  --env dev \
  --runner DataflowRunner
```

