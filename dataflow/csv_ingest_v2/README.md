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

## 🎯 Design Objectives

* Enforce **Bronze-layer governance**, not raw dumping
* Fail early using **pre-ingestion guardrails**
* Keep **business rules out of pipeline code**
* Support **multi-sink fan-out** from a single parse
* Enable **safe retries, replay, and audit**
* Prepare for **Flex Template–based deployment** (in progress)


---

## 🎯 Design Objectives

* Enforce **Bronze-layer governance**, not raw dumping
* Fail early using **pre-ingestion guardrails**
* Keep **business rules out of pipeline code**
* Support **multi-sink fan-out** from a single parse
* Enable **safe retries, replay, and audit**
* Prepare for **Flex Template–based deployment** (in progress)

---

## 🧱 Pre-Dataflow Guardrails (Control Plane)

Before Dataflow is ever invoked, **two mandatory guardrails** ensure that only valid, governable data reaches distributed compute.

These guardrails are **not optional** and are treated as part of the ingestion contract.

---

### 1. Table Creation & Standards Enforcement

All Bronze tables are created and validated **ahead of ingestion** via CI/CD.

**Responsibilities:**
* Enforce partitioning & clustering standards
* Validate presence and structure of required `meta_*` columns
* Prevent schema drift from being introduced implicitly

**Artifacts:**
* [Standards Definition](../../bigquery/ops/tables/standards_definition.sql)
* [Standards Seeds](../../bigquery/ops/seeds/bronze_v1_ddl_standards.sql)
* [Schema Validation Script](../../bigquery/scripts/validate_schema.py)

No Dataflow job is allowed to create or mutate tables.

---

### 2. Cloud Function Validation (Ingress Control)

A Cloud Function is triggered when a file arrives in the landing GCS bucket.

Its responsibility is to **validate files before Dataflow** is allowed to run.

**Shared Components:**
* [context.py](../../cloud_functions/csv_validator/context.py) – shared metadata
* [main.py](../../cloud_functions/csv_validator/main.py) – execution control
* [steps.py](../../cloud_functions/csv_validator/steps.py) – reusable validation steps

**CSV-specific Logic:**
* [csv_validator.py](../../cloud_functions/csv_validator/csv_validator.py)

**Validation Steps:**
* File path validation
* Header validation
* Encoding & delimiter checks
* Unicode and binary character detection
* Sample row inspection (top-N, metadata-driven)
* Schema drift detection using JSON contracts
* Audit entry creation

Only validated files are eligible for Dataflow ingestion.

---

## 🏗️ Dataflow Architecture Overview

GCS (CSV files)
↓
Read & Normalize
↓
Contract Validation
↓
Metadata Annotation
↓
Deduplication
↓
Fan-out Sinks
├── BigQuery (Bronze)
├── GCS (archive / errors)
└── Pub/Sub (events)


The pipeline is **single-parse, multi-sink** by design.

---
## 🧠 Execution Model

The execution model is **explicit, modular, and intentionally decomposed**.  
There is no “magic” file — each module owns a **single responsibility** in the ingestion lifecycle.

The pipeline is currently invoked **per input file**, with all behavior resolved dynamically via metadata.

---

## 🧠 Execution Flow (File-by-File)

For a single `--input_file`, execution proceeds through the following layers:

main.py
↓
pipeline.py
↓
read_csv.py
↓
validation.py
↓
metadata_loader.py
↓
dedup.py
↓
(write_bq.py | write_gcs.py | write_pubsub.py)
↓
dlq.py



Each step is isolated, testable, and replaceable.

---

## 📄 Execution Components (Full Breakdown)

### [`main.py`](./main.py) — Runtime Orchestrator

Responsibilities:
* CLI argument parsing
* GCP project & ADC resolution
* Passing runtime context to the pipeline
* Launching the Beam job

**Does NOT contain:**
* Validation logic
* Schema rules
* Sink logic
* Metadata decisions

This file exists to **start execution, not define behavior**.

---

### [`pipeline.py`](./pipeline.py) — Beam Graph Assembly

Responsibilities:
* Constructing the Apache Beam graph
* Wiring transforms together
* Managing branching for valid vs invalid records
* Coordinating fan-out to sinks

This file defines **how data flows**, not **what rules apply**.

---

## 📥 Input Layer

### [`io/read_csv.py`](./io/read_csv.py) — File Reader

Responsibilities:
* Read CSV files from GCS
* Handle delimiter, encoding, and newline normalization
* Emit structured row dictionaries

**Guarantees:**
* No validation
* No schema enforcement
* No metadata logic

Pure ingestion only.

---

## 🧩 Governance & Enforcement Layer

### [`common/validation.py`](./common/validation.py) — Contract Enforcement

Responsibilities:
* Header validation
* Required vs nullable enforcement
* Datatype validation
* Schema drift detection
* Invalid character detection (WIP)

Outputs:
* Valid rows
* Invalid rows with failure reason

Validation **never stops the job** — it routes records.

---

### [`common/metadata_loader.py`](./common/metadata_loader.py) — Control Plane Adapter

Responsibilities:
* Load file ingestion mapping metadata
* Resolve target dataset/table
* Determine DLQ routing behavior
* Attach ingestion context to each record

This module bridges **BigQuery control tables** and **runtime execution**.

---

### [`common/dedup.py`](./common/dedup.py) — Deduplication Guardrail

Responsibilities:
* Generate `pk_hash`
* Enforce deduplication per batch
* Prevent duplicate row insertion

Current scope:
* Batch-level deduplication

Future scope:
* Cross-file windowed deduplication

---

## 📤 Sink Layer (Fan-out)

### [`io/write_bq.py`](./io/write_bq.py) — BigQuery Sink

Responsibilities:
* Write valid rows using Storage Write API
* Apply dynamic schema hints
* Capture BigQuery-internal failures

Failures are **not dropped** — they are forwarded to DLQ.

---

### [`io/write_gcs.py`](./io/write_gcs.py) — GCS Sink

Responsibilities:
* Persist invalid records
* Archive rejected rows
* Enable replay and debugging

All records are JSON-encoded with failure context.

---

### [`io/write_pubsub.py`](./io/write_pubsub.py) — Pub/Sub Sink

Responsibilities:
* Emit ingestion events
* Signal downstream systems
* Support partial-success workflows

Fully optional and decoupled.

---

## 🧯 Failure Normalization

### [`common/dlq.py`](./common/dlq.py) — DLQ Standardization

Responsibilities:
* Normalize all failure types into a single schema:
  * Validation failures
  * Schema drift
  * Dedup violations
  * Sink-level errors
* Route failures consistently across sinks

This guarantees **observability and replayability**.

---

## 📋 Execution Guide

```bash
# IF using my dot_profiles repo
# gset <profile_name>

# Otherwise
gcloud config set project prj-lbd-shared-np
gcloud auth application-default login

python main.py \
  --input_file "gs://bkt-.../incoming/clinical/20251229/synthea/patient-20251229.csv"
```





## Planned additions / new learnings


---

### Pytest
* Unit testing Beam `DoFn`s in isolation
* Validation logic correctness (positive and negative cases)
* Metadata-driven test fixtures
* Deterministic testing of edge cases (nulls, bad encodings, drift)

---
### 🔎 Great Expectations
* Declarative validation rules for common data-quality checks
* Reuse of expectations across ingestion and analytics layers
* Classification of failures into DLQ categories
* Runtime GX validation vs pre-ingestion enforcement
* Performance tradeoffs inside Beam
* Overlap vs redundancy with existing contract validation

---
### 🧱 Dataform or dbt
* Contract alignment between Bronze and Silver layers
* Schema documentation generation
* Column-level lineage and freshness indicators
* Ability to reuse ingestion contracts
* Operational complexity
* Fit with metadata-driven standards
* Team adoption and maintainability

---
The system must be designed to evolve without rewrites, schema chaos, or silent data loss.
