# Golden Customer Record — Google Colab Submission

## Overview

This submission creates a **Golden Customer Record** in **PySpark** from two input sources:

- `crm_customers.csv` — CRM customer profile data
- `transaction_customers.csv` — transaction customer data

The solution is implemented as a **Google Colab notebook** using a **Medallion Architecture** pattern:

- **Bronze**: raw ingestion
- **Silver**: cleansing, standardization, deduplication, and reconciliation
- **Gold**: final curated customer dataset

The notebook reads the input files from the Colab file system and writes the final outputs back to `/content`.

---

## Notebook

Main notebook:
- `golden_customer_project.ipynb`

The notebook is self-contained and is intended to be run directly in **Google Colab**.

---

## Input Files

Place the following files in Colab before running the notebook:

- `/content/crm_customers.csv`
- `/content/transaction_customers.csv`
- `/content/ABOUT_DATA.md`

---

## Architecture

### Bronze Layer
Reads the raw CSV files exactly as received.

**Purpose**
- preserve raw source data
- inspect the input structure
- establish a clear source boundary

### Silver Layer
Applies cleansing and reconciliation logic.

**Key processing steps**
- normalize email, phone, text, and country values
- parse mixed date formats
- remove exact duplicates
- deduplicate records within each source using recency rules
- reconcile customers using exact email matches
- apply controlled fallback matching using:
  - first name
  - last name
  - city
  - country

Fallback matching is only used when:
- email is missing
- the fallback key is unique on both sides

### Gold Layer
Builds the final Golden Customer Record.

**Output columns include**
- `golden_customer_id`
- `crm_customer_id`
- `first_name`
- `last_name`
- `email`
- `phone`
- `address`
- `city`
- `country`
- `registration_date`
- `first_purchase_date`
- `last_purchase_date`
- `transaction_count`
- `match_rule`

---

## Matching Strategy

### Primary match
The notebook first matches records using **normalized email** because it is the strongest available identifier in this dataset.

### Fallback match
If email is missing, the notebook uses a fallback key built from:
- first name
- last name
- city
- country

This fallback path is deliberately conservative to reduce false-positive matches.

---

## Assumptions

The following assumptions were made:

1. **CRM is the primary profile source** for customer registration data.
2. **Transaction data enriches** the customer record with purchase history and missing fields.
3. Email is the most reliable customer match key when available.
4. Fallback matching is lower confidence and must be uniqueness-controlled.
5. Mixed date formats are expected and are parsed using multiple formats.

---

## Colab Run Steps

1. Open the notebook in **Google Colab**
2. Upload:
   - `crm_customers.csv`
   - `transaction_customers.csv`
   - `ABOUT_DATA.md`
3. Run the notebook cells from top to bottom
4. Confirm the notebook exports these folders:
   - `/content/gold_golden_customers`
   - `/content/gold_metrics`

---

## Output Paths

The notebook writes:

- Gold dataset:
  - `/content/gold_golden_customers`
- Metrics dataset:
  - `/content/gold_metrics`

Each folder contains a Spark-generated `part-*.csv` file.

---

## Validation / Tests in Colab

This submission includes a Colab-friendly test file:

- `/content/test_colab_outputs.py`

### Install test dependencies in Colab
```bash
!pip -q install pytest pandas pyyaml
```

### Run the tests
```bash
!pytest -q /content/test_colab_outputs.py
```

These tests validate that:
- the Gold output exists
- the Metrics output exists
- the Gold dataset is not empty
- `golden_customer_id` is unique
- `match_rule` values are valid
- required metric names exist
- `gold_total_records` matches the exported Gold row count

---

## Configuration

The configuration file for Colab is:

- `/content/colab_config.yaml`

It documents:
- input paths
- output paths
- matching rules
- validation expectations

This keeps the notebook submission aligned with a more production-style structure while remaining Colab-only.

---

## Files Included

- `golden_customer_project.ipynb`
- `README.md`
- `config/colab_config.yaml`
- `tests/test_colab_outputs.py`

---

## Notes

This submission was intentionally kept **notebook-first** because the assignment was implemented in **Google Colab**.  
The README, config, and tests were added to make the notebook easier to review and closer to a production-quality submission.

The notebook was developed and tested in Google Colab and that the tests passed successfully.
