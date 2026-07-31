# Data Pipeline Workflow

## 1. Architecture Overview

The pipeline is orchestrated as a sequential workflow that transforms raw data into a clean, star schema model gold layer. It use PySpark and Delta Lake for scalable processing and ACID transactions.

### High-Level Flow:

1. Extraction Loop: Ingests raw data into the Bronze schema.
2. Silver Technical (silver_t): Standardizes and types the data.
3. Silver OBT (silver_b): Join silver tables into "One Big Table".
4. Data Quality Check: Validates data against business and technical rules.
5. Gold Processing: Simultaneously populates Dimensions (SCD Type 2) and Fact tables.
6. Maintenance: Optimizes Delta tables for performance and storage efficiency.

## 2. Pipeline Stages

### Stage 1: Bronze Ingestion (Extraction Loop)

* This stage uses a ForEach loop containing an Extraction_Loop activity
* It performs the Bronze_Extraction process, which pulls data from external sources (REST APIs) and save them as Delta tables in their raw, untransformed state under the bronze schema.
* This stage established a "Source of Truth". By landing data in Bronze layer first, this allow for re-producibility - if a transformation fails, it allow re-running the pipeline from the Bronze layer without having to hit the external source systems again.


### Stage 2: Silver Technical Layer (silver_t)

This stage focus on Technical cleaning to ensure data consistency across the pipeline.

* **Schema Enforcement**: Converts string-based Bronze data into proper types: IntegerType, DecimalType(18,2), TimestampType, and BooleanType
* **Data Standardization**: Trims whitespace, lowercases email addresses, and normalizes "Y/N" flags to Booleans
* **Integrity**: Removes records with null or empty primary keys and performs initial de-duplication
* Output: Generates cleaned technical tables for each dataset.

### Stage 3: Silver OBT (One Big Table) Generation (silver_b)

This stage joins the relational normalized structure into a single wide table (sales_obt) to simplify Star schema modeling.

* **Dynamic Join Engine**: Uses metadata configs to programmatically build SQL joins query, using orders_t as the base table.
* **Conflict Resolution**: Automatically handles ambiguous column names (e.g., city, email) by applying prefixes like customer_ or store_ for their respective source .
* **Auditability**: Includes an **obt_processed_at** timestamp for every merged record

### Stage 4: Data Quality Validation

This stage acts as a 'gatekeeper'. It is designed to halt the entire pipeline if critical tests fail.

* **Technical Tests**: Checks the uniqueness of primary keys and null values across all silver tables.
* **Business Rules**: Validates domain-specific logic, such as ensuring product prices are non-negative and verifying email formats with Regex
* **Logging**: All results are written to a dq_test_results table to maintain a historical audit log of data validation

### Stage 5: Gold Layer - Dimensions (gold dim)

Implements Slowly Changing Dimensions (SCD Type 2) to track historical changes in dimensions table over time.

* Change Detection: Uses SHA-256 hashing on tracked attributes to identify changes between the source OBT and the target dimension.
* History Management: Add valid_from, valid_to, and is_current flags.
* Surrogate Keys: Generates and maintains unique surrogate keys for every dimension tables.

### Stage 5: Gold Layer - Fact (gold fact)

Build the central fact_orders table at the order-item grain.

Grain Preservation: Selects specific measures (quantity, line_amount) and natural keys from the OBT.

De-duplication: Ensures the fact table remains at the correct grain by removing any duplicate rows based on order_id and order_item_id.

## 3. Maintenance & Optimization

TO maintain long-term performance of the pipeline, a separate maintenance workflow is implemented.

* Compaction (Optimizes): merge small files into larger ones to improve read performance, with specific time period for each fact and dimension tables.
* Cleanup (Vacuum): Removes expire delta files that are no longer needed for time travel, using 7-day (168-hour) retention period
* Reporting: Logs maintenance duration and status for every table to a table_maintenance_log table.
