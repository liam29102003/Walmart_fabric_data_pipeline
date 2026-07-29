Project Overview



1\. Executive Summary



This project implements a Medallion Architecture data pipeline using Microsoft Fabric, Pyspark, and Delta Lake. This project purpose is to build a pipeline that transforms raw  sales dataset into business-ready Star Schema optimized for high-performance analytics.

This implementation includes metadata-driven engine for dynamic SQL generation, a robust Data Quality framework, and a "Circuit Breaker" pattern to ensure data integrity.



2\. Business Objective



The goal of this project is to transform raw transactional sales data into an analytics-ready data platform that enables reliable reporting, historical analysis, and scalable business intelligence workloads.





3\. Data Architecture (Medallion)



This project follows Medallion Archit (lakehouse architecture) to ensure traceability and reliability:

* **Bronze** (Raw): Ingests the source data from REST API into the Bronze layer as Delta tables. This project implements cursor-based Change Data Capture pattern, detecting incremental changes using updated\_timestamp columns to fetch only new or modified records since the last execution. This improves pipeline efficiency by processing only incremental changes through automated upserts using primary keys and maintain idempotency by preventing the inserting of duplicate records during repeated runs.
* **Silver** (Technical \& Business) :The silver layer is separated into two sub-layers.

  * **Technical Layer**: This layer is implemented using modular cleaning functions which handle whitespace trimming, type casting and primary key validation.
  * **Business Layer**: This layer is for building a "One Big Table" (OBT) model that combines related business entities before analytical modeling using a custom-built dynamic SQL generator based on metadata configurations, to ensure that pipline scales without code modifications as new tables are added.
* **Gold (Analytics):** Deconstructs the OBT into a star schema with facts and dimension tables.

  * SCD Type 2 logic: Implements manual Slowly Changing Dimensions (Type 2) using SHA-256 row hashing for change detection to track historical data accurately.



4\. Engineering Design Principles



This project implements software engineering practices for extensibility, modularity and reliability to ensure the pipeline is dependable.



* **Metadata-Driven Design:** All complex transformations (OBT joins and Dimension table logic) are implemented using configuration lists, decoupling business logic from the execution engine. (Separation of concern and Declarative programming)
* **Dispatch Table pattern:** Creates dispatch tables for mapping the result of data quality tests to specific lambda functions, satisfying the Open-Closed Principle. (Behavioral Design Patterns and SOLID Principles)
* **Circuit Breaker Pattern:** The pipeline is designed to act as a data quality gatekeeper. If critical data quality standards are violated, the system triggers exception handling procedures programmatically that stop downstream star schema development. (Fault Tolerance and Fail-Fast Architecture)
* **Operational Excellence:** Includes an automated Maintenance task for gold layer that runs OPTIMIZE and VACUUM commands, to manage database storage and improve performance.



5\. Technology Stack



* Storage \& Engine: Delta Lake, Spark(PySpark), Microsoft Fabric
* Orchestration: Microsoft Fabric Pipelines
* Version Control: Git \& GitHub
* Languages: Python (PySpark), SQL



6\. Future Extensions



To demonstrate a long-term roadmap for production environments, the following features are planned:

* Automated Alerting: Integration with Office 365/Teams to send notifications on pipeline failure.
* Environment Parameterization: Implementing dynamic environment switching for CI/CD readiness.
* Semantic Layer Integration: Automating the refresh of Power BI Semantic Models upon successful Gold-layer completion.

