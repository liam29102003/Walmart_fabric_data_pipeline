Maintenance Strategy



1\. Objective



The goal of this maintenance strategy is to ensure the Gold Layer is in good performance and cost-effective. Over time, frequent updates (SCD type 2) and full-fact overwrite can lead to a "small file problem". This strategy automates the compaction of files and the removal of expired data versions.



2\. Core maintenance



This strategy use two primary delta lake commands to maintain table health:

* **OPTIMIZE:** This operation combine small files into larger, more efficient parquet files. This significantly improves read performance for downstream bi tools and analytical queries.
* **VACUUM:** This operation removes data files that are no longer in the latest state of the transaction log and are older than a defined retention period. This is essential for restoring storage space.



3\. Targeted Table Tiers



Maintenance is applied to all tables under the gold schema, separated by their updated frequency (Fact table is set as high-frequency and dim tables are set for weekly maintenance).



4\. Configuration \& Threshold



To balance performance with compute cost, the following thresholds are implemented:

* **Default retention**: 7 days retention period is set for Vacuum operations, which ensure that time-travel operations is only limited to one weak period and clean the older data.
* **Optimization Threshold**: The system is designed to trigger optimization once a table reaches a minimum file count of 500 which is meant to prevent unnecessary compute on already optimized small tables.



5\. Automated Monitoring and Auditing



The maintenance workflow is fully observable through centralized logging system.

* Log Table: All test results are inserted into table\_maintenance\_log table.
* Metrics Tracked: This table records the optimize\_status, vacuum\_status, and the duration (in seconds) for each operation for every table.
* Schema Evolution: The log table uses the **.option("mergeSchema", "true")** setting to ensure that if maintenance metrics are expanded in the future, the log remains stable



6\. Error Handling \& Workflow Integrity



The maintenance notebook acts as a "smart" process within the pipeline

* **Failure Detection**: The script calculates a failed\_count at the end of every run. If any table fails one of its maintenance tasks, the process raises an Exception
* **Alerting**: This failure notification ensures that the data engineers are alerted to failure issues before these impact downstream query performance.

