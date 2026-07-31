#### Lessons Learned





###### **1. Architectural Robustness**



* **Separating Transformation from Business Logic:** Keeping "Technical CLeaning" separated from "Business Logic" ensures that data-type issues are resolved before complex joins or historical tracking is started.



###### **2. Data Quality as a "Circuit Breaker"**



* Implementing Data Quality check between the Silver and Gold layers is essential. If that Data quality check failed, the pipeline prevent corrupt data from reaching the downstream reporting layers
* **Dispatch Pattern**: Implementing a "dispatch table" for Data Quality tests allows the data quality framework can add new validation rules with minimal code changes.
* **Table Caching**: Reading source tables into a cache during data quality process prevents redundant and expensive I/O operations when multiple tests are performed on the same dataset.



###### **3. Advanced Transformation Techniques**



* Change Detection with Hashing: For dimension tables, using SHA-256 hashing on tracked attributes is better and more reliable option than comparing every individual column. It is a fast and efficient way to see if any data has changed, regardless of which part is different, so the system can keep an accurate record of its history.
* Metadata-Driven SQL: Building SQL programmatically with metadata config for "One Big Table" reduces risk for manual coding errors and simplifies the handling of ambigious column names (e.g., first\_name appearing in both Customer and Employee tables)





###### **4. Delta Table Maintenance Strategies**



* Performance in Delta Lake degrades over time due to the "small file problem." Regularly scheduled OPTIMIZE and VACUUM operations are needed to maintain high-speed query performance.



