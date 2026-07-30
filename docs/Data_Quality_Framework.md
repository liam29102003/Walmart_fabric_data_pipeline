# Data Quality Framework


## 1. Overview


The Data Quality Framework is a centralized, meta-driven validation engine designed to ensure data integrity as it moves from the Silver technical layer (silver\_t) to the Gold layer . It is design[..]


## 2. Core Validation Logic


This framework use a Dispatch pattern to map specific test types to modular Pyspark validation functions. This is meant for easy extensibility. New test case can be added by simply defining a new [...]


#### Supported test types:


* **not\_null**: Checks rows if critical columns (typically Primary Keys) contain NULL or empty string values.
* **unique**: Calculates the total number of records involved in a duplicate group, providing a direct count of records requiring investigation.
* **threshold**: Validates that numeric values satisfy specific mathematical operators (e.g., >=, <, >) against a defined threshold (e.g., ensuring product prices are never negative)
* **email\_format**: Uses a Regex pattern (**EMAIL\_REGEX**) to validate the structure of email addresses while intentionally ignoring NULLs (which are handled by the **not\_null** test)


## 3. Metadata-Drive Execution


The framework is configured using a metadata list (DQ\_TEST\_METADATA), which allow to  define test without writing new code for every table.


* **Generic Technical Tests**: Generate automatically for every table defined in the PRIMARY\_KEYS dictionary to ensure PKs are always unique and populated.


* **Business Rule Tests**: Defined specifically for domain logic, such as:

  * **product\_price\_non\_negative**: Ensures price >= 0 in the **products\_t** table


## 4. Operational Features


##### **Efficient Processing**


The framework minimizes compute costs by using a table cache during execution. Even if multiple tests are performed on the same table. the source data is only read once per session.


##### **Historical Auditing & Logging**


All test results—including table names, test names, failed record counts, and execution timestamps—are appended to a persistent Delta table: **silver.dq\_test\_results**.


##### **Pipeline "Circuit Breaker"**


The framework enforces a strict quality standard. If any test results in a FAIL status, the notebook raises an Exception, which stop the pipeline before it can insert data to the Gold layer.


