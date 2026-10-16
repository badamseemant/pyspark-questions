# PySpark Questions and Practice

A hands-on learning project focused on practicing PySpark DataFrame operations, data transformations, aggregations, and ETL pipelines.

## Project Files

| File | Description |
|---|---|
| `practice.py` | PySpark practice questions and ETL exercises |
| `data_setup.py` | Creates the employee dataset used for practice |
| `day1.py` | Initial PySpark practice |
| `.gitignore` | Excludes generated files and local environment files |

## Topics Practiced

- DataFrame selection and filtering
- Column creation and transformations
- Sorting and duplicate removal
- Handling missing values
- String and array functions
- Grouping and aggregations
- Joins and unions
- Window functions
- User-defined functions (UDFs)
- CSV and Parquet operations
- ETL pipeline development
- Data cleaning and validation

## ETL Projects

The project includes practice pipelines for:

1. Calculating annual salaries.
2. Generating department-wise salary reports.
3. Validating employee data and categorising salaries.
4. Calculating employee bonuses and generating department-wise reports.

## Technology Stack

- Python
- Apache PySpark
- Apache Spark
- Parquet

## Setup

Install PySpark using:

```powershell
py -m pip install pyspark==3.5.9