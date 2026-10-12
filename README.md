# Day 22 — E-commerce Batch Processing Pipeline

## Project Overview
This project implements an end-to-end batch data processing pipeline using Scala and Apache Spark. It reads raw e-commerce transactions, cleans invalid records, joins customer and product master data, aggregates sales revenue, and writes partitioned Parquet output.

## Technologies Used
- Scala 2.12.18
- Apache Spark 3.5.3
- Java 17
- SBT 1.12.15
- CSV and Parquet
- Ubuntu on WSL2

## Pipeline Workflow
1. Read raw transaction data from CSV files.
2. Remove invalid records and duplicate transaction IDs.
3. Join transactions with customer and product master data.
4. Calculate revenue using quantity multiplied by unit price.
5. Aggregate transaction counts, quantities, and revenue by date, city, and product category.
6. Write the daily sales report to partitioned Parquet files.
7. Read the Parquet output to verify the results.

## Input Files
- `data/transactions.csv` — transaction records
- `data/customers.csv` — customer details and cities
- `data/products.csv` — product names and categories

## Data Cleaning
The pipeline filters records with missing transaction, customer, or product IDs, invalid quantities, non-positive unit prices, and invalid dates. Duplicate transaction IDs are removed.

Transactions that do not match customer or product master data are excluded by inner joins.

## Revenue Calculation
Revenue is calculated as:

`revenue = quantity * unit_price`

The report groups data by transaction date, city, and product category.

## Output
Partitioned Parquet output is stored in:

`output/daily-sales`

The output is partitioned by:
- `year`
- `month`
- `day`

Example folder structure:

`year=2026/month=10/day=1/`

## How to Run

Compile the project:

```bash
sbt compile
```

Run the pipeline:

```bash
sbt "runMain Day22BatchPipeline"
```

## Key Learnings
- Building an end-to-end Spark batch pipeline.
- Reading CSV files with an explicit schema.
- Cleaning invalid and duplicate records.
- Joining fact data with customer and product dimensions.
- Aggregating revenue with Spark SQL functions.
- Writing and validating partitioned Parquet output.

## Author
**Yelamaneni Bhavyasai**

30 Days Scala and Apache Spark Training — Day 22
