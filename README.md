# Instructions and Documentation

## requirements

Install python dependencies using pip:

```bash
pip install -r requirements.txt
```

Java is also required to run pyspark. If not installed, install using brew:

```bash
brew install openjdk@21
```

## Task 2

This task requires answer to 5 questions. The answer to questions are pretty simple as thy are simple group by and aggregations. 

In as real world scenario, these questions would be answered using SQL queries. Since the data is provoided in CSV format, I have used duckdb to run SQL queries over the CSV file directly. There are lots of ther options available like pandas, pyspark etc. In a real world scenario, the choice of tool would depend on the data size, infrastructure available and team expertise.

Since this is a small dataset without too many constraints, duckdb is a good fit here due to simplicity. If it was a larger dataset, pyspark would be a better fit. The answer to the questions are in the file `johnson_and_johnson/src/task2.py` as SQL queries.

### Short description of the questions answered

#### Question 1: What’s the total maintenance cost?

We jsut need to aggregate the cost_eur column over the entire table:
```sql
SELECT round(sum(cost_eur), 2) as total_cost from 'data/maintenance_events.csv'
```

#### Question 2: How many minutes of downtime have there been? 

We jsut need to aggregate the downtime_min column over the entire table:
```sql
SELECT round(sum(downtime_min), 2) as total_downtime from 'data/maintenance_events.csv'
```

#### Question 3: How many maintenance events occurred?

For this one we need to count the number of distinct event_id in the table:
```sql
SELECT count(distinct event_id) as total_events from 'data/maintenance_events.csv'
```

#### Question 4: How many breakdowns (unplanned) happened?

We need to count all the rows where reason is 'Unplanned Breakdown':
```sql
SELECT count(*) as unplanned_breakdowns FROM 'data/maintenance_events.csv' WHERE reason = 'Unplanned Breakdown'
```

#### Question 5: What’s the average downtime per event?

I am not sure what th definition of even is in here, considering `per event` to mean per different reasons, we can calculate average downtime per reason:
```sql
SELECT reason, round(avg(downtime_min), 2) as avg_downtime_per_event FROM 'data/maintenance_events.csv' GROUP BY reason
```

### Running the code

To run the code for Task 2, use the following command:

```bash
python johnson_and_johnson/src/task2.py
```

## Task 3

For this task, 3 fils are provided in the `johnson_and_johnson/src/` directory:
- `load_bronze.py`: Contains a spark job that loads data from `input_data` into `johnson_and_johnson/data/bronze` in 2 directories for csv and parquet files. Basically the data is dumped without any changes. 
- `load_silver.py`: Contains a spark job that reads data from bronze layer, does some basic transformations (in this case just deduplication) and writes the data to `johnson_and_johnson/data/silver` layer in parquet format.
- `load_gold.py`: Contains a spark job that reads data from silver layer into dimension and fact tables and writes them to `johnson_and_johnson/data/gold` layer in parquet format. For this task, there are 2 tables:
    - dim_operators: Contains operator_id and name columns from operators_roster.csv
    - fct_manufacturing_factory_dataset: Contains all columns from maintenance_events.csv with operator_id column joined from dim_operators table.

### Running the code

run the following commands to execute the ETL pipeline:

```bash
python johnson_and_johnson/src/load_bronze.py
python johnson_and_johnson/src/load_silver.py
python johnson_and_johnson/src/load_gold.py
```

check out the result in parquet or csv in the respective directories in `johnson_and_johnson/data/` in bronze, silver and gold folders.

# Task 4

Did not have time to implement this task. But the idea would be to write unit tests and integration tests as explained below:

    - Unit tests: For unit tests using packages like mockito or pytest, we would use mocked data to test the functionality of each transformation step in isolation. These tests can be run in github actions as part of CI/CD pipeline to ensure that any changes to the code do not break existing functionality.
    - Integration tests: For integration tests, we would use a small subset of real data to test the end-to-end data pipeline. This could involve running the entire ETL in development or staging environments. 