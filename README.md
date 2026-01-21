# Instructions and Documentation

## requirements

Install python dependencies using pip:

```bash
pip install -r requirements.txt
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