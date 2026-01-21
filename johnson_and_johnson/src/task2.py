import duckdb

# 1. What’s the total cost of maintenance events recorded (in EUR)
duckdb.sql("SELECT round(sum(cost_eur), 2) as total_cost from 'data/maintenance_events.csv'").show()

# 2. Total downtime (in minutes) across all maintenance events
duckdb.sql("SELECT round(sum(downtime_min), 2) as total_downtime from 'data/maintenance_events.csv'").show()

# 3. How many unique maintenance events are recorded
duckdb.sql("SELECT count(distinct event_id) as total_events from 'data/maintenance_events.csv'").show()

# 4. How many breakdowns (unplanned) happened?
duckdb.sql("SELECT count(*) as unplanned_breakdowns FROM 'data/maintenance_events.csv' WHERE reason = 'Unplanned Breakdown'").show()

# 5. What’s the average downtime per event?
duckdb.sql("SELECT reason, round(avg(downtime_min), 2) as avg_downtime_per_event FROM 'data/maintenance_events.csv' GROUP BY reason").show()
