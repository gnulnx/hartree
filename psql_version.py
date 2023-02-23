#!/usr/bin/env python
"""
This file was used to quickly figure out the proper SQL query to use for the apache beam SQLTransorm statement.
It is included for posterity sake.
"""
import psycopg2

conn = psycopg2.connect(
    database="test",
    host="localhost",
    user="postgres",
    password="postgres",
    port="5432"
)

cursor = conn.cursor()

# drop previous tables in case you need to rerun
cursor.execute("""DROP TABLE IF EXISTS ds1""")
cursor.execute("""DROP TABLE IF EXISTS ds1""")
conn.commit()

# Create ds1 table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS ds1 (
        invoice_id INTEGER PRIMARY KEY,
        legal_entity VARCHAR(40),
        counter_party VARCHAR(40),
        rating INTEGER,
        status VARCHAR(40),
        value INTEGER
    )
""")
conn.commit()

# Create ds2 table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS ds2 (
        counter_party VARCHAR(40),
        tier INTEGER
    )
""")
conn.commit()

# read in dataset1
with open("dataset1.csv", "r") as f:
    header = f.readline()
    rows = [line.strip().split(",") for line in f.readlines()]
   
# Create ds1 table with dataset1 values
for vals in rows:
    cursor.execute("""
        INSERT INTO ds1 (invoice_id, legal_entity, counter_party, rating,status, value)
        VALUES (%s, %s, %s, %s, %s, %s)""",
        vals
    )
    conn.commit()

# read in dataset2
with open("dataset2.csv", "r") as f:
    header = f.readline()
    rows = [line.strip().split(",") for line in f.readlines()]

# Create ds2 table with dataset2 values
for vals in rows:
    cursor.execute("""
        INSERT INTO ds2 (counter_party, tier)
        VALUES (%s, %s)""",
        vals
    )
    conn.commit()


# Fully working example.  Yah!
# This is the KEEPER
cursor.execute("""
    SELECT 
        ds1.legal_entity,
        ds1.counter_party,
        ds2.tier,
        max(ds1.rating) as max_rating_counter_party,

        CASE
            WHEN arap.total is null THEN 0
            ELSE arap.total
        END arap_total,

        CASE
            WHEN accr.total is null THEN 0
            ELSE accr.total
        END accr_total

    FROM ds1
    
    INNER JOIN ds2 on ds1.counter_party = ds2.counter_party 

    LEFT JOIN (
        SELECT ds1.legal_entity, ds1.counter_party, sum(ds1.value) as total
        FROM ds1
        WHERE ds1.status = 'ARAP'
        GROUP BY ds1.legal_entity, ds1.counter_party
    ) arap 
    ON ds1.counter_party = arap.counter_party 
    AND ds1.legal_entity = arap.legal_entity

    LEFT JOIN (
        SELECT ds1.legal_entity, ds1.counter_party, sum(ds1.value) as total
        FROM ds1
        WHERE ds1.status = 'ACCR'
        GROUP BY ds1.legal_entity, ds1.counter_party
    ) accr 
    ON ds1.counter_party = accr.counter_party 
    AND ds1.legal_entity = accr.legal_entity
    
    GROUP BY ds1.legal_entity, ds1.counter_party, ds2.tier, arap.total, accr.total
    ORDER BY ds1.legal_entity, ds1.counter_party LIMIT 1000
""")

# Print out results to console
for result in cursor:
    print(result)


# output to console
"""
('L1', 'C1', 1, 3, 40, 0)
('L1', 'C3', 3, 6, 5, 0)
('L1', 'C4', 4, 6, 40, 100)
('L2', 'C2', 2, 3, 20, 40)
('L2', 'C3', 3, 2, 0, 52)
('L2', 'C5', 5, 6, 1000, 115)
('L3', 'C3', 3, 4, 0, 145)
('L3', 'C6', 6, 6, 145, 60)
"""