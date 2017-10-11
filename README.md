# DSE203_Demand_EDA
Team Horkos' books sales demand prediction repo

to connect to pgadmin in python:


```python
import psycopg2
import pandas as pd

# create a connection to database. make sure that the db name and password are correct
try:
    conn = psycopg2.connect("dbname='SQLBook' user='postgres' host='/tmp/' password='Kitty555'")
except:
    print "unable to connect to the database"

df = pd.read_sql("select * from calendar", con=conn)
conn.close()
df.head()```
