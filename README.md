# DSE203_Demand_EDA
Team Horkos' books sales demand prediction repo

to connect to pgadmin in python:


```python
import psycopg2
import pandas as pd

# create a connection to database. make sure that the db name and password are correct
try:
    conn = psycopg2.connect("dbname='SQLBook' user='postgres' host='/tmp/' password='YOURPASSWORD'")
except:
    print "unable to connect to the database"

df = pd.read_sql("select * from calendar", con=conn)
conn.close()
df.head()
```
SQL++
```sql
USE TinySocial;

SELECT uid AS uid, ARRAY_COUNT(grp) AS reviewCount
FROM reviews r
GROUP BY r.reviewerID AS uid
GROUP AS grp(r AS msg);
```


Linux node box: www.linode.com

user: yn2moreno

pwd: rM0fGTBDuF

Launch a console through terminal by copying the lish via SSH command or
launch a console in the browser through the Glish link

after logging in, log on through shell:

user: root

pwd: !QAZXSW@1qazxsw2

