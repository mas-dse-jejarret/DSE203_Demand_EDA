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

SET UP A SERVER FROM PGADMIN:
Launch Pgadmin from local machine.
Right click on Servers, Create and then Server
On General Tab, give a name to your server "cloudpostgres", click on Connection tab and select following settings:
Hostname/Address: 45.79.91.219
Leave everything else default, and Save.
Then You Should be able to see new server added as "cloudpostgres". Click on it to connect. It pops up a window asking password, Just click Ok without anything, coz we have not set any password. 
You should now see Server Connected mesg.
Go to Database MyBookStore which has new tables added.
