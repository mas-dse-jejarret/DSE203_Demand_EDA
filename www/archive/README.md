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

SETTING UP AND CONNECTING TO Database at SERVER FROM local machine PGADMIN:

1. Launch Pgadmin from local machine.

2. Right click on Servers, Create and then Server

3. On General Tab, give a name to your server "cloudpostgres", click on Connection tab and select following settings:

Hostname/Address: 45.79.91.219

4. Leave everything else default, and Save.

5. Then You Should be able to see new server added as "cloudpostgres". Click on it to connect. It pops up a window asking password, Just click Ok without anything, coz we have not set any password. 

6. You should now see Server Connected mesg.

7. Go to Database MyBookStore which has new tables added.

# steps to test the api/service
run ipython
or create a new notebook

```python
import requests

query = '''
{
    "from" : {
                "v1" : "PG/dbTest/public/table1",
                "v2" : "AX/AstxDB/TableBT"
            },
    "where" : "v1.id = v2.parent_id",
    "order" : "v1.name",
    "opts" : {
                "random" : {}
             },
    "return" : "v1.id, v2.parent_id, v1.name, v2.childName"
}
'''

res = requests.post('http://45.79.91.219/api/service', json=query)
if res.ok:
    print res.text
```

## Anatomy of REST API method
```python
@app.route("/api/web_method/<format>")
def api_web_method(format):

    engine = create_engine('postgresql+psycopg2://postgres@45.79.91.219/MyBookStore')
    conn = engine.connect()

    sql = """
    select *
    from orderlines o, products p
    where o.productid = p.productid
    LIMIT 10
    """

    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {}
        for item in request.args:
            c = request.args[item]
            d[c] = result[c]
        l.append(d)
    #
    theresult_json = json.dumps(l, default=json_serial)

    conn.close()

    return theresult_json
 ```
 
 ## Sample Client Request of REST API method - localhost could be changed to remote ip address

```python
import requests
res = requests.get('http://localhost:80/api/web_method/json?c1=productid&c2=shipdate&c3=unitprice', json=query)
if res.ok:
    print res.json()
```


## Sample Solr call
```python
import requests
res = requests.get('http://45.79.91.219/api/solrwrap')
if res.ok:
    print res.json()
```


## Sample Asterix call
```python
import requests
res = requests.get('http://45.79.91.219/api/asterixwrap')
if res.ok:
    print res.json()
```

## Sample Native Solr and Asterix Calls
```python
from __future__ import print_function
import requests
import json
import pprint

pp = pprint.PrettyPrinter(depth=6)
sql = """USE AstxDB;
        select *  from TableBT v;
"""

ads = AsterixDataSource()
jsonobj = ads.execute(sql)

pp.pprint(jsonobj)

sds = SolrDataSource()
jsonobj = sds.execute("*:*")

pp.pprint(jsonobj)
```