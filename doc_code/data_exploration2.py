import psycopg2
from sqlalchemy import create_engine, text
import json
import pandas as pd
try:
    from urllib import parse, request
except:
    from urlparse import urlparse as parse
import urllib
import urllib2
from json import loads,dumps
import simplejson
import pysolr
from textblob import TextBlob as tb
import requests

host="132.249.238.27"
dbname='bookstore_dp'
user='student'
password='123456'

pg_connstring='postgresql+psycopg2://{0}:{1}@{2}:5432/{3}'.format(user, password, host, dbname)
astx_host="132.249.238.32" # not open or not running in ucsd
solr_host="132.249.238.28" # not open or not running in ucsd


def Stats(stat_func, col_pair, table_pair, key_pair):
    """
    Stats function inputs SQL statistical functions (CORR, COV, etc.), columns/tables of interest, and required
     keys to join tables, and returns a statistical value.
    :param stat_func: str, SQL statistical function call (CORR, COV, etc.)
    :param col_pair: list or tuple of length 2. Contains two column names to calculate statistical function on
    :param table_pair: list or tuple of length 2. Contains two table names which columns located in
    :param key_pair: list or tuple of length 2. Values should be strings. Contains primary/foreign keys from table_pair
    :return: float Output of the statistical function

    """
    engine = create_engine(pg_connstring)
    conn = engine.connect()

    sql = """
    select %s(a.%s::int, b.%s::int)
    from %s a, %s b
    where a.%s = b.%s
    """ %(stat_func, col_pair[0], col_pair[1], table_pair[0], table_pair[1], key_pair[0], key_pair[1])

    stmt = text(sql)
    result = conn.execute(stmt)

    conn.close()
    return result.fetchone()[0] #str(result)


def Simpleagg(agg_func_pair, col_pair, table_pair, key_pair):
    """
    Simpleagg allow you to input aggregation functions and columns and returns aggregated data from those columns
    :param agg_func_pair: list or tuple of length 2. Inputs should strings. Strings must be names of SQL aggregate
        functions (count, max, sum, etc)
    :param col_pair: list or tuple of length 2. Values should be strings. Two column names to calculate statistical function on
    :param table_pair: list or tuple of length 2. Values should be strings.Table names which columns located in.
    :param key_pair: list or tuple of length 2. Values should be strings. Contains primary/foreign keys from table_pair
    :return: list of tuples, containing aggregations
    """
    engine = create_engine(pg_connstring)
    conn = engine.connect()

    sql = """
    select %s(a.%s::int), %s(b.%s::int)
    from %s a, %s b
    where a.%s = b.%s
    group by %s
    """ %(agg_func_pair[0], col_pair[0], agg_func_pair[1], col_pair[1], table_pair[0],
        table_pair[1], key_pair[0], key_pair[1], col_pair[0])

    stmt = text(sql)
    result = conn.execute(stmt)

    conn.close()
    return result.fetchall()

def Correlation(col_pair, table_pair, key_pair):
    """
    Correlation inputs columns/tables of interest, and required
     keys to join tables, and returns a the correlation coefficient

    :param col_pair: list or tuple of length 2. Contains two column names to calculate statistical function on
    :param table_pair: list or tuple of length 2. Contains two table names which columns located in
    :param key_pair: list or tuple of length 2. Values should be strings. Contains primary/foreign keys from table_pair
    :return: float Correlation coefficient of the two variables

    """
    stat = Stats("corr", col_pair, table_pair, key_pair)
    return json.dumps({"correlation" : stat})

def Covariance(col_pair, table_pair, key_pair):
    """
        Covariance inputs columns/tables of interest, and required
         keys to join tables, and returns the covariance coefficient between two columns

        :param col_pair: list or tuple of length 2. Contains two column names to calculate statistical function on
        :param table_pair: list or tuple of length 2. Contains two table names which columns located in
        :param key_pair: list or tuple of length 2. Values should be strings. Contains primary/foreign keys from table_pair
        :return: float Covariance coefficient of the two variables

    """
    stat = Stats("covar_samp", col_pair, table_pair, key_pair)
    return json.dumps({"covariance" : stat})

def Histogram(table, groupby, count):
    """
    Histogram returns data for generating histograms in Postgres

    :param table: str, table name
    :param groupby: str, column to group by
    :param count: str, column to be counted in the group by
    :return: list of dictionaries, containing count and group keys

    """
    engine = create_engine(pg_connstring)
    conn = engine.connect()

    sql = """
    SELECT %s AS Group, count(%s) AS Count
    FROM %s
    Group by %s
    Order by count(%s) DESC
    """ % (groupby, count, table, groupby, count)

    # print(sql)

    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {'Group': result[0], 'Count': result[1]}
        l.append(d)

    theresult_json = simplejson.dumps(l, ensure_ascii=False)
    theresult_json = simplejson.loads(theresult_json)

    conn.close()

    return theresult_json

def getTopCategories(limit):
    """
    TopCategories returns top selling categories

    :param limit: int, number of categories to return
    :return: list of dictionaries of categories with category as key

    """
    list = ['Education & Reference',
             'Geography & Cultures',
             'Programming',
             'Science, Nature & How It Works',
             'Graphics & Design',
             'Animals',
             'Early Learning',
             'Engineering',
             'Home Improvement & Design',
             'Growing Up & Facts of Life',
             'History',
             'Architecture',
             'Regional & International',
             'Programming Languages',
             'Cars, Trains & Things That Go',
             "Women's Health",
             'Digital Audio, Video & Photography',
             'Software',
             'Transportation',
             'Automotive',
             'Hardware & DIY',
             'Music',
             'Pregnancy & Childbirth',
             'Photography & Video',
             'Military',
             'Crafts & Hobbies',
             'Christian Books & Bibles',
             'Comics & Graphic Novels',
             'Games & Strategy Guides',
             'Biographies',
             'Asian Cooking',
             'Needlecrafts & Textile Crafts',
             'Aging',
             'Investing',
             'Activities, Crafts & Games',
             'Fairy Tales, Folk Tales & Myths',
             'Physics',
             'Gardening & Landscape Design',
             'Web Development & Design',
             'Literature & Fiction',
             'Nature & Ecology',
             'Mental Health',
             'Religions',
             'Puzzles & Games',
             'Diseases & Physical Ailments',
             'Europe',
             "Children's & Teens",
             'Industries',
             'Africa',
             'Management & Leadership']
    return [{'category': l} for l in list[:limit]]

def getCategories():
    """
    getCategories returns dictionary of categories

    :return: list of dictionaries of all categories with category as key
    """
    sql="""
    use bookstore_dp;

    select user.category.nested.nested.level_2 as category
    from ClassificationInfo user
    group by category;
    """


    ads = AsterixDataSource(host=astx_host)
    jsonobj = ads.execute(sql)

    return (jsonobj)

def getNodeIds(category_list):
    """
    getNodeIds inputs a list of categories and returns a dictionary of category,nodeId pairs

    :param category_list: list, categories to get nodeIDs for
    :return: list of dictionaries of nodeIds as keys. Aggregated together (not separated by category)
    """

    _where = ' OR '.join(['user.category.nested.nested.level_2 = "{0}"'.format(x) for x in category_list])

    sql="""
    use bookstore_dp;

    select user.nodeID
    from ClassificationInfo user
    WHERE {0}
    """.format(_where)

    # where  user.category.nested.nested.level_2 = "Education & Reference";

    ads = AsterixDataSource(host=astx_host)
    jsonobj = ads.execute(sql)

    return jsonobj

def convertToIn(_jlist):
    """
    Helper function for HighestMonthlySalesByCategory

    :param _jlist: list, ints of nodeIDs
    :return: list, strings of nodeIDs
    """

    m = ','.join(["'{0}'".format(str(x["nodeID"])) for x in _jlist])
    return '({0})'.format(m)

def HighestMonthlySalesByCategory(category_list, limit):
    """
    HighestMonthlySalesByCategory inputs a list of categories and limit and returns a dictionary of
    month/num_sold pairs

    :param category_list: list, list of categories
    :param limit: int, number of results per category to return
    :return: list of dictionaries with "month" and "num_sold" keys
    """
    engine = create_engine(pg_connstring)

    conn = engine.connect()

    _jlist = getNodeIds(category_list)
    _inStr = convertToIn(_jlist)

    #
    sql = """
       SELECT mon, sum(books_sold) AS num_sold
       FROM
         (     select EXTRACT(MONTH from o.billdate) as mon, p.nodeid as category, count(o.orderid) as books_sold
               from orderlines as o, products as p
               where o.productid = p.productid AND o.totalprice > 0::money
               group by p.productid, EXTRACT(MONTH from billdate)
               order by p.productid
         ) monthlysales
       WHERE category IN {0}
       GROUP By mon
       ORDER BY num_sold DESC
       LIMIT {1}
       """.format(_inStr, limit)
    #
    # print(sql)

    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {'mon': int(result[0]), 'num_sold' : int(result[1])}
        l.append(d)

    # theresult_json = json.dumps(l)

    conn.close()

    return (l)

def OptimizedTopCategories(num_categories, months):
    """
    OptimizedTopCategories inputs number of categories and months of interest and returns category and
    number of books from that category purchased in that time range

    :param num_categories: int, number of categories to return
    :param months: list, months
    :return:  list of tuples of each category and number of books purchased in month range
    """
    monthStr = ','.join([str(x) for x in months])

    mainList = []

    mainDict = {}

    categories = getTopCategories(5)

    for item in [x['category'] for x in categories]:
        category = [item]

        _jlist = getNodeIds(category)
        _inStr = convertToIn(_jlist)
        engine = create_engine(pg_connstring)
        conn = engine.connect()

        sql = """
        SELECT '{3}' as category, sum(books_sold) AS num_sold
        FROM
          (     select EXTRACT(MONTH from o.billdate) as mon, count(o.orderid) as books_sold
                from orderlines as o, products as p
                where o.productid = p.productid AND o.totalprice > 0::money
                AND p.nodeid IN {2}
                group by EXTRACT(MONTH from billdate)
          ) monthlysales
          WHERE mon in ({0})
        GROUP BY category
        """.format(monthStr,num_categories, _inStr, item.replace("'",""))

        stmt = text(sql)
        results = conn.execute(stmt)

        l = []

        result = results.fetchone()

        t = (item, 0)

        if result is not None:
            t = (item, float(result[1]))
            mainList.append(t)


        conn.close()



    return (sorted(mainList, key=lambda x: x[1], reverse=True))[:num_categories]


def Discontinue_Stocking(threshold, startyear, endyear):
    # return jsonify({ "n" : num_categories, "m" : months})
    """
    Discontinue_Stocking inputs a book sales threshold (minimum) and time frame, and returns categories whose
        sales are below that threshold

    :param threshold: int, minimum number of books needed to be sold during time period
    :param startyear: int, start year. Function considers time after (not including) start year
    :param endyear:  int, end year. Function considers time up to and including end year
    :return: list of dictionaries with only "Category" key and categoryID value
    """

    engine = create_engine(pg_connstring)
    conn = engine.connect()

    sql = """
    SELECT category
    FROM (
          select EXTRACT(YEAR from o.billdate) as yr,
          p.nodeid as category, sum(o.numunits) as books_sold
          from orderlines as o, products as p
          where o.productid = p.productid AND o.totalprice > 0::money
          group by p.nodeid , EXTRACT(YEAR from o.billdate)
          order by p.nodeid
          ) yearly_sales

    where books_sold < {0} AND (yr < {2} AND yr >= {1})

    """.format(threshold,startyear, endyear)

    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {'category': result[0]}
        l.append(d)

    theresult_json = simplejson.dumps(l)
    theresult_json = simplejson.loads(theresult_json)

    conn.close()
    return (theresult_json)

def Downward_Sales(season):
    """
    Downward_Sales inputs a season and returns a list of dictionaries with SaleTrend and category keys
    
    :param season: str, season (summer, fall, winter, spring)
    :return: list of dictionaries with "SaleTrend" and "Category" keys
    """
    # return jsonify({ "n" : num_categories, "m" : months})
    seasons = {'spring':(3,4,5),
                'summer':(6,7,8),
                'fall':(9,10,11),
                'winter':(12,1,2)
                }
    seasontrend = seasons.get((season.lower()))

    engine = create_engine(pg_connstring)
    conn = engine.connect()

    sql = """
    SELECT s.category, round(avg(s.change_in_sales_from_last_month)) AS sale_trend
        FROM
        (
        SELECT category, mon, count(mon) OVER (PARTITION BY category) as num_months,
        books_sold - lag(books_sold,1) over (PARTITION BY category ORDER BY mon) as change_in_sales_from_last_month
        FROM(			select EXTRACT(MONTH from o.billdate) as mon, p.nodeid as category, count(o.orderid) as books_sold
                    from orderlines as o, products as p
                    where o.productid = p.productid AND o.totalprice > 0::money
                    group by p.nodeid, EXTRACT(MONTH from billdate)
                    order by p.nodeid
        )  monthly_sales
        WHERE mon in {0}
        GROUP BY category, mon, books_sold
        ) AS s
        WHERE s.num_months = {1}
        AND s.mon in {0}
        GROUP BY s.category
        having round(avg(s.change_in_sales_from_last_month)) < 0
        ORDER BY sale_trend ASC


    """.format(seasontrend,len(seasontrend))
    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {'category': result[0],'SaleTrend': result[1]}
        l.append(d)

    theresult_json = simplejson.dumps(l)
    theresult_json = simplejson.loads(theresult_json)

    conn.close()
    # return (results)
    # print(theresult_json)
    return (theresult_json)

class AsterixDataSource():

    def __init__(self, host="localhost", port=19002):
        self.headers = {'Content-type': 'application/x-www-form-urlencoded'}
        self.host = host
        self.port = port

    def execute(self, _sql):
        connstr = 'http://{0}:{1}/query/service'.format(self.host, self.port)

        response = requests.post(connstr, data=_sql, headers=self.headers)
        jsonobj = json.loads(response.text.replace("{ ,", "{ "))

        if jsonobj['status'] == 'success':
            return jsonobj['results']
        else:
            return json.dumps("[]")

def Sales_Reviews(category, month):
    # AsterixDBConnection

    class QueryResponse:
        def __init__(self, raw_response):
            self._json = loads(raw_response)

            self.requestID = self._json['requestID'] if 'requestID' in self._json else None
            self.clientContextID = self._json['clientContextID'] if 'clientContextID' in self._json else None
            self.signature = self._json['signature'] if 'signature' in self._json else None
            self.results = self._json['results'] if 'results' in self._json else None
            self.metrics = self._json['metrics'] if 'metrics' in self._json else None

    class AsterixConnection:
        def __init__(self, server='http://{0}'.format(astx_host), port=19002):

            self._server = server
            self._port = port
            self._url_base = self._server + ':' + str(port)

            # print ("connecting to: " + server)

        def query(self, statement, pretty=False, client_context_id=None):
            endpoint = '/query/service'

            url = self._url_base + endpoint

            payload = {
                'statement': statement,
                'pretty': pretty
            }

            if client_context_id:
                payload['client_context_id'] = client_context_id
            try:
                data = parse.urlencode(payload).encode("utf-8")
                req = request.Request(url, data)
                response = request.urlopen(req).read()
            except:
                data = urllib.urlencode(payload)
                data = data.encode("utf-8")
                req = urllib2.Request(url, data)
                response = urllib2.urlopen(req).read()
            # print(data)

            return QueryResponse(response)

    asterix_conn = AsterixConnection()
    axquery = '''
        use bookstore_dp;
        SELECT * from ClassificationInfo where classification LIKE "%{0}%";'''.format(category)

    response = asterix_conn.query(axquery)


    df = pd.DataFrame(response.results)
    node_id = []
    for i in range(df.shape[0]):
        a = df.ClassificationInfo[i]['nodeID']
        node_id.append(a)
    node_id = [str(x) for x in node_id]
    node_id = set(node_id)
    conn_string = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format(host, dbname, user, password)
    # print ("Connecting to database\n	->%s" % (conn_string))
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    # print ("Connected!\n")

    sql = "SELECT DISTINCT o.productid, o.billdate, o.numunits, p.asin,p.nodeid\
        FROM orderlines o, products p\
        WHERE o.productid=p.productid\
        AND EXTRACT(month from billdate)={0};".format(month)

    cursor.execute(sql)

    # retrieve the records from the database
    records = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df1 = pd.DataFrame(records, columns=colnames)
    df2 = df1[df1['nodeid'].isin(node_id)]
    df3 = df2.groupby(['billdate', 'productid', 'asin', 'nodeid'], as_index=False)['numunits'].sum()
    my_asin = df3['asin']
    my_asin = set(my_asin)
    asin = ', '.join(my_asin)

    def solrWrap(core, params):
        query_string = 'http://{0}:8983/solr/{1}/select?'.format(solr_host, core)  # connecting to our linode server
        for key in params:
            query_string = query_string + key + '=' + params[key] + '&'

        # print(query_string)

        solrcon = pysolr.Solr(query_string, timeout=10)
        results = solrcon.search('*:*')
        docs = pd.DataFrame(results.docs)
        return docs

    d3 = {'q': 'asin:(%s)' % asin, 'rows': '77165'}
    d_res3 = solrWrap(dbname, d3)
    polarity_measure = []
    for i in range(d_res3.shape[0]):
        str1 = str(d_res3.reviewText[i])
        blob = tb(str1)
        polarity_measure.append(blob.sentiment.polarity)

    se = pd.Series(polarity_measure)
    d_res3['Sentiment_polarity'] = se.values
    d_res3['asin'] = d_res3['asin'].apply(lambda x: '' + str(x)[3:-2] + '')
    df_sentiment = d_res3.groupby(['asin'], as_index=False)['Sentiment_polarity'].mean()
    result = pd.merge(df3, df_sentiment, on='asin', how='inner')
    final_result = result.reset_index().to_json(orient='records')

    return final_result

def GetTextAttributes(nodeIds=[1]):
    _where = ' OR '.join(['user.nodeID = {0}'.format(x) for x in nodeIds])

    sql = """
    use bookstore_dp;

    select user.nodeID, user.classification, user.category.nested.nested.level_2
    from ClassificationInfo user
    where {0};
    """.format(_where)

    ads = AsterixDataSource(host=astx_host)
    jsonobj = ads.execute(sql)

    return jsonobj

# pip install psycopg2
# pip install pysolr
# pip install textblob

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=80)
    print("Correlation: \n")

    col_pair = ('numunits','productid')
    table_pair = ('orderlines','products')
    key_pair = ('productid', 'productid')

    retval=Correlation(col_pair, table_pair, key_pair)

    print(retval)

    print("Covariance: \n")


    retval=Covariance(col_pair, table_pair, key_pair)

    print(retval)

    print("Histogram: \n")


    table = "orders"
    groupby = 'state'
    count = 'customerid'

    h = Histogram(table, groupby, count)

    print(h)



    print("Top Categories: \n")

    tc = OptimizedTopCategories(3, [12])
    print(tc)

