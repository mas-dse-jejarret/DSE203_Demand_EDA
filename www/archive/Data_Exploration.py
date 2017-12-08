from __future__ import print_function, division, unicode_literals

from datetime import date, datetime

import simplejson
from flask import Flask, request, jsonify

from www.archive.datasources import AsterixDataSource
from www.archive.datasources import SolrDataSource
from www.archive.datasources import convertToIn
from www.archive.middleware import VirtualIntegrationSchema
from www.archive.middleware import WebSession

#from sentiment_polarity import Sales_Reviews


import pandas as pd
from urllib import parse, request
import json
from json import loads
import psycopg2
from sqlalchemy import create_engine, text
import pysolr
from textblob import TextBlob as tb


app = Flask(__name__)

@app.route("/")
def Hello():
    return "Hello World!"


@app.route('/api/service', methods=['POST'])
def api_service():
    query = request.get_json(silent=True)

    # needs to change to reading from xml file
    xml = VirtualIntegrationSchema()

    web_session = WebSession(xml)
    return jsonify(web_session.get_result_sets(query))

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

@app.route("/api/web_method/<format>")
def api_web_method(format):

    engine = create_engine('postgresql+psycopg2://student:123456@132.249.238.27:5432/bookstore_dp')
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
            print (c)
            d[c] = result[c]
        l.append(d)
    #
    theresult_json = json.dumps(l, default=json_serial)

    conn.close()

    return theresult_json


@app.route("/api/correlation/<col1>/<col2>")
def Correlation(col1, col2):

    engine = create_engine('postgresql+psycopg2://student:123456@132.249.238.27:5432/bookstore_dp')
    conn = engine.connect()

    sql = """
    select corr(%s::int, %s::int)
    from orderlines o, products p
    where o.productid = p.productid
    """ %(col1, col2)

    stmt = text(sql)

    result = conn.execute(stmt)

    for row in result:
        print(row[0])
    conn.close()

    return str(row[0])

@app.route("/api/covariance/<col1>/<col2>", methods=['GET'])
def Covariance(col1, col2):
    """Determine the covariance coefficient between two columns."""

    engine = create_engine('postgresql+psycopg2://student:123456@132.249.238.27:5432/bookstore_dp')
    conn = engine.connect()

    sql = """
    select covar_samp(%s::int, %s::int)
    from orderlines o, products p
    where o.productid = p.productid
    """ %(col1, col2)

    stmt = text(sql)

    result = conn.execute(stmt)

    for row in result:
        print(row[0])
    conn.close()

    return str(row[0])


@app.route("/api/histogram/<groupby>/<count>")
def Histogram(groupby, count):

    engine = create_engine('postgresql+psycopg2://student:123456@132.249.238.27:5432/bookstore_dp')
    conn = engine.connect()

    sql = """
    SELECT %s AS Group, count(%s) AS Count
    FROM orders
    Group by %s
    Order by count(%s) DESC
    """ % (groupby, count, groupby, count)

    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {'Group': result[0], 'Count': result[1]}
        l.append(d)

    theresult_json = json.dumps(l)

    conn.close()

    return theresult_json


@app.route('/api/add_message/<uuid>', methods=['GET', 'POST'])
def add_message(uuid):
    content = request.get_json(silent=True)
    print (content)
    return jsonify('{"h" : "ok"}')


@app.route('/api/asterixwrap', methods=['GET'])
def api_asterixwrap():
    sql = """USE AstxDB;
            select *  from TableBT v;
    """

    ads = AsterixDataSource()
    jsonobj = ads.execute(sql)

    return jsonify(jsonobj)


def getNodeIds(category_list):

    _where = ' OR '.join(['user.category.nested.nested.level_2 = "{0}"'.format(x) for x in category_list])

    sql="""
    use bookstore_dp;

    select user.nodeID
    from ClassificationInfo user
    WHERE {0}
    """.format(_where)

    # where  user.category.nested.nested.level_2 = "Education & Reference";

    ads = AsterixDataSource(host="132.249.238.32")
    jsonobj = ads.execute(sql)

    return jsonobj


@app.route("/api/highest_monthly_sales_by_category/<list>")
def api_highest_monthly_sales_by_category(list):
    category_list = list.split(",")

    # print(category_list)

    engine = create_engine('postgresql+psycopg2://student:123456@132.249.238.27:5432/bookstore_dp')
    #engine = create_engine('postgresql+psycopg2://postgres@45.79.91.219/MyBookStore')
    #
    conn = engine.connect()
    #
    _jlist = getNodeIds(category_list) # will be replaced by asterix call once connected to DB - the result will not change though
    print(_jlist)
    _inStr = convertToIn(_jlist)
    # print(_inStr)
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
       """.format(_inStr)
    #
    print(sql)

    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {'mon': int(result[0]), 'num_sold' : int(result[1])}
        l.append(d)

    theresult_json = json.dumps(l)

    conn.close()

    return (theresult_json)



@app.route('/api/solrwrap', methods=['GET'])
def api_solrwrap():
    q = "*:*"

    ads = SolrDataSource()
    jsonobj = ads.execute(q)

    return jsonify(jsonobj)


@app.route("/api/Top_Categories/<num_categories>/<months>")
def Top_categories(num_categories, months):
    # return jsonify({ "n" : num_categories, "m" : months})


    engine = create_engine('postgresql+psycopg2://student:123456@132.249.238.27:5432/bookstore_dp')
    conn = engine.connect()

    sql = """
    SELECT category, sum(books_sold) AS num_sold
    FROM
      (     select EXTRACT(MONTH from o.billdate) as mon, p.nodeid as category, count(o.orderid) as books_sold
            from orderlines as o, products as p
            where o.productid = p.productid AND o.totalprice > 0::money
            group by p.nodeid, EXTRACT(MONTH from billdate)
            order by p.nodeid
      ) monthlysales
      WHERE mon in ({0})
    GROUP BY category
    ORDER BY num_sold DESC
    LIMIT ({1})
    """.format(months,num_categories)

    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {'category': result[0], 'num_sold': float(result[1])}
        l.append(d)

    theresult_json = json.dumps(l)

    conn.close()
    # return (results)
    # print(theresult_json)
    return (theresult_json)



@app.route("/api/Discontinue_Stocking/<threshold>/<startyear>/<endyear>")
def Discontinue_Stocking(threshold, startyear, endyear):
    # return jsonify({ "n" : num_categories, "m" : months})


    engine = create_engine('postgresql+psycopg2://student:123456@132.249.238.27:5432/bookstore_dp')
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

    theresult_json = json.dumps(l)

    conn.close()
    # return (results)
    # print(theresult_json)
    return (theresult_json)



@app.route("/api/Downward_Sales/<season>")
def Downware_Sales(season):
    # return jsonify({ "n" : num_categories, "m" : months})
    seasons = {'spring':(3,4,5),
                'summer':(6,7,8),
                'fall':(9,10,11),
                'winter':(12,1,2)
                }
    seasontrend = seasons.get((season.lower()))

    engine = create_engine('postgresql+psycopg2://student:123456@132.249.238.27:5432/bookstore_dp')
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

    conn.close()
    # return (results)
    # print(theresult_json)
    return (theresult_json)

@app.route('/sentiment_polarity/<category>/<month>', methods=['GET'])
def Sentiment_Polarity(category, month):
    sr = Sales_Reviews(category, month)
    return sr

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
        def __init__(self, server='http://45.79.91.219', port=19002):
            self._server = server
            self._port = port
            self._url_base = self._server + ':' + str(port)

        def query(self, statement, pretty=False, client_context_id=None):
            endpoint = '/query/service'

            url = self._url_base + endpoint

            payload = {
                'statement': statement,
                'pretty': pretty
            }

            if client_context_id:
                payload['client_context_id'] = client_context_id

            data = parse.urlencode(payload).encode("utf-8")
            req = request.Request(url, data)
            response = request.urlopen(req).read()

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
    conn_string = "host='45.79.91.219' dbname='MyBookStore' user='postgres' password=''"
    print ("Connecting to database\n	->%s" % (conn_string))
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print ("Connected!\n")
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
    asin_str = ', '.join(my_asin)

    def solrWrap(core, params):

        query_string = 'http://45.79.91.219:8983/solr/' + core + '/select?'  # connecting to our linode server
        for key in params:
            query_string = query_string + key + '=' + params[key] + '&'
            # print (query_string)
        solrcon = pysolr.Solr(query_string, timeout=10)
        results = solrcon.search('*:*')
        docs = pd.DataFrame(results.docs)
        return docs

    d3 = {'q': 'asin:(%s)' % asin_str, 'rows': '77165'}
    d_res3 = solrWrap('bookstore', d3)
    polarity_measure = []
    for i in range(d_res3.shape[0]):
        str1 = str(d_res3.reviewText[i])
        blob = tb(str1)
        polarity_measure.append(blob.sentiment.polarity)

    se = pd.Series(polarity_measure)
    d_res3['Sentiment_polarity'] = se.values
    d_res3['asin'] = d_res3['asin_str'].apply(lambda x: '' + str(x)[2:-2] + '')
    df_sentiment = d_res3.groupby(['asin'], as_index=False)['Sentiment_polarity'].mean()
    result = pd.merge(df3, df_sentiment, on='asin', how='inner')
    final_result = result.reset_index().to_json(orient='records')
    return final_result


if __name__ == "__main__":
    app.run(host='0.0.0.0',port=80)
