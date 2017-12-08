from __future__ import division, unicode_literals
import pandas as pd
from urllib import parse, request
import json
from json import loads
import psycopg2
from sqlalchemy import create_engine, text
import pysolr
import math
from textblob import TextBlob as tb


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