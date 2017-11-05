from __future__ import print_function
from flask import Flask, request, jsonify
from middleware import WebSession
from middleware import VirtualIntegrationSchema
from DataSources2 import AsterixDataSource
from DataSources2 import SolrDataSource

from sqlalchemy import create_engine, text
from datetime import date, datetime


import json


app = Flask(__name__)

@app.route("/")
def hello():
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
            print (c)
            d[c] = result[c]
        l.append(d)
    #
    theresult_json = json.dumps(l, default=json_serial)

    conn.close()

    return theresult_json


@app.route("/api/correlation/<col1>/<col2>")
def correlation(col1, col2):

    engine = create_engine('postgresql+psycopg2://postgres@45.79.91.219/MyBookStore')
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
	
@app.route("/api/covariance/<col1>/<col2>")
def covariance(col1, col2):
    """Determine the covariance coefficient between two columns."""

    engine = create_engine('postgresql+psycopg2://postgres@45.79.91.219/MyBookStore')
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
def histogram(groupby, count):

    engine = create_engine('postgresql+psycopg2://postgres@45.79.91.219/MyBookStore')
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


@app.route('/api/solrwrap', methods=['GET'])
def api_solrwrap():
    q = "*:*"

    ads = SolrDataSource()
    jsonobj = ads.execute(q)

    return jsonify(jsonobj)



if __name__ == "__main__":
    app.run(host='0.0.0.0',port=80)


