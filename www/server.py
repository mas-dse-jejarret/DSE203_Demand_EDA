from __future__ import print_function
from flask import Flask, request, jsonify
from middleware import WebSession
from middleware import VirtualIntegrationSchema
from sqlalchemy import create_engine, text



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


@app.route("/api/correlation/<col1>/<col2>")
def correlation(col1, col2):

    engine = create_engine('postgresql+psycopg2://postgres@45.79.91.219/MyBookStore')
    conn = engine.connect()

    sql = """
    select corr(o.numunits, p.productid::int)
    from orderlines o, products p
    where o.productid = p.productid
    """

    stmt = text(sql)

    result = conn.execute(stmt)

    for row in result:
        print(row[0])
    conn.close()

    return row[0]

@app.route("/histogram/<string:name>/")
def histogram(name):
    return name


@app.route('/api/add_message/<uuid>', methods=['GET', 'POST'])
def add_message(uuid):
    content = request.get_json(silent=True)
    print (content)
    return jsonify('{"h" : "ok"}')

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=80)


