from flask import Flask, request, jsonify

from middleware import WebSession
from middleware import VirtualIntegrationSchema
from DataSources2 import AsterixDataSource
from DataSources2 import SolrDataSource

import json

app = Flask(__name__)


@app.route('/api/service', methods=['POST'])
def server():
    query = request.get_json(silent=True)

    # needs to change to reading from xml file
    xml = VirtualIntegrationSchema()

    web_session = WebSession(xml)
    return web_session.get_result_sets(query)

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

@app.route("/")
def hello():
    return "You shouldn't be here"

if __name__ == "__main__":
    app.run()