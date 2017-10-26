from flask import Flask, request

from middleware import WebSession
from middleware import VirtualIntegrationSchema
import json

app = Flask(__name__)


@app.route('/api/service', methods=['POST'])
def server():
    query = request.get_json(silent=True)

    # needs to change to reading from xml file
    xml = VirtualIntegrationSchema()

    web_session = WebSession(xml)
    return web_session.get_result_sets(query)


@app.route("/")
def hello():
    return "You shouldn't be here"

if __name__ == "__main__":
    app.run()