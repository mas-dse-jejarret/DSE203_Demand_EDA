from werkzeug.contrib.cache import SimpleCache

from flask import Flask, request, jsonify, send_from_directory
import random
import time
import os
import json

cache = SimpleCache()


app = Flask(__name__) #, static_url_path='/static')

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/data")
def data():
    d = {"d" : random.randint(1, 100) }
    return jsonify(d)

@app.route("/data/<api_id>")
def data_api_id(api_id):
    v = cache.get(api_id)
    if v is None:
        v = random.randint(1, 100)
    d = {"d" : v }
    return jsonify(d)


@app.route("/save/<api_id>/<value>")
def save_api_value(api_id, value):
    cache.set(api_id, value, timeout=5 * 60)
    return jsonify(1)

@app.route("/get/<api_id>")
def get_api_value(api_id):
    value = cache.get(api_id);

    if value is None:
        value = 0
    return jsonify({ 'value' :  value })

@app.route("/cache")
def get_my_item():
    rv = cache.get('my-item')
    if rv is None:
        rv = random.randint(1, 100)
        cache.set('my-item', rv, timeout=60)
    else:
        print("already in")
    return jsonify(rv)

@app.route("/set")
def set():
    pass

if __name__ == '__main__':
    app.run(host= '0.0.0.0',port=8080,debug=True,use_reloader=True, threaded=True)