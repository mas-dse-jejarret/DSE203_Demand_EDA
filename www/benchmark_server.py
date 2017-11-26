from werkzeug.contrib.cache import SimpleCache

from flask import Flask, request, jsonify, send_from_directory
import random
import time
import os
import json
import numpy as np

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


def get_stack():
    stack = cache.get('stack')
    if stack is None:
        stack = []
        cache.set("stack", stack, timeout=60*5)
    return stack

def get_ave_cache():
    ave = cache.get('ave')
    if ave is None:
        ave = 0
        cache.set("ave", ave, timeout=60*5)
    return ave



@app.route("/save/<api_id>/<value>")
def save_api_value(api_id, value):
    stack = get_stack()
    cache.set(api_id, value, timeout=60*5)

    if len(stack) > 100:
        stack.pop(0)

    stack.append(float(value));
    cache.set("stack", stack, timeout=60*5)

    ave_cache = get_ave_cache()
    ave_cache = np.average([ave_cache, np.average(stack)])
    cache.set("ave", ave_cache, timeout=60*5)


    return jsonify(1)


@app.route("/get_stack_json")
def get_stack_json():
    stack = get_stack()
    ave = get_ave_cache()
    fave = "{0:.2f}".format(round(ave, 2))

    return jsonify({ 'series' : stack, 'average' : fave })


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