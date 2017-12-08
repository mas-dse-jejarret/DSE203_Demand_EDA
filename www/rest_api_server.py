from __future__ import print_function
from flask import Flask, request, jsonify
from data_exploration2 import *
from werkzeug.contrib.cache import SimpleCache

cache = SimpleCache()

app = Flask(__name__)

months=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

@app.route("/")
def root():
    return "Welcome to Data Exploration API!"

@app.route("/clearCache")
def clearCache():
    cache.clear()
    return "cleared"

@app.route("/api/highest_monthly_sales_by_category/<list>/<limit>")
def api_highest_monthly_sales_by_category(list, limit):
    category_list = list.split(",")

    hmsb = HighestMonthlySalesByCategory(category_list, limit)

    # print(hmsb)
    for item in hmsb:
        print(months[item['mon'] - 1])
    # return jsonify(hmsb)

    return(jsonify([ {"name" : months[item['mon'] - 1], "value" : item['num_sold']} for item in hmsb]))


@app.route("/api/top_sales_category/<ncategories>/<list>")
def api_top_sales_category(ncategories, list):
    key = "{0}:{1}".format(ncategories, list)
    tc = cache.get(key)
    if tc is None:
        month_list = [int(x) for x in list.split(",")]

        print(month_list)
        print(ncategories)

        tc = OptimizedTopCategories(int(ncategories), month_list)
        cache.set(key, tc, timeout=60 * 2)

    return (jsonify([{"name": item[0], "value": item[1]} for item in tc]))

@app.route("/api/top_sales_category2/<ncategories>/<list>")
def api_top_sales_category2(ncategories, list):
    month_list = [int(x) for x in list.split(",")]

    print(month_list)
    print(ncategories)

    tc = OptimizedTopCategories(int(ncategories), month_list)

    return (jsonify([{"name": item[0], "value": item[1]} for item in tc]))

@app.route("/api/correlation/<col1>/<col2>/<table1>/<table2>/<key1>/<key2>")
def api_correlation(col1, col2, table1, table2, key1, key2):
    col_pair = (col1,col2)
    table_pair = (table1,table2)
    key_pair = (key1,key2)

    retval=Correlation(col_pair, table_pair, key_pair)

    return(retval)

@app.route('/sentiment_polarity/<category>/<month>', methods=['GET'])
def sentiment_polarity(category, month):
    sr = Sales_Reviews(category, month)
    return sr


if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000)
