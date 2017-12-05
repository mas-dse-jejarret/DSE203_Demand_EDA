from __future__ import print_function
from flask import Flask, request, jsonify
from data_exploration2 import *

import json

app = Flask(__name__)

months=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

@app.route("/")
def root():
    return "Welcome to Data Exploration API!"

@app.route("/api/highest_monthly_sales_by_category/<list>/<limit>")
def api_highest_monthly_sales_by_category(list, limit):
    category_list = list.split(",")
    hmsb = HighestMonthlySalesByCategory(category_list, limit)

    # print(hmsb)
    for item in hmsb:
        print(months[item['mon'] - 1])


    # return jsonify(hmsb)

    return(jsonify([ {"name" : months[item['mon'] - 1], "value" : item['num_sold']} for item in hmsb]))

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
    app.run(host='0.0.0.0',port=81)
