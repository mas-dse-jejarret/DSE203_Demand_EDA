from __future__ import print_function
import requests
import json
import pprint


class AsterixDataSource():

    def __init__(self, host="45.79.91.219", port=19002):
        self.headers = {'Content-type': 'application/x-www-form-urlencoded'}
        self.host = host
        self.port = port

    def execute(self, sql):
        connStr = 'http://{0}:{1}/query/service'.format(self.host, self.port)
        print(connStr)
        response = requests.post(connStr, data=sql, headers=self.headers)
        #clean empty properties
        jsonobj = json.loads(response.text.replace("{ ,", "{ "))
        return jsonobj

if __name__ == '__main__':
    pp = pprint.PrettyPrinter(depth=6)
    sql = """USE AstxDB;
            select *  from TableBT v;
    """

    ads = AsterixDataSource()
    jsonobj = ads.execute(sql)

    if jsonobj['status']== 'success':
        pp.pprint(jsonobj['results'])



