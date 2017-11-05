from __future__ import print_function
import requests
import json
import pprint

#create solr data source
class SolrDataSource():

    def __init__(self, host="45.79.91.219", port=8983):
        self.host = host
        self.port = port

    def execute(self, q):
        connstr = 'http://{0}:{1}/solr/bookstore/select?q={2}'.format(self.host, self.port, q)
        # print(connstr)
        response = requests.get(connstr)

        jsonobj = json.loads(response.text)

        if jsonobj['responseHeader']['status'] == 0:
            return jsonobj['response']['docs']


class AsterixDataSource():

    def __init__(self, host="45.79.91.219", port=19002):
        self.headers = {'Content-type': 'application/x-www-form-urlencoded'}
        self.host = host
        self.port = port

    def execute(self, _sql):
        connstr = 'http://{0}:{1}/query/service'.format(self.host, self.port)

        response = requests.post(connstr, data=_sql, headers=self.headers)
        #clean empty properties
        jsonobj = json.loads(response.text.replace("{ ,", "{ "))

        if jsonobj['status'] == 'success':
            return jsonobj['results']
        else:
            return json.dumps("[]")

if __name__ == '__main__':
    pp = pprint.PrettyPrinter(depth=6)
    sql = """USE AstxDB;
            select *  from TableBT v;
    """

    ads = AsterixDataSource()
    jsonobj = ads.execute(sql)

    pp.pprint(jsonobj)

    sds = SolrDataSource()
    jsonobj = sds.execute("*:*")

    pp.pprint(jsonobj)



