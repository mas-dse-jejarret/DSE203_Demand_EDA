from __future__ import print_function
import requests
import json
import pprint


def get_node_ids():
    stub="""
    { "nodeID": 2732 }
    { "nodeID": 11661 }
    { "nodeID": 2737 }
    { "nodeID": 11686 }
    { "nodeID": 10020691011 }
    { "nodeID": 3176 }
    { "nodeID": 3564978011 }
    { "nodeID": 8944243011 }
    { "nodeID": 8944244011 }
    { "nodeID": 3351944011 }
    { "nodeID": 69802 }
    { "nodeID": 69803 }
    { "nodeID": 69804 }
    { "nodeID": 15356861 }
    { "nodeID": 69805 }
    { "nodeID": 69806 }
    { "nodeID": 69807 }
    { "nodeID": 69808 }
    { "nodeID": 69809 }
    { "nodeID": 69810 }
    { "nodeID": 3190 }
    { "nodeID": 2917 }
    { "nodeID": 2918 }
    { "nodeID": 15356871 }
    { "nodeID": 2919 }
    { "nodeID": 2920 }
    { "nodeID": 69281 }
    { "nodeID": 2922 }
    { "nodeID": 2923 }
    { "nodeID": 2924 }
    { "nodeID": 2925 }
    { "nodeID": 2939 }
    { "nodeID": 2940 }
    { "nodeID": 2941 }
    { "nodeID": 2942 }
    { "nodeID": 2943 }
    { "nodeID": 2944 }
    { "nodeID": 2945 }
    { "nodeID": 2946 }
    { "nodeID": 2947 }
    { "nodeID": 2948 }
    { "nodeID": 2949 }
    { "nodeID": 8951191011 }
    { "nodeID": 2950 }
    { "nodeID": 2955 }
    { "nodeID": 2963 }
    { "nodeID": 2965 }
    { "nodeID": 3022 }
    { "nodeID": 3188 }
    { "nodeID": 3253 }
    { "nodeID": 3254 }
    { "nodeID": 3255 }
    { "nodeID": 3256 }
    { "nodeID": 3258 }
    { "nodeID": 3260 }
    { "nodeID": 171106 }
    { "nodeID": 3189 }
    { "nodeID": 3182 }
    { "nodeID": 3183 }
    { "nodeID": 3184 }
    { "nodeID": 3185 }
    { "nodeID": 3186 }
    { "nodeID": 10166936011 }
    { "nodeID": 3187 }
    { "nodeID": 7009082011 }
    { "nodeID": 3177 }
    { "nodeID": 3178 }
    { "nodeID": 16244021 }
    { "nodeID": 3179 }
    { "nodeID": 3180 }
    { "nodeID": 7009139011 }
    { "nodeID": 3206 }
    { "nodeID": 3192 }
    { "nodeID": 7009083011 }
    { "nodeID": 3209 }
    { "nodeID": 3210 }
    { "nodeID": 3211 }
    { "nodeID": 3212 }
    { "nodeID": 3214 }
    { "nodeID": 3215 }
    { "nodeID": 3216 }
    { "nodeID": 3217 }
    { "nodeID": 3220 }
    { "nodeID": 16244041 }
    { "nodeID": 3221 }
    { "nodeID": 3208 }
    { "nodeID": 3261 }
    { "nodeID": 3262 }
    { "nodeID": 3263 }
    { "nodeID": 8883929011 }
    { "nodeID": 8883930011 }
    { "nodeID": 3267 }
    { "nodeID": 3270 }
    { "nodeID": 3273 }
    { "nodeID": 3274 }
    { "nodeID": 16244061 }
    { "nodeID": 3278 }
    { "nodeID": 3279 }
    { "nodeID": 3280 }
    { "nodeID": 3283 }
    { "nodeID": 3301 }
    { "nodeID": 3200 }
    { "nodeID": 3201 }
    { "nodeID": 3203 }
    { "nodeID": 3204 }
    { "nodeID": 3344092011 }
    { "nodeID": 10367686011 }
    { "nodeID": 10367687011 }
    { "nodeID": 10367688011 }
    { "nodeID": 10367689011 }
    { "nodeID": 10379660011 }
    { "nodeID": 10367690011 }
    { "nodeID": 17433 }
    { "nodeID": 10367691011 }
    { "nodeID": 10367692011 }
    { "nodeID": 10367693011 }
    { "nodeID": 10367694011 }
    { "nodeID": 10367676011 }
    { "nodeID": 10367695011 }
    { "nodeID": 10367696011 }
    { "nodeID": 10367697011 }
    { "nodeID": 10367698011 }
    { "nodeID": 10367699011 }
    { "nodeID": 10367700011 }
    { "nodeID": 10367701011 }
    { "nodeID": 10367702011 }
    { "nodeID": 171118 }
    { "nodeID": 10367703011 }
    { "nodeID": 10367704011 }
    { "nodeID": 10367705011 }
    { "nodeID": 10367706011 }
    { "nodeID": 10367707011 }
    { "nodeID": 10367708011 }
    { "nodeID": 10367709011 }
    { "nodeID": 10367710011 }
    { "nodeID": 10367711011 }
    { "nodeID": 10367712011 }
    { "nodeID": 10367713011 }
    { "nodeID": 10367714011 }
    { "nodeID": 10367715011 }
    { "nodeID": 10367716011 }
    { "nodeID": 10367717011 }
    { "nodeID": 3344094011 }
    { "nodeID": 10367718011 }
    { "nodeID": 10367719011 }
    { "nodeID": 10367720011 }
    { "nodeID": 10367721011 }
    { "nodeID": 1099204 }
    { "nodeID": 10367722011 }
    { "nodeID": 10367723011 }
    { "nodeID": 10367724011 }
    { "nodeID": 10367725011 }
    { "nodeID": 10367726011 }
    { "nodeID": 17460 }
    { "nodeID": 10367727011 }
    { "nodeID": 10367728011 }
    { "nodeID": 10367729011 }
    { "nodeID": 17463 }
    { "nodeID": 10367730011 }
    { "nodeID": 1099194 }
    { "nodeID": 10367731011 }
    { "nodeID": 1099202 }
    { "nodeID": 10367732011 }
    { "nodeID": 10367733011 }
    { "nodeID": 10367734011 }
    { "nodeID": 10367736011 }
    { "nodeID": 10367737011 }
    { "nodeID": 1099206 }
    { "nodeID": 10367738011 }
    { "nodeID": 10367739011 }
    { "nodeID": 10367740011 }
    { "nodeID": 10367741011 }
    { "nodeID": 10367742011 }
    { "nodeID": 10367743011 }
    { "nodeID": 1099198 }
    { "nodeID": 10367744011 }
    { "nodeID": 10367745011 }
    { "nodeID": 10367746011 }
    { "nodeID": 10367747011 }
    { "nodeID": 10367748011 }
    { "nodeID": 10367749011 }
    { "nodeID": 1099200 }
    { "nodeID": 10367750011 }
    { "nodeID": 10367751011 }
    { "nodeID": 1099196 }
    { "nodeID": 10367752011 }
    { "nodeID": 10367753011 }
    { "nodeID": 10367754011 }
    { "nodeID": 10367756011 }
    { "nodeID": 10367755011 }
    { "nodeID": 10367757011 }
    { "nodeID": 10367758011 }
    { "nodeID": 10367759011 }
    { "nodeID": 10367760011 }
    { "nodeID": 10367761011 }
    { "nodeID": 10367762011 }
    { "nodeID": 3344093011 }
    { "nodeID": 8946314011 }
    { "nodeID": 8946313011 }
    { "nodeID": 10367764011 }
    { "nodeID": 8946315011 }
    { "nodeID": 8946316011 }
    { "nodeID": 8946317011 }
    { "nodeID": 8946318011 }
    { "nodeID": 10367765011 }
    { "nodeID": 10367768011 }
    { "nodeID": 10367771011 }
    { "nodeID": 10367774011 }
    """
    stub = stub.lstrip().rstrip()
    csv = ','.join([x for x in stub.split('\n') if len(x) > 0])
    #print (csv)
    json_str = '[{0}]'.format(csv)
    #print(json_str)
    return json.loads(json_str)


def convertToIn(_jlist):
    m = ','.join(["'{0}'".format(str(x["nodeID"])) for x in _jlist])
    #print(m)
    return '({0})'.format(m)




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
    # pp = pprint.PrettyPrinter(depth=6)
    # sql = """USE AstxDB;
    #         select *  from TableBT v;
    # """
    #
    # ads = AsterixDataSource()
    # jsonobj = ads.execute(sql)
    #
    # pp.pprint(jsonobj)
    #
    sds = SolrDataSource()
    jsonobj = sds.execute("*:*")
    #
    # pp.pprint(jsonobj)

    _jlist = get_node_ids()
    print(convertToIn(_jlist))



