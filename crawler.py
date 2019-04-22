import urllib.request as req
import json as jsn
import pymongo as mongo
import datetime
import  pprint
import ssl



#connect to MongoDB
connection = mongo.MongoClient()
connection = mongo.MongoClient('localhost', 27017)
db = connection['TugasAkhir']
col = db['ListPemda']
gcontext = ssl.SSLContext()

ListPemda = {}
try:
    empCol = col.find()
    for emp in empCol:
        ListPemda [emp['id_pemda']] = emp['linkPackage']
except Exception as e:
    print (str(e))

# Iterate
# for k,v in ListPemda.items():
#     print(k)


def writePackage(json,id):
    post = db['package_list']
    listPack = []
    for res in json['result']:
        listPack.append(res)
    posts = {"id_pemda": id,
             "crawl_at" : datetime.datetime.now(),
            "packageList" : json['result']}
    post.insert(posts)

def getPackage(aList,id):
    request = req.urlopen(aList, context=gcontext)
    reads = jsn.load(request)
    writePackage(reads,id)

def readPackage():
    pl = db['package_list']
    col = pl.find()
    listPackage = {}
    arrayPa = []
    for i in col:
        arrayPa.append(i['packageList'])
    return  arrayPa

if __name__ == '__main__':
    # print(readPackage())
    request = req.urlopen('http://opendata.makassar.go.id')
    reads = jsn.load(request)
    print(reads)

