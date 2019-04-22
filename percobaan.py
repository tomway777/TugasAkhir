from ckanapi import RemoteCKAN
import pymongo as mg


conn = mg.MongoClient('localhost', 27017)
db = conn['TugasAkhir']

col = db['ListPemda']
# print(col.find_one())
listo = {}
for i in col.find():
    listo[i['id_pemda']] = i['linkPackage']
print(listo)

def writeMeta(data,id,name):
    post = db['meta']
    posts = {"id_pemda" : id,
             "meta" : data} #TODO: buat iterasi biar tidak langsung masuk objek
    post.insert(posts)


ua = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0'
rem = RemoteCKAN(listo.get(1), user_agent=ua)
meta = rem.action.package_show(id='acara-wisata-balai-kota')
# print(meta)

# serializer = RDFSerializer()
percobaan = serializer.serialize_dataset(meta, _format='xml')
# post = db['meta']
# posts = {'id_pemda' : 2,
#          'nama' : 'Gamal'}
# tes = {**posts, **meta}
# print(tes)
# post.insert(tes)
# print(type(meta))
# print(listo.get(1))