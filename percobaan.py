from ckanapi import RemoteCKAN
import pymongo as mg
import pre_assessment as ps
from db_connection import mongo_connection
conn = mg.MongoClient('localhost', 27017)
db = conn['TugasAkhir']

# col = db['ListPemda']
# print(col.find_one())
# listo = {}
# for i in col.find():
#     listo[i['id_pemda']] = i['linkPackage']
# print(listo)

init = mongo_connection('localhost',27017)
db = init.connect('TugasAkhir')

listmeta = db['pemdaMetaNew']
# temp = listmeta.find({'id_pemda' : 1})
temp1 = listmeta.find({'id_pemda' : 2}).__getitem__(0) #TODO: untuk percobaan individual



tes = ps.assessment(temp1) #TODO: Tes def
tem = tes.as_openformat_validate()
print(tem)
# list = [
# 'cc-by-4.0',
# 'cc-by-sa-4.0',
# 'cc0-1.0'
# ]
#
# for i in list:
#     if 'cc-by' in i:
#         print('py')
# # ls = [1,2,3,5,6]
# if 5 in ls:
#     print('ok')












# def writeMeta(data,id,name):
#     post = db['meta']
#     posts = {"id_pemda" : id,
#              "meta" : data} #TODO: buat iterasi biar tidak langsung masuk objek
#     post.insert(posts)
#
#
# ua = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0'
# rem = RemoteCKAN(listo.get(1), user_agent=ua)
# meta = rem.action.package_show(id='acara-wisata-balai-kota')
# print(meta)

# serializer = RDFSerializer()
# percobaan = serializer.serialize_dataset(meta, _format='xml')
# post = db['meta']
# posts = {'id_pemda' : 2,
#          'nama' : 'Gamal'}
# tes = {**posts, **meta}
# print(tes)
# post.insert(tes)
# print(type(meta))
# print(listo.get(1))