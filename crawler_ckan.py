import pymongo as mongo
from ckanapi import RemoteCKAN
import datetime
from tqdm import tqdm
import urllib.request as req
import json as jsn


now = datetime.datetime.now()

#connect DB
try:
    conn = mongo.MongoClient('localhost', 27017)
    db = conn['TugasAkhir']
except:
    print('Periksa Sambungan Mongo')

#read collection TODO: ambil link dan id
lp = db['ListPemda']
listPemda = {}
for i in lp.find():
    listPemda[i['id_pemda']] = i['linkPackage']

#ckan get package
ua = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0'

def getPackage(aList,id):
    try:
        ckan = RemoteCKAN(aList, user_agent=ua)
        package = ckan.action.package_list()
        writeDB(package,id)
    except:
        print('\n ada kesalahan')

# TODO : Untuk setiap percobaan ubah collection agar tidak ada ganguan data utama -> write
def writeDB(data,id):
    post = db['packagelist1']
    posts = {"id_pemda" : id,
             "crawl_at" :  now,
             "package" : data}
    post.insert(posts)

#gabungkan link dan package
def mergePackage():
    temp = {}
    pl = db['packagelist1']
    for i in pl.find():
        temp [i['id_pemda']] = i['package']
    link_pack = {}
    for k,v in listPemda.items():
        for i,j in temp.items():
            if k == i:
                link_pack[v] = j
    return link_pack

#get metadata
def getMetadata(key, value, id):
    try:
        ckan = RemoteCKAN(key, user_agent=ua)
        show = ckan.action.package_show(id=value)
        writeMeta(show,id,value)
    except:
        print('\n Terjadi kesalahan')

def getMetadataOldApi(key, value, id, link):
    urls = link + '/api/action/package_show?id=' + value
    request = req.urlopen(urls)
    reads = jsn.load(request)
    results = reads['result'][0]
    writeMeta(results,id,value)

def writeMeta(data,id,name):
    post = db['pemdaMetaNew1']
    posts = {"id_pemda" : id,
             "crawl_at" :  now,
             "nama" : name}
    temp = {**posts, **data}
    post.insert(temp)



if __name__ == '__main__':
    import pprint
    while True:
        print('Crawler CKAN : \n 1. Lihat list CKAN Pemda \n 2. Ambil Package Pemda \n 2.1. Ambil Package Pemda Tertentu \n 3. Ambil Metadata CKAN Pemda Tertentu \n 4. Ambil Semua Metadata CKAN Pemda')
        pilihan = input('Masukan pilihan : ')
        if pilihan == '1':
            print('list ckan : ')
            pprint.pprint(listPemda)
        elif pilihan == '2':
            print('Ambil Package List Pemda  \n')
            for k,v in tqdm(listPemda.items(), desc='Get Package list'):
                getPackage(v,k)

        elif pilihan == '2.1':
            pemdapil = input('masukan id : ')
            for k,v in listPemda.items():
                if int(pemdapil) == k:
                    getPackage(v,k)

        elif pilihan == '3':
            pemdapilihan = input('masukan id : ')
            tipeckan = input('tipe ckan : ')
            if tipeckan == 'old':
                for i,j in listPemda.items():
                    if int(pemdapilihan) == i:
                        lispackage = mergePackage()
                        for k,v in lispackage.items():
                            for a in tqdm(range(len(v))):
                                if j == k:
                                    getMetadataOldApi(k,v[i],i,j)
            else:
                for i,j in listPemda.items():
                    if int(pemdapilihan) == i:
                        lispackage = mergePackage()
                        for k,v in lispackage.items():
                            for a in tqdm(range(len(v))):
                                if j == k:
                                    getMetadata(k,v[i],i)

        else:
            print("ckan")
            tes = mergePackage()
            for a,b in listPemda.items():
                for k,v in tes.items():
                    for i in tqdm(range(len(v)), desc='Download Metadata'):
                        if b == k:
                            getMetadata(k,v[i],a)

        print('Crawler Ckan: ', pilihan)
        print('\n ========================================================== \n')
