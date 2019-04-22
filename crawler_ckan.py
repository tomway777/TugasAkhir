import pymongo as mongo
from ckanapi import RemoteCKAN
import datetime
from tqdm import tqdm



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

#write to DB
def writeDB(data,id):
    post = db['packagelist']
    posts = {"id_pemda" : id,
             "crawl_at" :  now,
             "package" : data}
    post.insert(posts)

# for k,v in listPemda.items():
#     getPackage(v)
# print(type(getPackage(listPemda[3])))

# print(now.year)
# getPackage(listPemda[3],3)

#gabungkan link dan package
def mergePackage():
    temp = {}
    pl = db['packagelist']
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

def writeMeta(data,id,name):
    post = db['pemdaMetaNew']
    posts = {"id_pemda" : id,
             "crawl_at" :  now,
             "nama" : name} #TODO: coba ulangi crawling untuk besok!!!
    temp = {**posts, **data}
    post.insert(temp)
    # tes = mergePackage()
    # print(tes['http://data.jakarta.go.id'])



if __name__ == '__main__':
    # te = mergePackage()
    # print(te.values())
    # for i in listPemda.values():
    #     print(i)
    #TODO: Test hasil dari crawling metadata
    while True:
        print('Crawler CKAN : \n 1. Lihat list CKAN Pemda \n 2. Ambil Package Pemda \n 3. Ambil Metadata CKAN Pemda')
        pilihan = input('Masukan pilihan : ')
        if pilihan == '1':
            print('list ckan : \n', listPemda)
        elif pilihan == '2':
            print('Ambil Package List Pemda  \n')
            for k,v in tqdm(listPemda.items(), desc='Get Package list'):
                getPackage(v,k)
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

    # tes = mergePackage()
    # for k,v in tes.items():
    #     for i in tqdm(range(len(v)), desc='Download Metadata'):
    #         getMetadata(k,v[i],1)
