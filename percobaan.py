from ckanapi import RemoteCKAN
import pymongo as mg
from decimal import Decimal

import pre_assessment as ps
from db_connection import mongo_connection,mysql_connection
conn = mg.MongoClient('localhost', 27017)
db = conn['TugasAkhir']

# MYSQL
init_mysql = mysql_connection('gamalTA', 'adgjmptw12')
startdb = init_mysql.connect('tugasakhir')
cursor = startdb.cursor()
#------------------------------------------------Kategori Email ----------------------------------------------#
def getEmail(id=None):
    meta = db['pemdaMetaNew']
    ls = []
    import json
    pips = [
        # {'$unwind' : '$groups'},
        {'$match': {'id_pemda': id}},
        {'$group': {'_id': '$author_email', 'count': {'$sum': 1}}}
    ]
    ls = list(meta.aggregate(pips))

    def categorizeEmail(lis):
        res = {}
        emailResmi = "go.id"
        countResmi = 0
        countOther = 0
        for i in lis:
            if i['_id'] != '' and i['_id'] is not None:
                pan = len(i['_id'].split('@'))
                if pan > 1:
                    if i['_id'].split('@')[1].find(emailResmi) != -1:
                        print(i)
                        countResmi += 1
                        # print(i['_id'].split('@')[1])
                    else:
                        countOther += 1

        res['emailResmi'] = countResmi
        res['emailOther'] = countOther
        return res
    try:
        res = json.dumps(categorizeEmail(ls))
        return res
    except:
        print('error')

# for i in range(1,15,1):
#     print("Nomor : {} : {}".format(i, getEmail(i)))
# print(getEmail(13))
#------------------------------------------------Kategori Email ----------------------------------------------#
#------------------------------------------------Kategori License ----------------------------------------------#


def getLicense(id=None):
    meta = db['pemdaMetaNew']
    ls = []
    pips = [
        # {'$unwind' : '$groups'},
        {'$match': {'id_pemda': id}},
        {'$group': {'_id': '$license_id', 'count': {'$sum': 1}}}
    ]
    ls = list(meta.aggregate(pips))
    import json
    def categorizeLicense(lis):
        #lic - Other - None
        dctlic = {}
        valNone = 0
        valOther = 0

        for i in lis:
            # print(i)
            if i['_id'] == 'notspecified' or i['_id'] == None :
                valNone += i['count']
                # print(valNone)
            elif i['_id'].find('other') != -1:
                valOther += i['count']
                # print(valOther)
            else:
                dctlic[i['_id']] = i['count']
        if valOther != 0 or valNone != 0:
            dctlic['other'] = valOther
            dctlic['none'] = valNone
        return dctlic
    res = json.dumps(categorizeLicense(ls))
    return res

# for i in range(1,15,1):
#     print("Nomor : {} : {}".format(i, getLicense(i)))

#------------------------------------------------Kategori License ----------------------------------------------#
#------------------------------------------------Kategori Format ----------------------------------------------#

def getFormat(id=None):
    meta = db['pemdaMetaNew']
    ls = []
    import json
    pips = [
        {'$match': {'id_pemda': id}},
        {'$unwind': '$resources'},
        {'$group': {'_id': '$resources.format', 'count': {'$sum': 1}}}
    ]
    ls = list(meta.aggregate(pips))

    def categorizeFormat(lis):
        standfor = [
            "csv", "docx", "exe", "geojson", "html",
            "jpeg", "json", "none", "other", "pdf",
            "php", "png", "pptx", "rar",
            "rdf", "rtf", "svg", "txt", "webp",
            "xlsx", "xml", "zip"
        ]
        dctfor = {}
        valNone = 0
        for i in lis:
            if i['_id'] == '' or i['_id'] is None:
                valNone += i['count']
            elif standfor.__str__().find(i['_id'].lower()) != -1:
                dctfor[i['_id'].lower()] = i['count']
            else:
                print(i)
            dctfor['none'] = valNone
        return dctfor
    try:
        res = json.dumps(categorizeFormat(ls))
        return res
    except:
        print('error')

#------------------------------------------------Kategori Format ----------------------------------------------#

def getKategori(id=None):
    meta = db['pemdaMetaNew']
    ls = []
    import json
    pips = [
            {'$match': {'id_pemda': id}},
            {'$unwind': '$groups'},
            {'$group': {'_id': '$groups.title', 'count': {'$sum': 1}}}
        ]
    ls = list(meta.aggregate(pips))
    js = json.dumps(ls)
    return js

print(getEmail(3))
# for i in range(1,15,1):
#     if i == 1:
#         id = 31
#     elif i == 2:
#         id = 3273
#     elif i == 3:
#         id = 3271
#     elif i == 4:
#         id = 3374
#     elif i == 5:
#         id = 3372
#     elif i == 6:
#         id = 7371
#     elif i == 7:
#         id = 1571
#     elif i == 8:
#         id = 3371
#     elif i == 9:
#         id = 33
#     elif i == 10:
#         id = 1171
#     elif i == 11:
#         id = 11
#     elif i == 12:
#         id = 3324
#     elif i == 13:
#         id = 3329
#     elif i == 14:
#         id = 3373
#     filefor = getFormat(i)
#     licen = getLicense(i)
#     email = getEmail(i)
#     kat = getKategori(i)
#     print(email)
#     sql = "UPDATE pemdackan SET fileformattp = %s, licensetp=%s, emailtype=%s, kategori =%s WHERE id_pemda = "+str(id)
    # cursor.execute(sql, (filefor, licen, email, kat))
    # startdb.commit()








# pl = db['packagelist']
# {'crawl_at':'/^2019-06-12/'}
# for i in pl.find({'id' : 1}):
#     print(i)
#MYSQL
# init_mysql = mysql_connection('gamalTA', 'adgjmptw12')
# startdb = init_mysql.connect('tugasakhir')
# cursor = startdb.cursor(dictionary=True)

# sqlquery = "SELECT * FROM penliaiankualitas WHERE penilaianke = 2;"
# cursor.execute(sqlquery)
# select = cursor.fetchall()
#['access'] + $a['discovery'] + $a['right']+$a['contact'] +$a['preservation']+ $a['date']
# listdatax = []
# listdata = {}
# existence = 0
# conformance =0
# opendata = 0
# total = 0
# ae =1
# ac =1
# ao=1
# for i in select:
#     listdata['id_pemda'] = i['id_pemda']
#     listdata['penilaianke'] = i['penilaianke']
#     listdata['Existence'] = round((i['access'] + i['discovery'] + i['right']+i['contact'] +i['preservation']+ i['date'])/6,3)
#     listdata['Conformance'] = round((i['accessurl'] + i['contacturl'] + i['dateformat'] + i['license']+ i['fileformat'] + i['contactemail'])/6, 3)
#     listdata['OpenData'] = round((i['openformat'] + i['machineread'] + i['openlicense'])/3,3)
#     listdata['Total'] = round(ae*((i['access'] + i['discovery'] + i['right']+i['contact'] +i['preservation']+ i['date'])/6) +
#                               ac*((i['accessurl'] + i['contacturl'] + i['dateformat'] + i['license']+ i['fileformat'] + i['contactemail'])/6) +
#                               ao*((i['openformat'] + i['machineread'] + i['openlicense'])/3),3)
#     listdatax.append(listdata)
# #
# # print(listdatax)
#     sqlQueryIn = "INSERT INTO `penilaiandimensi` ( `id_pemda`,`penilaianke`,`Existence`,`Conformance`,`OpenData`, `Total`) " \
#                 "VALUES ( %(id_pemda)s,%(penilaianke)s, %(Existence)s, %(Conformance)s, %(OpenData)s, %(Total)s)"
#     cursor.executemany( sqlQueryIn, listdatax )
#     startdb.commit()


# print(pl.find_one({'crawl_at': '/2019-04-02/'}))
# for i in range(1, 15, 1):
#     print(i)


# import ssl
# import urllib.request as req
# gcontext = ssl.SSLContext()
# code = req.urlopen('http://opendata.makassar.go.id', context=gcontext).getcode()
# print(code)


# from validate_email import validate_email
# is_valid = validate_email('gamaltn@gmail.com', verify=True)
#
# print(is_valid)


# import urllib.request as req
# import json as jsn

# col = db['ListPemda']
# print(col.find_one())
# listo = {}
# for i in col.find():
#     listo[i['id_pemda']] = i['linkPackage']
# print(listo)

# init = mongo_connection('localhost',27017)
# db = init.connect('TugasAkhir')
#
# listmeta = db['pemdaMetaNew']
# temp = listmeta.find({'id_pemda' : 2}).count()
# temp1 = listmeta.find({'id_pemda' : 1}).__getitem__(2) #TODO: untuk percobaan individual

# for i in range(1,15,1):
#     temp = listmeta.find({'id_pemda':i}).count()
#     print(temp)
# #print(temp)



# listmeta.remove({'id_pemda' : 10})
# count = 0
# for i in temp:
#     count += ps.assessment(i).as_discovery()
#
# print(count)


# request = req.urlopen('http://data.bandaacehkota.go.id/index.php/api/action/package_show?id=14d4bebb-e763-44ee-bef4-c2dba7a35d1b')
# reads = jsn.load(request)
# print(reads['result'][0])

#
#
# tes = ps.assessment(temp1) #TODO: Tes def
# tem = tes.as_access_validate()
# print(tem)
# #
# listsa = {}
# listsa['access'] = 1
# print(listsa)

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
#
# import pycurl
# from io import BytesIO
#
# mail = 'gamaltn@gmail.com'
# buffer = BytesIO()
# c = pycurl.Curl()
# c.setopt(c.URL, 'http://app.ilead.io/api/verify?email='+mail)
# c.setopt(c.WRITEDATA, buffer)
# c.perform()
# c.close()
# body = buffer.getvalue()
# print(body.decode('iso-8859-1'))


# from smtplib import SMTP
# address_to_test = "gamaltn@gmail.com"
#
# try:
#     with SMTP('smtp.gmail.com') as smtp:
#         host_exists = True
#         smtp.helo() # send the HELO command
#         smtp.mail('tom.way777@gmail.com') # send the MAIL command
#         resp = smtp.rcpt("gamaltn@gmail.com")
#         if resp[0] == 250: # check the status code
#             deliverable = True
#             print('True')
#         elif resp[0] == 550:
#             deliverable = False
#         else:
#             print(resp[0])
# except :
#     print("SMTP connection error")


# import smtplib
# gmail_user = 'dmpbfp@gmail.com'
# gmail_pass = 'adgjmptw12'
# try:
#     server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
#     server.ehlo()
#     server.login(gmail_user, gmail_pass)
#     # server.mail(gmail_user)
#     sent = server.sendmail(gmail_user,'tadfs.s77@gmail.com', 'hai')
#     print('success')
# except:
#     print('error')


# from smtplib import SMTP
# import smtplib
# address_to_test = "data@bandung.go.id"
#
# try:
#     with SMTP('gmail-smtp-in.l.google.com') as smtp:
#         host_exists = True
#         smtp.helo() # send the HELO command
#         smtp.mail('dmpbfp@gmail.com') # send the MAIL command
#         resp = smtp.rcpt(address_to_test)
#         if resp[0] == 250: # check the status code
#             deliverable = True
#         elif resp[0] == 550:
#             deliverable = False
#         else:
#             print(resp[0])
# except smtplib.SMTPServerDisconnected as err:
#     print("SMTP connection error")


# try:
#     server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
#     # server = smtplib.SMTP_SSL('gmail-smtp-in.l.google.com', 465)
#     server.login('dmpbfp@gmail.com', 'adgjmptw12')
#     server.mail('dmpbfp@gmail.com')
#     # sent = server.sendmail(gmail_user,'tadfs.s77@gmail.com', 'hai')
#     resp = server.rcpt('a@a.com')
#     print(resp)
#     if resp[0] == 250:  # check the status code
#         deliverable = True
#         print('True')
#     elif resp[0] == 550:
#         deliverable = False
#     else:
#         print(resp[0])
#
# except:
#     print('error')


# from validate_email import validate_email
# is_valid = validate_email('gamaltn@gmail.com',check_mx=True)

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


# listmeta = db['pemdaMetaNew']

# ===================================
# listseq = []
# for i in tqdm(range(1, 15, 1), desc='Penilaian Metadata : '):
#     print(i)
#     try:
#         listseq.append(assesBatchMeta(i))
#     except:
#         print("err")
# pemda_1 = assesBatchMeta(1)
# pemda_2 = assesBatchMeta(2)
# pemda_3 = assesBatchMeta(3)
# # pemda_4 = assesBatchMeta(4)
# pemda_5 = assesBatchMeta(5)
# pemda_6 = assesBatchMeta(6)
# pemda_7 = assesBatchMeta(7)
# pemda_8 = assesBatchMeta(8)
# pemda_9 = assesBatchMeta(9)
# pemda_10 = assesBatchMeta(10)
# pemda_10["penilaianke"] = 5 # Traking penilaian keberapa
# pemda_11 = assesBatchMeta(11)
# pemda_12 = assesBatchMeta(12)
# # pemda_13 = assesBatchMeta(13)
# pemda_14 = assesBatchMeta(14)
# listseq.append(pemda_1)
# listseq.append(pemda_2)
# listseq.append(pemda_3)
# listseq.append(pemda_4)
# listseq.append(pemda_5)
# listseq.append(pemda_6)
# listseq.append(pemda_7)
# listseq.append(pemda_8)
# listseq.append(pemda_9)
# listseq.append(pemda_10)
# listseq.append(pemda_11)
# listseq.append(pemda_12)
# # listseq.append(pemda_13)
# listseq.append(pemda_14)
# print(listseq)
# # print(listseq)
# sqlQuery = "INSERT INTO `penliaiankualitas` ( `penilaianke`,`id_pemda`, `access`, `discovery`, `contact`, `right`, `preservation`," \
#             "`date`,`accessurl`,`contacturl`,`dateformat`,`license`,`fileformat`,`contactemail`,`openformat`,`machineread`,`openlicense`) " \
#             "VALUES ( %(penilaianke)s,%(id_pemda)s, %(access)s, %(discovery)s, %(contact)s, %(right)s, %(preservation)s," \
#             " %(date)s, %(accessurl)s, %(contacturl)s, %(dateformat)s, %(license)s, %(fileformat)s, %(contactemail)s" \
#             ",%(openformat)s, %(machineread)s, %(openlicense)s )"
# cursor.executemany( sqlQuery, listseq)
# startdb.commit()