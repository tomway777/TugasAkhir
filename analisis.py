from db_connection import mongo_connection
import pre_assessment as ps
init = mongo_connection('localhost', 27017)
db = init.connect('TugasAkhir')
listmeta = db['pemdaMetaNew1']


#Pertitungan jumlah resource dan metadata setiap portal open data
#------------------------------- Count -----------------------------------#
def getNumresource(id_pemda):
    temp = listmeta.find({'id_pemda' : id_pemda})
    count = 0
    for i in temp:
        count += ps.assessment(i).getNumResource()
    return count


def getCountMeta(value=None):
    templist = listmeta.count({'id_pemda' : value})
    return templist

listmetas = []
for i in range(1,15,1):
    listmetas.append(getCountMeta(i))
print(listmetas)


lisdata = []
for i in range(1,15,1):
    lisdata.append(getNumresource(i))#
print(lisdata)

# Analisis Konten Metadata pada setiap dataset portal open data

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