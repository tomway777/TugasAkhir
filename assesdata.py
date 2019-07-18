from db_connection import mongo_connection, mysql_connection
import pre_assessment as ps
from tqdm import tqdm

#-----------------------Inisialisasi DB & Kelas Penilaian----------------------------#
# TODO : Ganti ke collection lama untuk list meta
init = mongo_connection('localhost', 27017)
db = init.connect('TugasAkhir')
listmeta = db['pemdaMetaNew']
ds = ps.assessment()

#MYSQL
init_mysql = mysql_connection('gamalTA', 'adgjmptw12')
startdb = init_mysql.connect('tugasakhir')
cursor = startdb.cursor()

#-----------------------Method-----------------------------------------#

def getPemdaById(value=None):
    templist = listmeta.find({'id_pemda' : value})
    return templist

def getCountMeta(value=None):
    templist = listmeta.count({'id_pemda' : value})
    return templist

def fillDatasetResource(idpemda):
    tempMeta = getCountMeta(idpemda)
    listmetadata = listmeta.find({'id_pemda' : idpemda})
    for i in listmetadata:
        tempResource = ps.assessment(i).getNumResource()

def assesBatchMeta(id_pemda):
    #Dimensi Existence
    temp_access = 0
    temp_discovery = 0
    temp_contact = 0
    temp_right = 0
    temp_preserv = 0
    temp_date = 0

    #Dimensi Conformance
    temp_accessURL = 0
    temp_contactURL = 0
    temp_dateFormat = 0
    temp_license = 0
    temp_fileFormat = 0
    temp_contactEmail = 0

    #Dimensi OpenData
    temp_openFormat = 0
    temp_machineRead = 0
    temp_openLicense = 0

    #Hitung Total metadata
    countMeta = getCountMeta(id_pemda)
    # countMeta = 2000

    pemdaValuation = {}
    metaPemda = getPemdaById(id_pemda)
    #-----------------------------------#
    for i in tqdm(metaPemda, desc='Penilaian Metriks'):
        temp_access += ps.assessment(i).as_access()
        temp_discovery += ps.assessment(i).as_discovery()
        temp_contact += ps.assessment(i).as_contact()
        temp_right += ps.assessment(i).as_right()
        temp_preserv += ps.assessment(i).as_preserv()
        temp_date += ps.assessment(i).as_date()
        # -------------------------------------------- #
        temp_accessURL += ps.assessment(i).as_access_validate()
        temp_contactURL += ps.assessment(i).as_Contact_validation()
        temp_dateFormat += ps.assessment(i).as_date_validation()
        temp_license += ps.assessment(i).as_license_validate()
        temp_fileFormat += ps.assessment(i).as_format_validation()
        temp_contactEmail += ps.assessment(i).as_contactEmail_validate()
        # -------------------------------------------- #
        temp_openFormat += ps.assessment(i).as_openformat_validate()
        temp_machineRead += ps.assessment(i).as_machineRead_validate()
        temp_openLicense += ps.assessment(i).as_OpenLicense_validation()

    #Masukan Ke Dictionary
    if id_pemda == 1:
        pemdaValuation['id_pemda'] = 31
    elif id_pemda == 2:
        pemdaValuation['id_pemda'] = 3273
    elif id_pemda == 3:
        pemdaValuation['id_pemda'] = 3271
    elif id_pemda == 4:
        pemdaValuation['id_pemda'] = 3374
    elif id_pemda == 5:
        pemdaValuation['id_pemda'] = 3372
    elif id_pemda == 6:
        pemdaValuation['id_pemda'] = 7371
    elif id_pemda == 7:
        pemdaValuation['id_pemda'] = 1571
    elif id_pemda == 8:
        pemdaValuation['id_pemda'] = 3371
    elif id_pemda == 9:
        pemdaValuation['id_pemda'] = 33
    elif id_pemda == 10:
        pemdaValuation['id_pemda'] = 1171
    elif id_pemda == 11:
        pemdaValuation['id_pemda'] = 11
    elif id_pemda == 12:
        pemdaValuation['id_pemda'] = 3324
    elif id_pemda == 13:
        pemdaValuation['id_pemda'] = 3329
    elif id_pemda == 14:
        pemdaValuation['id_pemda'] = 3373


    try:
        pemdaValuation['access'] = (temp_access/countMeta)
        pemdaValuation['discovery'] = (temp_discovery/countMeta)
        pemdaValuation['contact'] = (temp_contact/countMeta)
        pemdaValuation['right'] = (temp_right/countMeta)
        pemdaValuation['preservation'] = (temp_preserv/countMeta)
        pemdaValuation['date'] = (temp_date/countMeta)
        #------------------------------------------#
        pemdaValuation['accessurl'] = (temp_accessURL/countMeta)
        pemdaValuation['contacturl'] = (temp_contactURL/countMeta)
        pemdaValuation['dateformat'] = (temp_dateFormat/countMeta)
        pemdaValuation['license'] = (temp_license/countMeta)
        pemdaValuation['fileformat'] = (temp_fileFormat/countMeta)
        pemdaValuation['contactemail'] = (temp_contactEmail/countMeta)
        #------------------------------------------#
        pemdaValuation['openformat'] = (temp_openFormat/countMeta)
        pemdaValuation['machineread'] = (temp_machineRead/countMeta)
        pemdaValuation['openlicense'] = (temp_openLicense/countMeta)
    except:
        print("error")

    # Output
    return pemdaValuation


#-----------------------Percobaan-----------------------------------------#












if __name__ == '__main__':
    listseq = []
    print('#################################### \n Penilaian Kualitas Portal Data')
    print(' 1. Penilaian semua Portal Open Data \n 2. Simpan Ke Database \n 3.Penilaian Spesifik ')
    while True:
        pilihan = input('Masukan input : ')
        if pilihan == '1' :
            for i in tqdm(range(1,15,1), desc='Penilaian Metadata : '):
                listseq.append(assesBatchMeta(i))
            print(listseq)
        if pilihan == '3':
            ins = input('Masukan id portal :')
            listseq.append(assesBatchMeta(int(ins)))
        if pilihan == '2':
            sqlQuery = "INSERT INTO `penliaiankualitas` ( `id_pemda`, `access`, `discovery`, `contact`, `right`, `preservation`," \
                       "`date`,`accessurl`,`contacturl`,`dateformat`,`license`,`fileformat`,`contactemail`,`openformat`,`machineread`,`openlicense`) " \
                       "VALUES ( %(id_pemda)s, %(access)s, %(discovery)s, %(contact)s, %(right)s, %(preservation)s," \
                       " %(date)s, %(accessurl)s, %(contacturl)s, %(dateformat)s, %(license)s, %(fileformat)s, %(contactemail)s" \
                       ",%(openformat)s, %(machineread)s, %(openlicense)s )"
            cursor.executemany( sqlQuery, listseq )
            startdb.commit()
try:
    pemda_10 = assesBatchMeta(10)
except Exception as e:
    print(e)



# pemda_14 = assesBatchMeta(14)
# pemda_13 = assesBatchMeta(13)
#
# listseq.append(pemda_14)
# listseq.append(pemda_13)
#
# print(listseq)
#
# # connect to mysql
# sqlQuery = "INSERT INTO `penliaiankualitas` ( `id_pemda`, `access`, `discovery`, `contact`, `right`, `preservation`," \
#            "`date`,`accessurl`,`contacturl`,`dateformat`,`license`,`fileformat`,`contactemail`,`openformat`,`machineread`,`openlicense`) " \
#            "VALUES ( %(id_pemda)s, %(access)s, %(discovery)s, %(contact)s, %(right)s, %(preservation)s," \
#            " %(date)s, %(accessurl)s, %(contacturl)s, %(dateformat)s, %(license)s, %(fileformat)s, %(contactemail)s" \
#            ",%(openformat)s, %(machineread)s, %(openlicense)s )"
# print(sqlQuery)
#
# cursor.executemany( sqlQuery, listseq )
# startdb.commit()

# cursor.execute("SELECT * FROM pemdackan")
#
# myresult = cursor.fetchall()
#

# CountDataset = getCountMeta(1)
# print(CountDataset)
# # print(temp_access/CountDataset)
# print(temp_discovery/CountDataset)
# # print(temp_preserv/CountDataset)

