from db_connection import mongo_connection, mysql_connection
import pre_assessment as ps


init = mongo_connection('localhost', 27017)
db = init.connect('TugasAkhir')
listmeta = db['pemdaMetaNew']

templist = listmeta.find({'id_pemda' : 1})

def getPemdaById(value=None):
    templist = listmeta.find({'id_pemda' : value})
    return templist

def getCountMeta(value=None):
    templist = listmeta.count({'id_pemda' : value})
    return templist
#
temps = getPemdaById(value=1)
temp_access = 0
temp_discovery = 0
temp_preserv = 0
ds = ps.assessment()


for i in temps:
    temp_access += ps.assessment(i).as_access()
    temp_discovery += ps.assessment(i).as_discovery()
    #temp_preserv += ps.assessment(i).as_preserv()

#
# # for i in templist:
# #     print(i)

#connect to mysql
init_mysql = mysql_connection('gamalTA', 'adgjmptw12')
startdb = init_mysql.connect('tugasakhir')
cursor = startdb.cursor()
cursor.execute("SELECT * FROM pemdackan")

myresult = cursor.fetchall()


CountDataset = getCountMeta(1)
# # print(temp_access/CountDataset)
# print(temp_discovery/CountDataset)
# # print(temp_preserv/CountDataset)

