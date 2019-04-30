# from db_connection import mongo_connection
import re
# init = mongo_connection('localhost',27017)
# db = init.connect('TugasAkhir')

# listmeta = db['pemdaMetaNew']
# temp = listmeta.find({'id_pemda' : 1})
# temp1 = listmeta.find({'id_pemda' : 1}).__getitem__(0) #TODO: untuk percobaan individual
# print(temp)
# for i in temp:
#     print(i)

class assessment():
    def __init__(self, dict={}):
        self.newdict = dict

    def process_dict(self, var):
        new_dict = {}
        for k,v in self.newdict.items():
            if k == var:
                new_dict[k] = v
                return new_dict

    def as_access(self): #TODO : Perlu rata rata
        resources = self.process_dict('resources')
        tempNilaiMeta = 0
        countUrl = 0
        nilaiMetaPemda = 0
        for i in resources.values():
            for j in i:
                for k,v in j.items():
                    if k == 'url':
                        countUrl += 1
                        if v is not None:
                            tempNilaiMeta +=1
                        else:
                            tempNilaiMeta += 0
                    else:
                        tempNilaiMeta += 0
        nilaiMetaPemda = tempNilaiMeta/countUrl
        return nilaiMetaPemda
#---------------------------------------------------------------------------------#
    def as_discovery(self):  #TODO : Finished avg
        tempTitle =0
        tempNotes =0
        tempTags = 0
        countTags =0

        for k,v in self.newdict.items():
            if k == 'title' and v is not None:
                tempTitle += 1
            elif k == 'notes' and v is not None:
                tempNotes += 1
            else:
                tempNotes += 0
                tempTitle += 0
        try:
            var_tags = self.process_dict('tags')
            for j in var_tags.values():
                for i in j:
                    countTags += 1
                    for n,m in i.items():
                        if n == 'name' and m is not None:
                            tempTags += 1
                        else:
                            tempTags += 0
            avgTags = tempTags / countTags
        except:
            avgTags = 0
            #TODO : JANGAN KALAU VALUE = NULL MASUKAN KE TRY
        total_val = (avgTags + tempNotes + tempTitle)/3
        return total_val
#---------------------------------------------------------------------------------#
    def as_contact(self):
        var_authorMaintener = self.newdict
        countAuthor = 0
        countAuthorEmail = 0
        for k,v in var_authorMaintener.items():
            if k == 'author' and v is not None:
                countAuthor +=1
            elif k == 'author_email' and v is not None:
                countAuthorEmail +=1
            else:
                countAuthor += 0
                countAuthorEmail += 0
        totalMaintener = (countAuthor + countAuthorEmail)/2
        countpublisher = 0
        var_publisher = self.newdict
        for k,v in var_publisher.items():
            if k == 'organization':
                for i,j in v.items():
                    if i == 'title':
                        countpublisher += 1
            else:
                countpublisher += 0
        totalnilai = (totalMaintener + countpublisher)/2
        return totalnilai
#---------------------------------------------------------------------------------#
    def as_right(self):
        countval = 0        #TODO: rata2 setiap nilai
        rigth = self.newdict
        for k,v in rigth.items():
            if k == 'license_id' and v is not None:
                countval += 1
            else:
                countval += 0
        return countval
# ---------------------------------------------------------------------------------#
    def as_preserv(self):
        countAcc = 0
        try:    #TODO: Curr period still error
            var_accural = self.process_dict('extras')
            for i in var_accural.values():
                for j in i:
                    for k,v in j.items:
                        if k == 'frequency' and v is not None:
                            countAcc += 1
                        else:
                            countAcc += 0
        except:
            countAcc = 0
        
        countfor = 0
        counttype = 0
        countsize = 0
        count = 0
        var_format = self.process_dict('resources')
        for i in var_format.values():
            for j in i:
                count += 1
                for k,v in j.items():
                    if k == 'format' and v is not None:
                        countfor += 1
                    elif k == 'mimetype' and v is not None:
                        counttype += 1
                    elif k == 'size' and v is not None:
                        countsize += 1
                    else:
                        countfor += 0
                        counttype += 0
                        countsize += 0
        avgformat = countfor/count
        avgmime = counttype/count
        avgsize = counttype/count
        total = (avgformat + avgmime + avgsize)/3
        return total

# ---------------------------------------------------------------------------------#
    def as_date(self): #TODO: create dct dataset
        countIssued = 0 #issued = created
        countModified = 0 #modified = last modified
        count = 0
        var_dis = self.process_dict('resources')
        for i in var_dis.values():
            for j in i:
                count += 1
                for k,v in j.items():
                    if k == 'created' and v is not None:
                        countIssued +=1
                    elif k == 'last_modified' and v is not None:
                        countModified +=1
                    else:
                        countIssued += 0
                        countModified +=0
        avgIssued =countIssued/count
        avgMod = countModified/count
        total = (avgIssued + avgMod)/2
        return total
# ---------------------------------------------------------------------------------#
#Access Url = as_access() tidak perlu membuat lagi **periksa kevalidan TODO: ubah kodingan awal
    def as_access_validate(self): #TODO : Perlu rata rata
        regex = re.compile(
                r'^(?:http|ftp)s?://'  # http:// or https://
                r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
                r'localhost|'  # localhost...
                r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
                r'(?::\d+)?'  # optional port
                r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        resources = self.process_dict('resources')
        tempNilaiMeta = 0
        countUrl = 0
        nilaiMetaPemda = 0
        for i in resources.values():
            for j in i:
                countUrl += 1
                for k,v in j.items():
                    if k == 'url':
                        if re.match(regex,v) is not None:
                            tempNilaiMeta += 1
                        else:
                            tempNilaiMeta += 0
                    else:
                        tempNilaiMeta +=0
            nilaiMetaPemda = tempNilaiMeta/countUrl
        return nilaiMetaPemda
# ---------------------------------------------------------------------------------#
#Contact Url = as_contact tidak perlu buat lagi **periksa kevalidan
    def as_Contact_validation(self):
        var_contact = self.newdict
        countContact = 0
        for k,v in var_contact.items():
            if k == 'organization':
                for i,j in v.items():
                    if i == 'is_organization' and j is True:
                        countContact += 1
                    else:
                        countContact += 0
            else:
                countContact += 0
        return countContact
# ---------------------------------------------------------------------------------#
#TODO: Date format = 'YYYY - MM - DD'
    def as_date_validation(self):
        countdate = 0
        countmod = 0
        count = 0
        import dateutil.parser
        var_date = self.process_dict('resources')
        for i in var_date.values():
            for j in i:
                count += 1
                for k,v in j.items():
                    if k == 'created' and v is not None:
                        d = dateutil.parser.parse(v)
                        if d.strftime('%Y-%m-%d'):
                            countdate += 1
                    else:
                        countdate += 0
                    if k == 'last_modified' and v is not None:
                        d = dateutil.parser.parse(v)
                        if d.strftime('%Y-%m-%d'):
                            countmod += 1
                        else:
                            countmod += 0
                    else:
                        countmod += 0
        avgdate = countdate/count
        avgmod = countmod/count
        total = (avgdate + avgmod)/2
        return total
# ---------------------------------------------------------------------------------#
    #License Check
    def as_license_validate(self):   #TODO : Hitung rata - rata
        import pandas as pd
        datas = pd.read_csv('data/format-preprocess.csv')
        countlicense = 0
        ls = datas['id'].values.tolist()

        def validate(var=None):
            for i in ls:
                if var in i:
                    return True

        license = self.newdict
        for k,v in license.items():
            if k == 'license_id' and v is not None:
                if validate(v) is True:
                    countlicense +=1
                else:
                    countlicense += 0
        return countlicense
# ---------------------------------------------------------------------------------#
# File Format
    def as_format_validation(self):
        import pandas as pd
        countfor = 0
        counttype = 0
        dformat = pd.read_csv('data/IANA_formatfile.csv')
        var_formatval = self.process_dict('resources')

        def validate(value=None):
            if value in dformat.values:
                return True
            else:
                print('false')

        for i in var_formatval.values():
            for j in i:
                for k,v in j.items():
                    if k == 'format' and v is not None:
                        if validate(v.lower()) is True:
                            countfor = 1
                    elif k == 'mimetype' and v is not None:
                        if validate(v.lower()) is True:
                            counttype = 1
                    else:
                        counttype += 0
                        countfor +=0
        total = (countfor + counttype)/2
        return total
# ---------------------------------------------------------------------------------#
    def as_contactEmail_validate(self):
        var_contact = self.newdict
        regex = '^.+@([?)[a-zA-Z0-9-.]+.([a-zA-Z]{2,3}|[0-9]{1,3})(]?)$)'
        countValemail =0   #TODO: yang dicek cuma validitas email
        for k,v in var_contact.items():
            if k == 'author_email' and v is not None:
                if re.match(regex,v) is not None:
                    countValemail = 1
                else:
                    countValemail += 0
            else:
                countValemail += 0
        return countValemail

# ---------------------------------------------------------------------------------#
    #open format : dimensi Open Data
    def as_openformat_validate(self):
        var_formatval = self.process_dict('resources')
        countformat = 0
        countmime = 0
        listopenformat = [
            'ascii',
        'audio/mpeg','bmp','cdf','csv','csv.zip','dbf','gzip','html','iati','ical','ics','jpg',
        'dvi','geojson','geotiff','jpeg','json','kml','kmz','mpeg','netcdf','nt','ods','pdf','pdfa',
        'png','psv','psv.zip','rdf','rdfa','rss','rtf','sparql','svg','tar','tiff','tsv',
        'ttl', 'txt', 'wms', 'xml','xml.zip','zip'
        ]
        def isOpenFormat(value=None):
            for i in listopenformat:
                if i == value.lower():
                    return True

        for i in var_formatval.values():
            for j in i:
                for k,v in j.items():
                    if k == 'format' and v is not None:
                       if isOpenFormat(v) is True:
                           countformat = 1
                       else:
                           countformat += 0
                    elif k == 'mimetype' and v is not None:
                        if isOpenFormat(v) is True:
                            countmime = 1
                        else: countmime += 0
        total = (countformat + countmime)/2
        return total
# ---------------------------------------------------------------------------------#
#Machine Read : Terbaca Machine
    def as_machineRead_validate(self):
        listmachineformat= [
            'cdf', 'csv', 'csv.zip', 'esri', 'geojson', 'iati', 'ical', 'ics', 'json', 'kml', 'kmz', 'netcdf', 'nt',
            'ods', 'psv', 'psv.zip', 'rdf', 'rdfa','rss', 'shapefile', 'shp', 'shp.zip','sparql','tsv','ttl',
            'wms','xlb','xls','xls.zip','xlsx','xml','xml.zip'
        ]
        def isMachineReadable(value=None):
            for i in listmachineformat:
                if i == value.lower():
                    return True
        countformat = 0
        var_format = self.process_dict('resources')
        for i in var_format.values():
            for j in i:
                for k,v in j.items():
                    if k == 'format' and v is not None:
                       if isMachineReadable(v) is True:
                           countformat = 1
                       else:
                           countformat += 0
        return countformat
# ---------------------------------------------------------------------------------#
#opendef conplience license
    def as_OpenLicense_validation(self):
        import pandas as pd
        datas = pd.read_csv('data/format_compliance_prepro.csv')
        ls = datas['id'].values.tolist()

        countformat = 0
        var_license = self.newdict
        for k,v in var_license.items():
            if k == 'license_id' and v is not None:
                for i in ls:
                    if v in i:
                        countformat = 1
                    else:
                        countformat += 0
        return countformat



# temps = 0 #TODO : nilai menyimpan perthiungan dari access metadata
# disc =0
# for i in temp: #TODO : Iterasi semua nilai dataset
#     # temps =0
#     tes = assessment(i)
#     # temps += tes.as_access()
#     disc += tes.as_discovery()
# print(disc)
#Percobaan
# tes = assessment(temp1) #TODO: Tes def
# tem = tes.as_preserv()
# print(tem)

# tesa = {'id' : 'tes', 'tes' : ''}
# for i, j in tesa.items():
#     if len(j) > 0:
#         print(i,j)

