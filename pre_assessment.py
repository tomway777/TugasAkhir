from db_connection import mongo_connection
import re
init = mongo_connection('localhost',27017)
db = init.connect('TugasAkhir')

listmeta = db['pemdaMetaNew']
temp = listmeta.find({'id_pemda' : 1})
temp1 = listmeta.find({'id_pemda' : 1}).__getitem__(2) #TODO: untuk percobaan individual
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
        nilaiMetaPemda = tempNilaiMeta/countUrl
        return nilaiMetaPemda
#---------------------------------------------------------------------------------#
    def as_discovery(self):  #TODO: perlu rata-rata setiap nilai tags, title, notes
        tempTitle =0
        tempNotes =0
        tempTags =0
        var_tags = self.process_dict('tags')
        for k,v in self.__dict__.items():
            if k == 'title' and len(v) > 0:
                # print(v)
                tempTitle += 1
                # print(tempTitle)
                continue
            elif k == 'notes' and len(v) > 0:
                # print(v)
                tempNotes += 1
                # print(tempNotes)
                continue
        for i,j in var_tags.items():
            for v in j:
                # print(v)
                for k,n in v.items():
                    if k == 'name' and len(n) > 0:
                        tempTags += 1
        # print(tempTitle,tempTags,tempNotes)
        return tempTags + tempTitle +tempNotes
#---------------------------------------------------------------------------------#
    def as_contact(self):
        var_extra = self.process_dict('extras')
        for v in var_extra:
            print(v)
#---------------------------------------------------------------------------------#
    def as_right(self):
        countval = 0        #TODO: rata2 setiap nilai
        rigth = self.newdict
        for k,v in rigth.items():
            if k == 'license_id' and v is not None:
                countval += 1
        return countval
# ---------------------------------------------------------------------------------#
    def as_preserv(self):
        try:    #TODO: Curr period still error
            var_accural = self.process_dict('extras')
            for i in var_accural:
                print(i)
        except:
            print('None')
        countfor = 0
        counttype = 0
        countsize = 0
        var_format = self.process_dict('resources')
        for i in var_format.values():
            for j in i:
                for k,v in j.items():
                    if k == 'format' and v is not None:
                        countfor += 1
                    elif k == 'mimetype' and v is not None:
                        counttype += 1
                    elif k == 'size' and v is not None:
                        countsize += 0
        return countsize, counttype, countfor

# ---------------------------------------------------------------------------------#
    def as_date(self): #TODO: create dct dataset
        countIssued = 0 #issued = created
        countModified = 0 #modified = last modified
        var_dis = self.process_dict('resources')
        for i in var_dis.values():
            for j in i:
                for k,v in j.items():
                    if k == 'created' and v is not None:
                        countIssued +=1
                    elif k == 'last_modified' and v is not None:
                        countModified +=1
        return countModified, countIssued
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
                for k,v in j.items():
                    if k == 'url':
                        countUrl += 1
                        if re.match(regex,v) is not None:
                            tempNilaiMeta += 1
            nilaiMetaPemda = tempNilaiMeta/countUrl
        return nilaiMetaPemda
# ---------------------------------------------------------------------------------#
#Contact Url = as_contact tidak perlu buat lagi **periksa kevalidan
# ---------------------------------------------------------------------------------#
#TODO: Date format = 'YYYY - MM - DD'
    def as_date_validation(self):
        countdate = 0
        countmod = 0
        import dateutil.parser
        var_date = self.process_dict('resources')
        for i in var_date.values():
            for j in i:
                for k,v in j.items():
                    if k == 'created' and v is not None:
                        d = dateutil.parser.parse(v)
                        if d.strftime('%Y-%m-%d'):
                            print('true')
                            countdate += 1
                    if k == 'last_modified' and v is not None:
                        d = dateutil.parser.parse(v)
                        if d.strftime('%Y-%m-%d'):
                            countmod += 1
        return countdate, countmod

# ---------------------------------------------------------------------------------#
    #License Check
    def as_license_validate(self):   #TODO : Hitung rata - rata
        listacceptedformat = [
            'cc-zero', 'odc-pddl', 'cc-by', 'odc-by', 'cc-by-sa', 'odc-odbl',
            'against-drm', 'by-2-0', 'zero-2-0', 'dsl', 'eff-open-audio-license',
            'fal', 'gfdl', 'miros'
        ]
        countlicense = 0
        license = self.newdict
        for k,v in license.items():
            for i in listacceptedformat:
                if k == 'license_id':
                    if v == i:
                        countlicense +=1
        return countlicense




# temps = 0 #TODO : nilai menyimpan perthiungan dari access metadata
# disc =0
# for i in temp: #TODO : Iterasi semua nilai dataset
#     # temps =0
#     tes = assessment(i)
#     # temps += tes.as_access()
#     disc += tes.as_discovery()
# print(disc)

tes = assessment(temp1) #TODO: Tes def
tem = tes.as_license_validate()
print(tem)

# tesa = {'id' : 'tes', 'tes' : ''}
# for i, j in tesa.items():
#     if len(j) > 0:
#         print(i,j)

