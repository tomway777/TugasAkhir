from pymongo.mongo_client import MongoClient
import mysql.connector as msql

class mongo_connection:
    def __init__(self, host, port):
        self.host = host
        self.port = port


    def connect(self,dbname):
        try:
            conn = MongoClient(self.host,self.port)
            db = conn[dbname]
        except:
            print('Error, check your localhost/port')
        return db

class mysql_connection:
    def __init__(self,host,username,password,dbname):
        self.host = host
        self.username = username
        self.password = password
        self.dbname = dbname
    def connect(self, host, username, password):
        try:
            conn = msql.connect(
                host = host,
                username = username,
                password = password
            )
        except:
            print("Error Connection")
        return conn
