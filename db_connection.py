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
    def __init__(self,username,password):
        self.username = username
        self.password = password

    def connect(self, dbname):
        connect = msql.connect(
                host = 'localhost',
                user = self.username,
                passwd = self.password,
                database = dbname
            )
        return connect

