#Take the refrence from main sorce and write code for cassandera
from datetime import datetime
from os import listdir
import os
import csv
from application_logging.logger import App_Logger
import cassandra
import shutil
import pandas as pd
#!pip install cassandra-driver

ob = App_Logger()


class dbOperation:
    def __init__(self):
        pass

    def databaseConnection(self):
        try:
            from cassandra.cluster import Cluster
            from cassandra.auth import PlainTextAuthProvider

            cloud_config = {
                'secure_connect_bundle': r'B:\project\kumar\secure-connect-test1.zip'
            }
            auth_provider = PlainTextAuthProvider('pNlNfEEFSOrdfmlPYMgKqiYA','xfZZt0TZFX+aK6q.6kL8J4PJoiWg.9duB0_a1Mc9dGNR8ZMGI.DiKR4rkBYn4swPe+wg_vneHEb_4E9MjRMZwzIUvCp5StSoElZg,Y5.0k28ZXA7TwazXA_yAZ2SHYlw')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            row = session.execute("select release_version from system.local").one()
            file = open(r'B:\project\pythonmyproject\Prediction_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, 'connection was successful')
        except ConnectionError:
            file = open(r'B:\project\pythonmyproject\Prediction_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Error while connecting to database: %s" % ConnectionError)
            file.close()
            raise ConnectionError
        return session

    def createTableDb(self):
        conn = self.databaseConnection()
        try:
            row = conn.execute(
                "CREATE TABLE IF NOT EXISTS test2.Good_Raw_Data(age int PRIMARY KEY,workclass text,fnlwgt int,education text,educationsnum int,maritalstatus text,occupation text,relationship text,race text,sex text,capitalgain int,capitalloss int,hoursperweek int,nativecountry text);").one()
            file = open(r'B:\project\pythonmyproject\Prediction_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Tables created successfully!!")
            file.close()
        except Exception as e:
            file = open(r'B:\project\pythonmyproject\Prediction_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Error while creating table: %s " %e)
            file.close()

    #    def insertIntoTableGoodData(self): this will just move single csv file  to cassandra
    #        conn = self.databaseConnection()
    #        try:
    #            with open(r'B:\project\Data\adult4.csv','r') as data:
    #                next(data)

    #                data_csv= csv.reader(data)
    #                print(data_csv)
    #                for i in data_csv:
    #                    conn.execute("insert into test1.student41 (age,workclass,fnlwgt,education) VALUES(%s,%s,%s,%s)",[int(i[0]),(i[1]),int(i[2]),(i[3])])
    #                print('Finished')
    #                file = open(r'C:\Users\nihca\Documents\project\DataBaseConnectionLog.txt','a+')
    #                ob.log(file,"data loaded successfully!!")
    #                file.close()

    #        except:
    #            file = open(r'C:\Users\nihca\Documents\project\DataBaseConnectionLog.txt','a+')
    #            ob.log(file, "Error while creating table: %s " % e)
    #            file.close()
    def insertIntoTableGoodData(self):
        conn = self.databaseConnection()
        goodfilepath = r'B:\project\pythonmyproject\Prediction_Raw_Files_Validated\Good_Raw'  # give the path of the prediction path where good raw folder was created
        try:
            for i in os.listdir(goodfilepath):
                with open(goodfilepath + '/' + i, 'r') as f:
                    next(f)
                    csv_reader = csv.reader(f)
                    for line in csv_reader:
                        conn.execute("insert into test2.Good_Raw_Data (age,workclass,fnlwgt,education,educationsnum,maritalstatus,occupation,relationship,race,sex,capitalgain,capitalloss,hoursperweek,nativecountry) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                     [int(line[0]), (line[1]), int(line[2]), (line[3]),int(line[4]),(line[5]),(line[6]),(line[7]),(line[8]),(line[9]),int(line[10]),int(line[11]),int(line[12]),(line[13])])
                    print('Finished')
                    file = open(r'C:\Users\nihca\Documents\project\Prediction_Logs\DataBaseConnectionLog.txt', 'a+')
                    ob.log(file, "data loaded successfully!!")
                    file.close()
        except Exception as e:
            file = open(r'B:\project\pythonmyproject\Prediction_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Error while creating table: %s " % e)
            file.close()

    def selectingDatafromtableintocsv(self):
        conn = self.databaseConnection()
        try:
            fileName = r'B:\project\pythonmyproject\Prediction_FileFromDB\'InputFile.csv'
            field1 = ['age','workclass','fnlwgt','education','educationsnum','maritalstatus','occupation','relationship','race','sex','capitalgain','capitalloss','hoursperweek','nativecountry']
            rows = conn.execute("select * from test2.Good_Raw_Data")
            with open(fileName, 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(field1)
                csvwriter.writerows(rows)
            file = open(r'B:\project\pythonmyproject\Prediction_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "data loaded to csv successfully!!")
            file.close()
        except Exception as e:
            file = open(r'B:\project\pythonmyproject\Prediction_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Error while extracting data: %s " % e)
            file.close()