from application_logging.logger import App_Logger
from datetime import datetime
import cassandra
import os
import shutil
import pandas as pd
from os import listdir
import csv

ob = App_Logger()

class dbOperation:
    """
      This class shall be used for handling all the cassandra operations.
      """

    def __init__(self):
        pass

    def databaseConnection(self):

        """
                Method Name: dataBaseConnection
                Description: This method set up connection with cassandra cloud
                Output: Connection to the DB
                On Failure: Raise ConnectionError
                """
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
            file = open(r'B:\project\pythonmyproject\Training_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, 'connection was successful')
        except ConnectionError:
            file = open(r'B:\project\pythonmyproject\Training_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Error while connecting to database: %s" % ConnectionError)
            file.close()
            raise ConnectionError
        return session

    def createTableDb(self):
        """
                                Method Name: createTableDb
                                Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
                                Output: None
                                On Failure: Raise Exception
                                """
        conn = self.databaseConnection()
        try: #need to define the column names and its datatype before running
            row = conn.execute(
                "CREATE TABLE IF NOT EXISTS test2.good_raw(age int PRIMARY KEY,workclass text,fnlwgt int,education text,educationsnum int,maritalstatus text,occupation text,relationship text,race text,sex text,capitalgain int,capitalloss int,hoursperweek int,nativecountry text,income text);").one()
            file = open(r'C:\Users\nihca\Documents\project\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Tables created successfully!!")
            file.close()
        except Exception as e:
            file = open(r'C:\Users\nihca\Documents\project\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Error while creating table: %s " % e)
            file.close()

    def insertIntoTableGoodData(self):
        """
                                       Method Name: insertIntoTableGoodData
                                       Description: This method inserts the Good data files from the Good_Raw folder into the
                                                    above created table.
                                       Output: None
                                       On Failure: Raise Exception
                """
        conn = self.databaseConnection()
        goodfilepath = r'B:\project\pythonmyproject\Training_Raw_files_validated\Good_Raw'
        try:
            for i in os.listdir(goodfilepath):
                with open(goodfilepath + '/' + i, 'r') as f:
                    next(f)
                    csv_reader = csv.reader(f)
                    for line in csv_reader:
                        conn.execute("insert into test2.good_raw (age,workclass,fnlwgt,education,educationsnum,maritalstatus,occupation,relationship,race,sex,capitalgain,capitalloss,hoursperweek,nativecountry,income) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                     [int(line[0]), (line[1]), int(line[2]), (line[3]),int(line[4]),(line[5]),(line[6]),(line[7]),(line[8]),(line[9]),int(line[10]),int(line[11]),int(line[12]),(line[13]),(line[14])])
                    print('Finished')
                    file = open(r'B:\project\pythonmyproject\Training_Logs\DataBaseConnectionLog.txt', 'a+')
                    ob.log(file, "data loaded successfully!!")
                    file.close()
        except Exception as e:
            file = open(r'B:\project\pythonmyproject\Training_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Error while creating table: %s " % e)
            file.close()

    def selectingDatafromtableintocsv(self):
        """
                                       Method Name: selectingDatafromtableintocsv
                                       Description: This method exports the data in GoodData table as a CSV file. in a given location.
                                                    above created .
                                       Output: None
                                       On Failure: Raise Exception
                """
        conn = self.databaseConnection()
        try:
            fileName = r'B:\project\pythonmyproject\Training_FileFromDB\'InputFiless.csv'
            field1 = ['age','workclass','fnlwgt','education','educationsnum','maritalstatus','occupation','relationship','race','sex','capitalgain','capitalloss','hoursperweek','nativecountry','income']
            row = conn.execute("select * from test2.good_raw")
            with open(fileName, 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(field1)
                csvwriter.writerows(row)
            file = open(r'B:\project\pythonmyproject\Training_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "data loaded to csv successfully!!")
            file.close()
        except Exception as e:
            file = open(r'B:\project\pythonmyproject\Training_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Error while extracting data: %s " % e)
            file.close()