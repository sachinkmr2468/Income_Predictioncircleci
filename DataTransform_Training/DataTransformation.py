from datetime import datetime
from os import listdir
import os
import pandas as  pd
from application_logging.logger import App_Logger

ob = App_Logger()
# this code will work fine

'''This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.
'''
"""
                        Method Name: replaceMissingWithNull
                        Description: This method replaces the missing values in columns with "NULL" to
                                     store in the table. We are using substring in the first column to
                                     keep only "Integer" data for ease up the loading.
                                     This column is anyways going to be removed during prediction.
                                """


class dataTransform:
     def __init__(self):
         self.goodDataPath = r'C:\Users\nihca\Documents\project\Training_Raw_files_validated\Good_Raw'

     def replaceMissingWithNull(self):
        log_file = open(r"B:\project\pythonmyproject\Training_Logs\dataTransformLog.txt", 'a+')
        try:
            source = self.goodDataPath
               #onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in os.listdir(source):
                data = pd.read_csv(self.goodDataPath + "/" + file)
                 # list of columns with string datatype variables
                columns = ['workclass','education','maritalstatus','occupation','relationship','race','sex','nativecountry','income']
                for col in columns:
                    data[col] = data[col].apply(lambda x: "'" + str(x) + "'")
                    data.to_csv(self.goodDataPath + "/" + file, index=None, header=True)
                    ob.log(log_file, " %s: Quotes added successfully!!" % file)
                  # log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")

        except Exception as e:
            ob.log(log_file, "Data Transformation failed because:: %s" % e)
              # log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
            log_file.close()
            log_file.close()




