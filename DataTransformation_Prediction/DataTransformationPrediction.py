from datetime import datetime
from os import listdir #needs to write the code in my way
import pandas
from application_logging.logger import App_Logger

ob = App_Logger()
import os
class dataTransformPredict:

     """
                  This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.
                  """
     def __init__(self):
          self.goodDataPath = r"B:\project\pythonmyproject\Prediction_Raw_Files_Validated\Good_Raw" # give the path



     def replaceMissingWithNull(self):

          """
                                  Method Name: replaceMissingWithNull
                                  Description: This method replaces the missing values in columns with "NULL" to
                                               store in the table. We are using substring in the first column to
                                               keep only "Integer" data for ease up the loading.
                                               This column is anyways going to be removed during prediction.
                                          """
          try:
               log_file = open(r'B:\project\pythonmyproject\Prediction_Logs\dataTransformLog.txt','a+')
               source = self.goodDataPath
               for file in os.listdir(source):
                    data = pandas.read_csv(self.goodDataPath + "/" + file)
                    # list of columns with string datatype variables
                    columns = ['workclass','education','maritalstatus','occupation','relationship','race','sex','nativecountry']

                    for col in columns:
                         data[col] = data[col].apply(lambda x: "'" + str(x) + "'") #putting single quotes to all the string data present in all the columns
                    data.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
               ob.log(log_file,'File Transformed successfully!!')
               log_file.close()

          except Exception as e:
               log_file = open(r'B:\project\pythonmyproject\Prediction_Logs\dataTransformLog.txt','a+')
               ob.log(log_file,"Data Transformation failed because:: %s" % e)
               log_file.close()
               raise e

