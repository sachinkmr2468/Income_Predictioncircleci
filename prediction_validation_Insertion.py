from datetime import datetime
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import dbOperation
from DataTransformation_Prediction.DataTransformationPrediction import dataTransformPredict
from application_logging.logger import App_Logger
ob = App_Logger()

class pred_validation:
    def __init__(self):
        self.raw_data = Prediction_Data_validation()
        self.dataTransform = dataTransformPredict() # dataTransformPredict is a class called from module datatransformationprediction
        self.dbOperation = dbOperation()
        self.file_object = open(r'B:\project\pythonmyproject\Prediction_Logs\Prediction_Log.txt', 'a+')


    def prediction_validation(self):

        try:
            ob.log(self.file_object,'Start of Validation on files for prediction!!')

            #extracting values from prediction schema
            #valuesFromSchema is a method called from predictiondatavalidation
            LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,noofcolumns = self.raw_data.valuesFromSchema()
            #getting the regex defined to validate filename
            #manualRegexCreation method called from predictiondatavalidation class
            regex = self.raw_data.manualRegexCreation()
            #validating filename of prediction files
            #validationFileNameRaw method called from predictiondatavlidation
            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            #validating column length in the file
            #validateColumnLength mthod from class predictiondatavalidation
            self.raw_data.validateColumnLength(noofcolumns)
            #validating if any column has all values missing
            #method validateMissingValuesInWholeColumn from class predictiondatavalidation class
            self.raw_data.validateMissingValuesInWholeColumn()
            #writtng log to the file
            ob.log(self.file_object,"Raw Data Validation Complete!!")

#writtng log to the file
            ob.log(self.file_object,("Starting Data Transforamtion!!"))

            #replacing blanks in the csv file with "Null" values to insert in table
            #calling replaceMissingWithNull method from data tranfrom class
            self.dataTransform.replaceMissingWithNull()
#writing log to a file
            ob.log(self.file_object,"DataTransformation Completed!!!")

#writing log to a file
            ob.log(self.file_object,"Creating Prediction_Database and tables on the basis of given schema!!!")

          #create database with given name, if present open the connection! Create table with columns given in schema
            #here we need to do same things with cassenddra
            self.dbOperation.createTableDb()
            ob.log(self.file_object,"Table creation Completed!!")
            ob.log(self.file_object,"Insertion of Data into Table started!!!!")

            #insert csv files in the table
            self.dbOperation.insertIntoTableGoodData()
            ob.log(self.file_object,"Insertion in Table completed!!!")
            ob.log(self.file_object,"Deleting Good Data Folder!!!")

            #Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            ob.log(self.file_object,"Good_Data folder deleted!!!")

            ob.log(self.file_object,"Moving bad files to Archive and deleting Bad_Data folder!!!")

            #Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            ob.log(self.file_object,"Bad files moved to archive!! Bad folder Deleted!!")
            ob.log(self.file_object,"Validation Operation completed!!")
            ob.log(self.file_object,"Extracting csv file from table")

            #export data in table to csvfile
            self.dbOperation.selectingDatafromtableintocsv()
            self.file_object.close()

        except Exception as e:
            raise e








