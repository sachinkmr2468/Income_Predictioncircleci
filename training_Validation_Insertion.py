from datetime import datetime
from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTypeValidation_Insertion_Training.DataTypeValidation import dbOperation
from DataTransform_Training.DataTransformation import dataTransform
from application_logging.logger import App_Logger

ob = App_Logger()

class train_validation:
    def __init__(self):
#clling Raw_Data_validation class and passing its attributes as path, actually created object of raw_data_validation class
        self.raw_data = Raw_Data_validation()
#calling datatransform class
        self.dataTransform = dataTransform()
#calling db operation class
        self.dbOperation = dbOperation()

        self.file_object = open(r"B:\project\pythonmyproject\Training_Logs\Training_Main_Log.txt", 'a+') #storing all the logs



    def train_validation(self):
        try:
            #calling log_writer.log and saving the log in main_log file , log file is used via self.file_object
            #calling log method from App_Logger class
            ob.log(self.file_object, 'Start of Validation on files for training!!')

            # extracting values from prediction schema
            #here we calling the method valuesfromschema from raw_data_validaton class,self.raw_data = Raw_Data_validation(path), and extracting it values and storing in diff variables
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate filename
            #self.raw_data = Raw_Data_validation(path), means calling manualRegexCreation method from Raw_data_validation
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            #validationFileNameRaw method having attributes (- regex, LengthOfDateStampInFile, LengthOfTimeStampInFile) called from raw_data_validation(path),self.raw_data refer raw_data_validation
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # validating column length in the file
            #validateColumnLength method (attributes - noofcolumns)called from raw_data_validation class
            self.raw_data.validateColumnLength(noofcolumns)
            # validating if any column has all values missing
            #calling validateMissingValuesInWholeColumn() method from raw_data_validation
            self.raw_data.validateMissingValuesInWholeColumn()
            #log is the method from App logger class, self.file_object refer to the file main_log where we storing the log
            ob.log(self.file_object, "Raw Data Validation Complete!!")
            ob.log(self.file_object, "Starting Data Transforamtion!!")


            # replacing blanks in the csv file with "Null" values to insert in table
            # replaceMissingWithNull() method called from data tranform class, self.dataTransform refer class data transform
            self.dataTransform.replaceMissingWithNull()
            #calling log from app_logger class
            ob.log(self.file_object, "DataTransformation Completed!!!")

            #calling log from app_logger class
            ob.log(self.file_object,"Creating Training_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            #calling method createTableDb from class dboperations
            self.dbOperation.createTableDb() #little modification needed here
            #calling log from app_logger class
            ob.log(self.file_object, "Table creation Completed!!")

            #calling log from app_logger class
            ob.log(self.file_object, "Insertion of Data into Table started!!!!")

            # insert csv files in the table
            #calling insertIntoTableGoodData method from dboperation class
            self.dbOperation.insertIntoTableGoodData()
            ##calling log from app_logger class
            ob.log(self.file_object, "Insertion in Table completed!!!")
            ob.log(self.file_object, "Deleting Good Data Folder!!!")

            # Delete the good data folder after loading files in table
            #calling deleteExistingGoodDataTrainingFolder from raw_data_validation class
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            ob.log(self.file_object, "Good_Data folder deleted!!!")
            ob.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")

            # Move the bad files to archive folder
            #calling moveBadFilesToArchiveBad() method from raw_data_validation class
            self.raw_data.moveBadFilesToArchiveBad()
            ob.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            ob.log(self.file_object, "Validation Operation completed!!")
            ob.log(self.file_object, "Extracting csv file from table")

            # export data in table to csvfile
            #calling selectingDatafromtableintocsv method have value as Training from dboperation class
            self.dbOperation.selectingDatafromtableintocsv()
            #closing the main_log file now.
            self.file_object.close()

        except Exception as e:
            raise e









