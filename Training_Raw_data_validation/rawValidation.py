from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger

ob = App_Logger()

dicts = {"SampleFileName": "incomeData_021119920_010222.csv",
"LengthOfDateStampInFile": 9,
"LengthOfTimeStampInFile": 6,
"NumberofColumns" : 15,
"ColName": {
"age" : "Integer",
"workclass": "varchar",
"fnlwgt": "Integer",
"education": "varchar",
"education-num": "Integer",
"marital-status": "varchar",
"occupation": "varchar",
"relationship": "varchar",
"race": "varchar",
"sex": "varchar",
"capital-gain": "Integer",
"capital-loss": "Integer",
"hours-per-week": "Integer",
"native-country": "varchar",
"Income": "varchar"
}

        }

class Raw_Data_validation:

    """
             This class shall be used for handling all the validation done on the Raw Training Data!!.
             """

    def __init__(self):
        self.schema_path = (r'B:\project\pythonmyproject\schema_training.json')



    def valuesFromSchema(self):
        """
                        Method Name: valuesFromSchema
                        Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                        Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                        On Failure: Raise ValueError,KeyError,Exception
                                """
        try:
            with open(self.schema_path, 'w') as f:
                f.write(json.dumps(dicts))
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open(r"B:\project\pythonmyproject\Training_Logs\valuesfromSchemaValidationLog.txt", 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            ob.log(file, message)


            file.close()



        except ValueError:
            file = open(r"B:\project\pythonmyproject\Training_Logs\valuesfromSchemaValidationLog.txt", 'a+')
            ob.log(file,"ValueError:Value not found inside schema_training.json")

            file.close()
            raise ValueError

        except KeyError:
            file = open(r"B:\project\pythonmyproject\Training_Logs\valuesfromSchemaValidationLog.txt", 'a+')
            ob.log(file, "KeyError:Key value error incorrect key passed")

            file.close()
            raise KeyError

        except Exception as e:
            file = open(r"B:\project\pythonmyproject\Training_Logs\valuesfromSchemaValidationLog.txt", 'a+')
            ob.log(file,str(e))

            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):
        """
                                Method Name: manualRegexCreation
                                Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                            This Regex is used to validate the filename of the training data.
                                Output: Regex pattern
                                On Failure: None
                                        """
        regex = "['incomeData']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
                                      Method Name: createDirectoryForGoodBadRawData
                                      Description: This method creates directories to store the Good Data and Bad Data
                                                    after validating the training data.

                                      Output: None
                                      On Failure: OSError
                                              """

        try:
            directory = 'Good_Raw'
            parent_dir = r'B:\project\pythonmyproject\Training_Raw_files_validated'
            path = os.path.join(parent_dir, directory)
            if not os.path.isdir(path):
                os.makedirs(path)
            directory1 = 'Bad_Raw'
            parent_dir1 = r'B:\project\pythonmyproject\Training_Raw_files_validated'
            path = os.path.join(parent_dir1, directory1)
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open(r"B:\project\pythonmyproject\Training_Logs\GeneralLog.txt", 'a+')
            ob.log(file,"Error while creating Directory %s:" % ex)

            file.close()
            raise OSError

    def deleteExistingGoodDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingGoodDataTrainingFolder
                                            Description: This method deletes the directory made  to store the Good Data
                                                          after loading the data in the table. Once the good files are
                                                          loaded in the DB,deleting the directory ensures space optimization.
                                            Output: None
                                            On Failure: OSError
                                                    """
        try:
            location = r'B:\project\pythonmyproject\Training_Raw_files_validated'
            dirs = 'Good_Raw'
            path = os.path.join(location, dirs)
            if os.path.isdir(path):
                shutil.rmtree(path)
                file = open(r"B:\project\pythonmyproject\Training_Logs\GeneralLog.txt", 'a+')
                ob.log(file,"GoodRaw directory deleted successfully!!!")

                file.close()
        except OSError as s:
            file = open(r"B:\project\pythonmyproject\Training_Logs\GeneralLog.txt", 'a+')
            ob.log(file,"Error while Deleting Directory : %s" %s)

            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingBadDataTrainingFolder
                                            Description: This method deletes the directory made to store the bad Data.
                                            Output: None
                                            On Failure: OSError
                                                    """

        try:
            location = r'B:\project\pythonmyproject\Training_Raw_files_validated'
            dirs = 'Bad_Raw'
            path = os.path.join(location, dirs)
            if os.path.isdir(path):
                shutil.rmtree(path)
                file = open(r"B:\project\pythonmyproject\Training_Logs\GeneralLog.txt", 'a+')
                ob.log(file,"BadRaw directory deleted before starting validation!!!")

                file.close()
        except OSError as s:
            file = open(r"B:\project\pythonmyproject\Training_Logs\GeneralLog.txt", 'a+')
            ob.log(file,"Error while Deleting Directory : %s" %s)

            file.close()
            raise OSError

    def moveBadFilesToArchiveBad(self):

        """
                                            Method Name: moveBadFilesToArchiveBad
                                            Description: This method deletes the directory made  to store the Bad Data
                                                          after moving the data in an archive folder. We archive the bad
                                                          files to send them back to the client for invalid data issue.
                                            Output: None
                                            On Failure: OSError
                                                    """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source = r'B:/project/pythonmyproject/Training_Raw_files_validated/Bad_Raw/'
            directory = 'BadData''_' + str(date) + "_" + str(time)
            dirs = 'Bad_Raw/'
            parent_dir = (r'B:/project/pythonmyproject/TrainingArchiveBadData/')
            dest = os.path.join(parent_dir, directory)
            if os.path.isdir(source):
                print('Good to proceed')
                if not os.path.isdir(parent_dir):
                    os.makedirs(parent_dir)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                filesall = os.listdir(source)
                for f in filesall:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest + f)
                file = open(r"B:\project\pythonmyproject\Training_Logs\GeneralLog.txt", 'a+')
                ob.log(file,"Bad files moved to archive")

                path = 'Training_Raw_files_validated/'
                if os.path.isdir(dest):
                    shutil.rmtree(dest)
                ob.log(file,"Bad Raw Data Folder Deleted successfully!!")

                file.close()
        except Exception as e:
            file = open(r"B:\project\pythonmyproject\Training_Logs\GeneralLog.txt", 'a+')
            ob.log(file, "Error while moving bad files to archive:: %s" % e)

            file.close()
            raise e




    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
                    Method Name: validationFileNameRaw
                    Description: This function validates the name of the training csv files as per given name in the schema!
                                 Regex pattern is used to do the validation.If name format do not match the file is moved
                                 to Bad Raw Data folder else in Good raw data.
                    Output: None
                    On Failure: Exception
                """


        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        #create new directories
        self.createDirectoryForGoodBadRawData()
        try:
            path = (r'B:\project\pythonmyproject\Training_Batch_Files') #here will keep the files for checking name
            for i in os.listdir(path): #iterating over dir and checking filename convention one by one
                if (re.match(regex, i)):
                    splitAtDot = re.split('.csv',i)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy((r'B:\project\pythonmyproject\Training_Batch_Files\\' + i ), (r'B:\project\pythonmyproject\Training_Raw_files_validated\Good_Raw'))
                            file = open(r"B:\project\pythonmyproject\Training_Logs\nameValidationLog.txt", 'a+')
                            ob.log(file,"Good files moved to archive" + i)


                        else:
                            shutil.copy((r'B:\project\pythonmyproject\Training_Batch_Files\\' + i ), (r'B:\project\pythonmyproject\Training_Raw_files_validated\Bad_Raw'))
                            file = open(r"B:\project\pythonmyproject\Training_Logs\nameValidationLog.txt", 'a+')
                            ob.log(file, "Bad files moved to archive" + i)


                    else:
                        shutil.copy((r'B:\project\pythonmyproject\Training_Batch_Files\\' + i ), (r'B:\project\pythonmyproject\Training_Raw_files_validated\Bad_Raw'))
                        file = open(r"B:\project\pythonmyproject\Training_Logs\nameValidationLog.txt", 'a+')
                        ob.log(file, "Bad files moved to archive" + i)


                else:
                    shutil.copy((r'B:\project\pythonmyproject\Training_Batch_Files\\' + i ), (r'B:\project\pythonmyproject\Training_Raw_files_validated\Bad_Raw'))
                    file = open(r"B:\project\pythonmyproject\Training_Logs\nameValidationLog.txt", 'a+')
                    ob.log(file, "Bad files moved to archive" + i)
                file.close()




        except Exception as e:
            f = open(r"B:\project\pythonmyproject\Training_Logs\nameValidationLog.txt", 'a+')
            ob.log(f, "Error occured while validating FileName %s" % e)

            f.close()
            raise e




    def validateColumnLength(self,NumberofColumns):
        """
                          Method Name: validateColumnLength
                          Description: This function validates the number of columns in the csv files.
                                       It is should be same as given in the schema file.
                                       If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                       If the column number matches, file is kept in Good Raw Data for processing.
                                      The csv file is missing the first column name, this function changes the missing name to "incomeData".
                          Output: None
                          On Failure: Exception
                      """
        try:
            f = open(r"B:\project\pythonmyproject\Training_Logs\columnValidationLog.txt", 'a+')
            ob.log(f,"Column Length Validation Started!!")

            for file in listdir(r'B:/project/pythonmyproject/Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv(r'B:/project/pythonmyproject/Training_Raw_files_validated/Good_Raw/' + file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move(r'B:/project/pythonmyproject/Training_Raw_files_validated/Good_Raw/' + file, r'B:/project/pythonmyproject/Training_Raw_files_validated/Bad_Raw')
                    ob.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)

            ob.log(f, "Column Length Validation Completed!!")

        except OSError:
            f = open(r"B:\project\pythonmyproject\Training_Logs\columnValidationLog.txt", 'a+')
            ob.log(f, "Error Occured while moving the file :: %s" % OSError)

            f.close()
            raise OSError
        except Exception as e:
            f = open(r"B:\project\pythonmyproject\Training_Logs\columnValidationLog.txt", 'a+')
            ob.log(f, "Error Occured:: %s" % e)

            f.close()
            raise e
        f.close()

    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception
                              """
        try:
            f = open(r"B:\project\pythonmyproject\Training_Logs\missingValuesInColumn.txt", 'a+')
            ob.log(f,"Missing Values Validation Started!!")


            for file in listdir(r'B:/project/pythonmyproject/Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv(r'B:/project/pythonmyproject/Training_Raw_files_validated/Good_Raw/' + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move(r'B:/project/pythonmyproject/Training_Raw_files_validated/Good_Raw/' + file,
                                    r'B:/project/pythonmyproject/Training_Raw_files_validated/Bad_Raw/')
                        ob.log(f,"Invalid Column for the file!! File moved to Bad Raw Folder :: %s" % file)

                        break
                if count==0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv(r'B:/project/pythonmyproject/Training_Raw_files_validated/Good_Raw/' + file, index=None, header=True)
        except OSError:
            f = open(r"B:\project\pythonmyproject\Training_Logs\missingValuesInColumn.txt", 'a+')
            ob.log(f, "Error Occured while moving the file :: %s" % OSError)

            f.close()
            raise OSError
        except Exception as e:
            f = open(r"B:\project\pythonmyproject\Training_Logs\missingValuesInColumn.txt", 'a+')
            ob.log(f, "Error Occured:: %s" % e)

            f.close()
            raise e
        f.close()












