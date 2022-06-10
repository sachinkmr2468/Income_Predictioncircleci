import pandas as pd
from application_logging.logger import App_Logger
ob = App_Logger()

class Data_Getter_Pred:
    """
    This class is used for obtaining the data from the source for prediction.
    """
    def __init__(self):
        self.prediction_file=r'C:\Users\nihca\Documents\project\Training_FileFromDB\inputFilee.csv' # gives the path from where the data will be loaded
        self.file_object = open(r'B:\project\pythonmyproject\Prediction_Logs\data_getter.txt', 'a+')


    def get_data(self):
        """
        Method Name: get_data
        Description: This method reads the data from source.
        Output: A pandas DataFrame.
        On Failure: Raise Exception
        """
        ob.log(self.file_object,'Entered the get_data method of the Data_Getter class')

        try:
            self.data= pd.read_csv(self.prediction_file) # reading the data file
            ob.log(self.file_object,'Data Load Successful')

            return self.data
        except Exception as e:
            ob.log(self.file_object,
                   'Exception occured in get_data method of the Data_Getter class. Exception message: ' + str(e))
            ob.log(self.file_object,
                   'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise Exception()


