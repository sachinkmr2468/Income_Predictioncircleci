import pandas as pd
import numpy as np
from sklearn_pandas import CategoricalImputer
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import RandomOverSampler
from application_logging.logger import App_Logger
ob = App_Logger()

class Preprocessor:
    """
        This class helps to clean and transform the data before training.
        """

    def __init__(self):
        self.file_object = open(r'B:\project\pythonmyproject\Training_Logs\preprocessorlog.txt', 'a+')

    def remove_unwanted_spaces(self,data): #data is passed from diff location when this method is called
        """
                        Method Name: remove_unwanted_spaces
                        Description: This method removes the unwanted spaces from a pandas dataframe.
                        Output: A pandas DataFrame after removing the spaces.
                        On Failure: Raise Exception
                """
        #writting log to a file
        ob.log(self.file_object, 'Entered the remove_unwanted_spaces method of the Preprocessor class')
        self.data = data
        try:

            self.df_without_spaces=self.data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)  # remove space at the begging/ending of column names
            #writting log to a file

            ob.log(self.file_object,
                   'Unwanted spaces removal Successful.Exited the remove_unwanted_spaces method of the Preprocessor class')

            return self.df_without_spaces
        except Exception as e:
            #writting log

            ob.log(self.file_object,'Exception occured in remove_unwanted_spaces' +str(e))

            #writting log

            ob.log(self.file_object,'unwanted space removal Unsuccessful')


            raise Exception()


    def remove_columns(self,data,columns):
        """
                Method Name: remove_columns
                Description: This method removes the given columns from a pandas dataframe.
                Output: A pandas DataFrame after removing the specified columns.
                On Failure: Raise Exception
        """
        #writting log to a file
        ob.log(self.file_object,'Entered the remove_columns method of the Preprocessor class')
        self.data= data # this will be my df
        self.columns= columns
        try:
            self.useful_data = self.data.drop(labels=self.columns, axis=1) # col names and data are passed when this method is called and this will remove the col which are needed

            ob.log(self.file_object,'Column removed Successfully')

            return self.useful_data
        except Exception as e:
            ob.log(self.file_object,'Exception occured in remove_columns' +str(e))
            ob.log(self.file_object,'Column removal Unsuccessful')

            raise Exception()

    def separate_label_feature(self, data, label_column_name): #label_column_name - is passed when this method is called
        """
                        Method Name: separate_label_feature
                        Description: This method separates the features and a Label Coulmns.
                        Output: Returns two separate Dataframes, one containing features and the other containing Labels .
                        On Failure: Raise Exception
                """

        ob.log(self.file_object,'Entered the separate_label_feature method of the Preprocessor class')

        #wrote log to a file
        try:
            self.X=data.drop(labels=label_column_name,axis=1) # drop the columns specified and separate the feature columns
            self.Y=data[label_column_name] # Filter the Label columns
            ob.log(self.file_object,'Label Separation Successful')

            return self.X,self.Y # return the features and label col
        except Exception as e:
            ob.log(self.file_object,'Exception occured in separate_label_feature method' +str(e))
#            file = ('preprocessing.txt','a+')
            ob.log(self.file_object,'Exited the separate_label_feature method of the Preprocessor class')

            raise Exception()

    def is_null_present(self,data):
        """
                                Method Name: is_null_present
                                Description: This method checks whether there are null values present in the pandas Dataframe or not.
                                Output: Returns True if null values are present in the DataFrame, False if they are not present and
                                        returns the list of columns for which null values are present.
                                On Failure: Raise Exception
                        """

        ob.log(self.file_object,'Entered the is_null_present method of the Preprocessor class')

        self.null_present = False
        self.cols_with_missing_values=[] # empty list created
        self.cols = data.columns # will have all the cols of the dataframe stored here
        try:
            self.null_counts=data.isna().sum() # check for the count of null values per column
            for i in range(len(self.null_counts)): #iterrate with the len of null counts
                if self.null_counts[i]>0:
                    self.null_present=True
                    self.cols_with_missing_values.append(self.cols[i]) ##adding the missing col to listed created in self.cols_with_missing
            if(self.null_present): # write the logs to see which columns have null values
                self.dataframe_with_null = pd.DataFrame() #creating empty dataframe
                self.dataframe_with_null['columns'] = data.columns # #addign a col name col
                self.dataframe_with_null['missing values count'] = np.asarray(data.isna().sum()) #adding another col missing values count
                self.dataframe_with_null.to_csv(r'B:\project\pythonmyproject\preprocessing_data\null_values.csv') # storing the null column information to file

                ob.log(self.file_object,'Finding missing values is a success')

            return self.null_present, self.cols_with_missing_values
        except Exception as e:
            ob.log(self.file_object,'Exception occured in is_null_present method'+str(e))
            ob.log(self.file_object,'Finding missing values failed')

            raise Exception()

    def impute_missing_values(self, data, cols_with_missing_values): #cols_with_missing_values - will be passed when this method will be called
        """
                                        Method Name: impute_missing_values
                                        Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
                                        Output: A Dataframe which has all the missing values imputed.
                                        On Failure: Raise Exception
                     """

        ob.log(self.file_object,'Entered the impute_missing_values method of the Preprocessor class')

        self.data= data
        self.cols_with_missing_values=cols_with_missing_values
        try:
            self.imputer = CategoricalImputer() #created object of categoricalimputer(),helps to replace all the missing values with most frequent value
            for col in self.cols_with_missing_values: #iterrating over missing values col
                self.data[col] = self.imputer.fit_transform(self.data[col])

            ob.log(self.file_object,'Imputing missing values Successful')

            return self.data
        except Exception as e:
            ob.log(self.file_object,'Exception occured in impute_missing_values method' +str(e))
            ob.log(self.file_object,'Imputing missing values failed')

            raise Exception()
    def scale_numerical_columns(self,data): #Data will be passed when this method will be called
        """
                                                        Method Name: scale_numerical_columns
                                                        Description: This method scales the numerical values using the Standard scaler.
                                                        Output: A dataframe with scaled
                                                        On Failure: Raise Exception
                                     """

        ob.log(self.file_object,'Entered the scale_numerical_columns method of the Preprocessor class')


        self.data=data

        try:
            self.num_df = self.data.select_dtypes(include=['int64']).copy() #stores all the col with num values
            self.scaler = StandardScaler()
            self.scaled_data = self.scaler.fit_transform(self.num_df)
            self.scaled_num_df = pd.DataFrame(data=self.scaled_data, columns=self.num_df.columns) #datframe created with all the num values

            ob.log(self.file_object,'scaling for numerical values successful')

            return self.scaled_num_df

        except Exception as e:
            #writting log to class
            ob.log(self.file_object,'Exception occured in scale_numerical_columns method'+str(e))
            ob.log(self.file_object,'scaling for numerical columns Failed')

            raise Exception()
    def encode_categorical_columns(self,data): # data will be passed when this method will be called
        """
                                                Method Name: encode_categorical_columns
                                                Description: This method encodes the categorical values to numeric values.
                                                Output: only the columns with categorical values converted to numerical values
                                                On Failure: Raise Exception
                             """
        ob.log(self.file_object,'Entered the encode_categorical_columns method of the Preprocessor class')
        try:

            self.cat_df = data.select_dtypes(include=['object']).copy() #saving all cat col
            # Using the dummy encoding to encode the categorical columns to numericsl ones
            for col in self.cat_df.columns:
                self.cat_df = pd.get_dummies(self.cat_df, columns=[col], prefix=[col], drop_first=True)
            ob.log(self.file_object,'Exited the encode_categorical_columns method of the Preprocessor class')

            return self.cat_df

        except Exception as e:
            ob.log(self.file_object,'Exception occured in encode_categorical_columns method' +str(e))
            ob.log(self.file_object,'encoding for categorical columns Failed')

            raise Exception()

    def handle_imbalanced_dataset(self,x,y): # x, y will be passed when this method will be called
        """
        Method Name: handle_imbalanced_dataset
        Description: This method handles the imbalanced dataset to make it a balanced one.
        Output: new balanced feature and target columns
        On Failure: Raise Exception
                                     """

        ob.log(self.file_object,'Entered the handle_imbalanced_dataset method of the Preprocessor class')
        try:
            self.rdsmple = RandomOverSampler()
            self.x_sampled, self.y_sampled  = self.rdsmple.fit_resample(x,y)
            ob.log(self.file_object,'dataset balancing successful')

            return self.x_sampled,self.y_sampled

        except Exception as e:
            ob.log(self.file_object,'Exception occured in handle_imbalanced_dataset method of the Preprocessor class'+ str(e))
            ob.log(self.file_object,'dataset balancing Failed.')

            raise Exception()
