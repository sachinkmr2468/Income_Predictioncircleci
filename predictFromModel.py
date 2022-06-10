import pandas as pd
import numpy as np
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction

from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from application_logging.logger import App_Logger
ob = App_Logger()


class prediction:

    def __init__(self):
        self.file_object = open(r'B:\project\pythonmyproject\Prediction_Logs\Prediction_Logs.txt', 'a+')

        self.pred_data_val = Prediction_Data_validation()

    def predictionFromModel(self):

        try:
            #calling the mthod deletepredictionfile from class Prediction_Data_validation
            self.pred_data_val.deletePredictionFile() #deletes the existing prediction file from last run!
            ob.log(self.file_object,'Start of Prediction')
            #ceating the class Data_Getter_Pred from module data_loader_prediction and making an object
            data_getter=data_loader_prediction.Data_Getter_Pred()
            data=data_getter.get_data() # calling the method from data_getter class

            #code change
            # wafer_names=data['Wafer']
            # data=data.drop(labels=['Wafer'],axis=1)
            #calling the class preprocessor
            #preprocessing.Preprocessor this is another of way of calling the class from module
            preprocessor = preprocessing.Preprocessor()
            #calling method remove_columns from preprocessor class
            data = preprocessor.remove_columns(data, [
                'education'])  # remove the column as it doesn't contribute to prediction.
            #calling remove_unwanted_spaces method from preprocessor class
            data = preprocessor.remove_unwanted_spaces(data)  # remove unwanted spaces from the dataframe
            data.replace('?', np.NaN, inplace=True)  # replacing '?' with NaN values for imputation
#            X, Y = preprocessor.separate_label_feature(data, label_column_name='income')
 #           Y = Y.map({'<=50K': 0, '>50K': 1})
            # check if missing values are present in the dataset
            #callig is_null_present method from class preprocessor
            is_null_present, cols_with_missing_values = preprocessor.is_null_present(data)

            # if missing values are there, replace them appropriately.
            if (is_null_present): #calling methd impute_missing_values from class preprocessor
                data = preprocessor.impute_missing_values(data, cols_with_missing_values)  # missing value imputation

            # Proceeding with more data pre-processing steps
            # calling scale_numerical_columns from preprocessor and storing the value to scaled_num_df
            scaled_num_df = preprocessor.scale_numerical_columns(data)
            # calling encode_categorical_columns from class preprocessor and storing the value to cat_df
            cat_df = preprocessor.encode_categorical_columns(data)
            #creating x by putting the sacaled data and numericaiy converted data
            X = pd.concat([scaled_num_df, cat_df], axis=1)

            #from file methods module file operation is called and create an object of it
            file_loader=file_methods.File_Operation()
            # creating load_model from file_loader and storing the value to kmeans
            kmeans=file_loader.load_model('KMeans')

            ##Code changed
            #pred_data = data.drop(['Wafer'],axis=1)
            #doing prediction using training data x and stroing the value to clusters
            clusters=kmeans.predict(X)#drops the first column for cluster prediction
            X['clusters']=clusters # assing a column
            clusters=X['clusters'].unique() # this need to check later like what it does
            predictions=[] #creating a list of predictions
            for i in clusters:
                cluster_data= X[X['clusters']==i]
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                #calling find_correct_model_file method from file loader
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                result=(model.predict(cluster_data)) #do the coding for cluster algo then come here
                for res in result:
                    if res==0:
                        predictions.append('<=50K')
                    else:
                        predictions.append('>50K')
            #this line nt understood y zip is done
            final= pd.DataFrame(list(zip(predictions)),columns=['Predictions'])
            path="Prediction_Output_File/Predictions.csv"
            final.to_csv("Prediction_Output_File/Predictions.csv",header=True,mode='a+') #appends result to prediction file
            ob.log(self.file_object,'End of Prediction')
        except Exception as ex:
            ob.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return path




