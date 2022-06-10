"""
This is the Entry point for Training the Machine Learning Model.
"""


# Doing the necessary imports
from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from data_preprocessing import preprocessing
from data_preprocessing import clustering
from best_model_finder import tunner
from file_operations import file_methods
from application_logging.logger import App_Logger
import numpy as np
import pandas as pd

ob = App_Logger()
#Creating the common Logging object


class trainModel:

    def __init__(self):
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')
    def trainingModel(self):
        # Logging the start of Training
        ob.log(self.file_object, 'Start of Training')

        try:
            # Getting the data from the source
            #calling class Data_getter from module data-loader
            data_getter=data_loader.Data_Getter()
            data=data_getter.get_data() # callng the method get_data from class Data_getter


            """doing the data preprocessing"""
#calling class preprocessor from module preprocessing and saving it to variable
            preprocessor=preprocessing.Preprocessor()
            #calling method remove_columns from class preprocessor
            data=preprocessor.remove_columns(data,['education']) # remove the column as it doesn't contribute to prediction.
            #calling method remove_unwanted_spaces from class preprocessor
            data=preprocessor.remove_unwanted_spaces(data) # remove unwanted spaces from the dataframe
            data.replace('?',np.NaN,inplace=True) # replacing '?' with NaN values for imputation


            # create separate features and labels
            #calling separate_label_feature from class preprocessor
            X,Y=preprocessor.separate_label_feature(data,label_column_name='income')
            # encoding the label column
            # need to knw about map function
            Y = Y.map({'<=50K': 0, '>50K': 1})

            # check if missing values are present in the dataset
            # calling method is_null_present from class Preprocessor
            is_null_present,cols_with_missing_values=preprocessor.is_null_present(X)

            # if missing values are there, replace them appropriately.
            if(is_null_present):
 #                calling impute_missing_values method from class preprocessor
                X=preprocessor.impute_missing_values(X,cols_with_missing_values) # missing value imputation

            # Proceeding with more data pre-processing steps
            #applying standard scalar, scale_numerical_columns method from class preprocessor
            scaled_num_df=preprocessor.scale_numerical_columns(X)
            #chnaging cat values to numerical values, encode_categorical_columns method from class preprocessor
            cat_df=preprocessor.encode_categorical_columns(X)
            X=pd.concat([scaled_num_df,cat_df], axis=1)

            """Applying the oversampling approach to handle imbalanced dataset"""
            #calling method handle_imbalanced_dataset from class preprocessor
            X,Y=preprocessor.handle_imbalanced_dataset(X,Y)

            """ Applying the clustering approach"""
            #need to do coding on clustering
#KMeansClustering class called from module clustering
            kmeans=clustering.KMeansClustering() # object initialization.
            #calling elbow_plot from  KMeansClustering class
            number_of_clusters=kmeans.elbow_plot(X)  #  using the elbow plot to find the number of optimum clusters

            # Divide the data into clusters
            #calling create_clusters method from KMeansClustering class
            X=kmeans.create_clusters(X,number_of_clusters)

            #create a new column in the dataset consisting of the corresponding cluster assignments.
            X['Labels']=Y

            # getting the unique clusters from our dataset
            list_of_clusters=X['Cluster'].unique()

            """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

            for i in list_of_clusters:
                cluster_data=X[X['Cluster']==i] # filter the data for one cluster

                # Prepare the feature and Label columns
                cluster_features=cluster_data.drop(['Labels','Cluster'],axis=1)
                cluster_label= cluster_data['Labels']

                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3, random_state=355)

                model_finder=tunner.Model_Finder() # object initialization

                #getting the best model for each of the clusters
                #calling class model_finder from tunner module
                best_model_name,best_model=model_finder.get_best_model(x_train,y_train,x_test,y_test)

                #saving the best model to the directory.
                # calling File_Operation class from file_methods module
                file_op = file_methods.File_Operation()
                #calling save_model method from file_oeration
                save_model=file_op.save_model(best_model,best_model_name+str(i))

            # logging the successful Training
            ob.log(self.file_object, 'Successful End of Training')
            #closing the file
            self.file_object.close()

        except Exception as e:
            # logging the unsuccessful Training
            ob.log(self.file_object, 'Unsuccessful End of Training'+ str(e))

            self.file_object.close()
            raise Exception