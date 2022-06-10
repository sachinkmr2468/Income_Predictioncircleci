from sklearn.naive_bayes import GaussianNB
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score,accuracy_score
from application_logging.logger import App_Logger
ob = App_Logger()


class Model_Finder:
    """
                This class shall  be used to find the model with best accuracy and AUC score.
                Written By: iNeuron Intelligence
                Version: 1.0
                Revisions: None

                """

    def __init__(self):
        self.file_object = open(r'B:\project\pythonmyproject\Training_Logs\Model_Finderlog.txt', 'a+')
        self.gnb = GaussianNB()
        # need to practice XGBoost algo
        self.xgb = XGBClassifier(objective='binary:logistic',n_jobs=-1)

    def get_best_params_for_naive_bayes(self,train_x,train_y):
        """
        Method Name: get_best_params_for_naive_bayes
        Description: get the parameters for the Naive Bayes's Algorithm which give the best accuracy.
                     Use Hyper Parameter Tuning.
        Output: The model with the best parameters
        On Failure: Raise Exception


                        """
        #writting log to a file
        ob.log(self.file_object, 'Entered the get_best_params_for_naive_bayes method of the Model_Finder class')
        try:
            # initializing with different combination of parameters- i knw this
            self.param_grid = {"var_smoothing": [1e-9,0.1, 0.001, 0.5]}

            #Creating an object of the Grid Search class - need to go through algo
            self.grid = GridSearchCV(estimator=self.gnb, param_grid=self.param_grid, cv=5,  verbose=3)
            #finding the best parameters
            self.grid.fit(train_x, train_y)

            #extracting the best parameters and creating a col named var smoothing
            self.var_smoothing = self.grid.best_params_['var_smoothing']


            #creating a new model with the best parameters
            self.gnb = GaussianNB(var_smoothing=self.var_smoothing)
            # training the mew model
            self.gnb.fit(train_x, train_y)
            #writting log to a file
            ob.log(self.file_object,
                                   'Naive Bayes best params: '+str(self.grid.best_params_)+'. Exited the get_best_params_for_naive_bayes method of the Model_Finder class')

            return self.gnb
        except Exception as e:
            #writting log to file
            ob.log(self.file_object,
                                   'Exception occured in get_best_params_for_naive_bayes method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            ob.log(self.file_object,
                                   'Random Forest Parameter tuning  failed. Exited the get_best_params_for_naive_bayes method of the Model_Finder class')
            raise Exception()
# need to practice XGboost
    def get_best_params_for_xgboost(self,train_x,train_y):

        """
                                        Method Name: get_best_params_for_xgboost
                                        Description: get the parameters for XGBoost Algorithm which give the best accuracy.
                                                     Use Hyper Parameter Tuning.
                                        Output: The model with the best parameters
                                        On Failure: Raise Exception


                                """
        #writing log to file
        ob.log(self.file_object,
                               'Entered the get_best_params_for_xgboost method of the Model_Finder class')
        try:
            # initializing with different combination of parameters
            self.param_grid_xgboost = {

                "n_estimators": [10, 20], "criterion": ['gini', 'entropy'],
                               "max_depth": range(8, 10, 1)

            }
            # Creating an object of the Grid Search class
            self.grid= GridSearchCV(XGBClassifier(objective='binary:logistic'),self.param_grid_xgboost, verbose=3,cv=5)
            # finding the best parameters
            self.grid.fit(train_x, train_y)

            # extracting the best parameters
            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']
            self.n_estimators = self.grid.best_params_['n_estimators']

            # creating a new model with the best parameters
            self.xgb = XGBClassifier(criterion=self.criterion, max_depth=self.max_depth,n_estimators= self.n_estimators, n_jobs=-1 )
            # training the mew model
            self.xgb.fit(train_x, train_y)
            ob.log(self.file_object,
                                   'XGBoost best params: ' + str(
                                       self.grid.best_params_) + '. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            return self.xgb
        except Exception as e:
            #writting log to file
            ob.log(self.file_object,
                                   'Exception occured in get_best_params_for_xgboost method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            #writting log to file
            ob.log(self.file_object,
                                   'XGBoost Parameter tuning  failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise Exception()


    def get_best_model(self,train_x,train_y,test_x,test_y):
        """
                                                Method Name: get_best_model
                                                Description: Find out the Model which has the best AUC score.
                                                Output: The best model name and the model object
                                                On Failure: Raise Exception



                                        """
        #writing log to file
        ob.log(self.file_object,
                               'Entered the get_best_model method of the Model_Finder class')
        # create best model for XGBoost
        try:
            self.xgboost= self.get_best_params_for_xgboost(train_x,train_y)
            self.prediction_xgboost = self.xgboost.predict(test_x) # Predictions using the XGBoost Model

            if len(test_y.unique()) == 1: #if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.xgboost_score = accuracy_score(test_y, self.prediction_xgboost)
                ob.log(self.file_object, 'Accuracy for XGBoost:' + str(self.xgboost_score))  # Log AUC
            else:
                self.xgboost_score = roc_auc_score(test_y, self.prediction_xgboost) # AUC for XGBoost
                ob.log(self.file_object, 'AUC for XGBoost:' + str(self.xgboost_score)) # Log AUC

            # create best model for Random Forest(naive bayes)
            self.naive_bayes=self.get_best_params_for_naive_bayes(train_x,train_y)

            self.prediction_naive_bayes=self.naive_bayes.predict(test_x) # prediction using the Random Forest Algorithm

            if len(test_y.unique()) == 1:#if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.naive_bayes_score = accuracy_score(test_y,self.prediction_naive_bayes)
                # writing log to file
                ob.log(self.file_object, 'Accuracy for NB:' + str(self.naive_bayes_score))
            else:
                self.naive_bayes_score = roc_auc_score(test_y, self.prediction_naive_bayes) # AUC for Random Forest
                ob.log(self.file_object, 'AUC for RF:' + str(self.naive_bayes_score))

            #comparing the two models
            if(self.naive_bayes_score <  self.xgboost_score):
                return 'XGBoost',self.xgboost
            else:
                return 'NaiveBayes',self.naive_bayes

        except Exception as e:
            #writing log to file
            ob.log(self.file_object,
                                   'Exception occured in get_best_model method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            #writing log to file
            ob.log(self.file_object,
                                   'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise Exception()
