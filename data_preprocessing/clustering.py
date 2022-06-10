import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations import file_methods
from application_logging.logger import App_Logger
ob = App_Logger()

class KMeansClustering:
    """
            This class helps to divide the data into clusters before training.
            """

    def __init__(self):
        self.file_object = open(r'B:\project\pythonmyproject\Training_Logs\KMeansClusteringlog.txt','a+')

    def elbow_plot(self,data): #data is passed when this method is called
        """
                        Method Name: elbow_plot
                        Description: This method saves the plot to decide the optimum number of clusters to the file.
                        Output: A picture saved to the directory
                        On Failure: Raise Exception
                """

        ob.log(self.file_object,'Entered the elbow_plot method of the KMeansClustering class')

        wcss=[] # initializing an empty list which will hold all the wcss values
        try:
            for i in range (1,11):#we are testing it with 11 clusters and lets see how many clusters its gonna form
                kmeans=KMeans(n_clusters=i,init='k-means++',random_state=42) # initializing the KMeans object
                kmeans.fit(data) # fitting the data to the KMeans Algorithm
                wcss.append(kmeans.inertia_)
            plt.plot(range(1,11),wcss) # creating the graph between WCSS and the number of clusters to decide how many clusters can be created
            plt.title('The Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            #plt.show()
            plt.savefig(r'B:\project\pythonmyproject\Training_Logs\K-Means_Elbow.PNG') # saving the elbow plot locally
            # finding the value of the optimum cluster via programe
            self.kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            ob.log(self.file_object,'The optimum number of clusters is: '+str(self.kn.knee)+' . Exited the elbow_plot method of the KMeansClustering class')

            return self.kn.knee #gives the point of max curvation

        except Exception as e:
            ob.log(self.file_object,'Exception occured in elbow_plot method. Exception message:  ' + str(e))

            raise Exception()

    def create_clusters(self,data,number_of_clusters):#data,number_of_clusters is passed when this method is called
        """
                                Method Name: create_clusters
                                Description: Creating a dataframe with clusters values.
                                Output: A dataframe with cluster column
                                On Failure: Raise Exception
                        """
        ob.log(self.file_object,'Entered the create_clusters method of the KMeansClustering class')
        self.data=data

        try:#number_of_clusters - this we have identified by method elbow_plot, going to create a DF with the cluster
            self.kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', random_state=42)

            self.y_kmeans=self.kmeans.fit_predict(data) #  divide data into clusters
            self.file_op = file_methods.File_Operation() # post doing filemethods class will come back here
            self.save_model = self.file_op.save_model(self.kmeans, 'KMeans') # saving the KMeans model to directory
                                                                                    # passing 'Model' as the functions need three parameters

            self.data['Cluster']=self.y_kmeans  # create a new column in dataset for storing the cluster information
            ob.log(self.file_object,'succesfully created '+str(self.kn.knee))

            return self.data
        except Exception as e:
            ob.log(self.file_object,'Exception occured in create_clusters method. Exception message:  ' + str(e))

            raise Exception()