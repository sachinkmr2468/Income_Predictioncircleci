#this code is just a try version
from wsgiref import simple_server
from flask import Flask, request
from flask import Response
from flask_cors import CORS, cross_origin
from prediction_validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
from flask_monitoringdashboard import dashboard
from predictFromModel import prediction
import os
import pandas as pd

os.putenv('Land','en_US.UTF-8')
os.putenv('LC_ALL','en_US.UTF-8')

try:
  path = 'Prediction_Batch_Files'

     #pred_val = pred_validation(path) #object initialization
     #
     #pred_val.prediction_validation() #calling the prediction_validation function
  #creating object of prediction class from predictFromModel module
  pred = prediction(path) #object initialization

    # predicting for dataset present in database
  #calling predictionFromModel method from prediction class
  path = pred.predictionFromModel()
  print("Prediction File created at %s!!!" % path)
#
except ValueError:
    print("Error Occurred! %s" %ValueError)
except KeyError:
    print("Error Occurred! %s" %KeyError)
except Exception as e:
    print("Error Occurred! %s" %e)
