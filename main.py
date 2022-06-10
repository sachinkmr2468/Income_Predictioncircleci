from wsgiref import simple_server
from flask import Flask, request, render_template
from flask import Response
import os
from flask_cors import CORS, cross_origin
from prediction_validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
import flask_monitoringdashboard as dashboard
from predictFromModel import prediction

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app= Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')
@app.route("/predict",methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
       if request.json is not None:
#           path = request.json['filepath']
           pred_val = pred_validation()
           pred_val.prediction_validation()
           pred = prediction()  # object initialization

           # predicting for dataset present in database
           path = pred.predictionFromModel()
           return Response("Prediction File created at %s!!!" % path)
       elif request.form is not None:
#           path = request.form['filepath']
           pred_val = pred_validation()
           pred_val.prediction_validation()
           pred = prediction()
           path = pred.predictionFromModel()
           return Response('Prediction file created at %s!!!'%path)
    except ValueError:
        return Response('Error occured %s'%ValueError)
    except KeyError:
        return Response("Error occured! %s"%KeyError )
    except Exception as e:
        return Response("Error occured! %s"%e )
@app.route("/train", methods=['POST','GET'])
@cross_origin()
def trainRouteClient():
    try:
#        if request.json['folderPath'] is not None:
         if request.method=='GET':
            print('start')
 #           path = request.json['folderPath']
            train_valobj = train_validation()
            train_valobj.train_validation()
            trainModelobj = trainModel()
            trainModelobj.trainingModel()
            print('end')
    except ValueError:
        return Response('Error occured %s' % ValueError)
    except KeyError:
        return Response("Error occured! %s" % KeyError)
    except Exception as e:
        return Response("Error occured! %s" % e)
    return Response('Training completed successfully!!')

port = int(os.getenv("PORT",5000))
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
#    host = '0.0.0.0'
    #port = 5000
#    httpd = simple_server.make_server(host, port, app)
    # print("Serving on %s %d" % (host, port))
#    httpd.serve_forever()


