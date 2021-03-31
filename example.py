from olMEGA_DataService_Client import olMEGA_DataService_Client
from datetime import datetime

user = "Mustermann"
password = "12345"
host = "localhost"
#host = "10.42.0.202"
conditions = False

client = olMEGA_DataService_Client.client(user, password, host, debug = True)
#conditions =  {"datachunk" : {"subject": "NN07IS04"}};
#conditions =  {"questionnaire": {'surveyfile' : 'questionnaire20180425frei - simple.xml'}};
conditions =  {"datachunk" : {"subject": "NN07IS04"}, "questionnaire": {'surveyfile' : 'questionnaire20180425frei - simple.xml'}};
print("date and time =", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
myDataSet = client.getDataSet("datachunk", conditions)
print("date and time =", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
#client.downloadFiles("tmp", myDataSet)
#print("date and time =", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))





"""
count = 0
value = client.createNewFeatureValue("PSD")

value["subject"] = "AN05CH11"
value["value"] = random()
value["side"] = "r"
value["isvalid"] = True
value["start"] = datetime(2020, 1, 1, 0, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S.%f")
value["end"] = datetime(2020, 1, 1, 0, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S.%f")

myDataSet = client.saveFeatureValue(value)
"""

#myDataSet[0]["date"] = datetime.datetime(2020, 1, 1, 0, 0, 0, count).strftime("%Y-%m-%d %H:%M:%S.%f")

#for x in range(mFeatureData.shape[0]):
#    for y in range(1): #range(mFeatureData.shape[1]):
#        newValue = Value.copy()
#        newValue["start"] = datetime.datetime(2020, 1, 1, 0, 0, 0, count).strftime("%Y-%m-%d %H:%M:%S.%f")
#        newValue["end"] = datetime.datetime(2020, 1, 1, 0, 0, 0, count).strftime("%Y-%m-%d %H:%M:%S.%f")
#        newValue["validvalue"] = 1
#        newValue["valueleft"] = mFeatureData[0][0]
#        myDataSet[0]["featurevalue"].append(newValue)
#        count += 1

"""
client = EMAClient.client(user, password, host)
conditions = False
conditions =  {"subject": "ER05RT20"};

#conditions = {"subject": "0", "featurevalue" : {"name" : "rms"}}
#conditions =  {"subject": "ER05RT20"};
#conditions =  {"subject": "P24"};
#conditions =  {"subject": "P1", "date": "2018-08-16 10:28:06.285055"};

#myDataSet = client.uploadQuestinares("./Questinares/P1/")

myDataSet = client.uploadFiles("./FeatureFiles")

#myDataSet = client.getDataSet("FeatureSet", conditions)
#myDataSet = client.getDataSet("FeatureSet", conditions)
#myDataSet = client.exists("FeatureSet", conditions)
#myDataSet = client.getDataSet("FeatureSet", myDataSet[0])

#myDataSet = client.getDataSet("FeatureSet", conditions)
#newValue = client.createNewFeatureValue("PSD")
#myDataSet[0]["featurevalue"].append(newValue)
#myDataSet2 = client.updateDataSet#(myDataSet)

#myDataSet = client.getDataSet("FeatureSet", conditions)
#client.deleteFeatureValues(myDataSet)

#myFiles = client.downloadFiles("out", myDataSet)

#myDataSet = client.getDataSet("FeatureSet", conditions)

#myData = client.updateDataSet(myDataSet)

#myDataSet = client.getDataSQL("FeatureSet", "featureset_subject = 1")
"""

client.close()

print ("Done...")
pass
