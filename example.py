from time import strptime
from olMEGA_DataService_Client import olMEGA_DataService_Client
from datetime import datetime, timedelta
import random

user = "Mustermann"
password = "12345"
host = "localhost"
#host = "10.42.0.202"
conditions = False

client = olMEGA_DataService_Client.client(user, password, host, debug = True)

def get_all_participants(db):
        return db.executeQuery('SELECT DISTINCT Subject FROM EMA_datachunk')

def get_all_questionaire_filenames_for_one_subject (db, participent_pseudoname):
        sql_query_string = f'SELECT EMA_questionnaire.id, EMA_questionnaire.SurveyFile FROM EMA_questionnaire INNER JOIN EMA_datachunk ON EMA_questionnaire.DataChunk_id = EMA_datachunk.ID WHERE EMA_datachunk.Subject == "{participent_pseudoname}" '
        print (sql_query_string)
        return db.executeQuery(sql_query_string)

def get_questionaire_fillout_time(db, questionaire_id):
        sql_query_string = f'''SELECT EMA_answer.ID, EMA_answer.Text, EMA_questionnaire.Start, EMA_answer.Question_id,  EMA_question.QuestionKey 
                                FROM EMA_answer INNER JOIN EMA_questionnaire on EMA_answer.Questionnaire_id = EMA_questionnaire.ID INNER JOIN EMA_question on EMA_answer.Question_id = EMA_question.ID WHERE EMA_answer.Questionnaire_id = "{questionaire_id}" AND EMA_question.QuestionKey = "527d415b-35f7-4c68-a09d-f8e18e192d2d"'''
                                
        query_answer = db.executeQuery(sql_query_string)
        questionaire_start_time = datetime.strptime(query_answer[0]['start'],'%Y-%m-%d %H:%M:%S')
        # ToDo: JB correction for other answers than "now" is missing 
        return questionaire_start_time

def get_chunk_at_time(db, participant, desired_time):
        time_string = datetime.strftime(desired_time - timedelta(seconds = 59, milliseconds=999),'%Y-%m-%d %H:%M:%S')
        time_string_delta = datetime.strftime(desired_time ,'%Y-%m-%d %H:%M:%S')
        sql_query_string = f'''SELECT * FROM EMA_datachunk WHERE EMA_datachunk.Subject  = "{participant}" AND 
                                EMA_datachunk.Start > "{time_string}" AND 
                                EMA_datachunk.Start < "{time_string_delta}"'''
        return db.executeQuery(sql_query_string)

def get_chunks_for_time_interval(db, participant, start_time, end_time):
        time_string = datetime.strftime(start_time,'%Y-%m-%d %H:%M:%S')
        time_string_delta = datetime.strftime(end_time ,'%Y-%m-%d %H:%M:%S')
        sql_query_string = f'''SELECT * FROM EMA_datachunk WHERE EMA_datachunk.Subject  = "{participant}" AND 
                                EMA_datachunk.Start > "{time_string}" AND 
                                EMA_datachunk.Start <= "{time_string_delta}"'''
        return db.executeQuery(sql_query_string)
        
def get_filedict_for_chunks(db, chunks, file_extension):
        file_dict = []
        for one_chunk in chunks:
                print(one_chunk)
                
                sql_query_string = f'''SELECT Subject, Filename FROM EMA_datachunk 
                                JOIN EMA_file ON EMA_datachunk.ID = EMA_file.DataChunk_id 
                                JOIN EMA_filetype ON EMA_file.FileType_id = EMA_filetype.ID 
                                WHERE Subject = "{one_chunk['subject']}"
                                AND EMA_filetype.FileExtension = "{file_extension}" 
                                AND EMA_datachunk.ID = "{one_chunk['id']}" '''
                file_dict.append(db.executeQuery(sql_query_string))

        return file_dict
        
all_participants = get_all_participants(client)
one_participant = all_participants[1]['subject']
filenames = get_all_questionaire_filenames_for_one_subject(client,one_participant)
questionaire_id = filenames[2]['id']
#print(questionaire_id)
time_info = get_questionaire_fillout_time(client,questionaire_id)
print (time_info)

chunk = get_chunk_at_time(client, one_participant, time_info)
print(chunk)
chunk_start_time = datetime.strptime(chunk[0]['start'],'%Y-%m-%d %H:%M:%S')
print(chunk_start_time)
chunks = get_chunks_for_time_interval(client, one_participant,chunk_start_time-timedelta(minutes=5), chunk_start_time)
file_dict = get_filedict_for_chunks(client,chunks, 'psd')
print(file_dict)
client.downloadFiles("./tmp", file_dict, True)
file_dict = get_filedict_for_chunks(client,chunks, 'ovd')
print(file_dict)
client.downloadFiles("./tmp", file_dict, True)
client.

#print(chunks)

#print(allParticipants[0]['subject'])

#client
#EMA_questionnaire.SurveyFile
#SELECT  EMA_answer.ID, EMA_answer.Text, EMA_questionnaire.Start FROM EMA_answer INNER JOIN EMA_questionnaire on EMA_answer.Questionnaire_id = EMA_questionnaire.ID  WHERE EMA_answer.Questionnaire_id == "1bd1b05e-150b-4c36-b948-4eb3172d1270" AND Question_id == "8d4684cc-530b-4af5-8fae-8f6e671284df" 

#file_dict = client.executeQuery('SELECT Subject, Filename FROM EMA_datachunk \
#        JOIN EMA_file ON EMA_datachunk.ID = EMA_file.DataChunk_id \
#        JOIN EMA_filetype ON EMA_file.FileType_id = EMA_filetype.ID \
#        WHERE Subject = "%s" \
#        AND EMA_datachunk.Start >= "%s" \
#        AND EMA_datachunk.Start <= "%s" \
#        AND EMA_filetype.FileExtension = "%s" ' % ('NN07IS04', '2021-01-01 00:00:00', '2022-01-01 00:00:00', 'psd')) 

#client.downloadFiles("./tmp", file_dict, True)

#conditions =  {"subject": "NN07IS04"}
#conditions =  {"questionnaire": {'answer' : {'answerkey' : '1010102'}}};
#conditions =  {"datachunk" : {"subject": "NN07IS04"}, "questionnaire": {'surveyfile' : 'questionnaire20180425frei - simple.xml'}};
#conditions =  {'answer' : {'answerkey' : '1010101'}};


#print("date and time =", datetime.now().strftime("%d/%m/%Y %H:%M:%S:%f"))
#myDataSet = client.getDataSet("datachunk", conditions)
#myDataSet = client.executeQuery("delete FROM EMA_datachunk")
#myDataSet = client.getDataSet("answer", conditions)
#print (myDataSet)
#print("date and time =", datetime.now().strftime("%d/%m/%Y %H:%M:%S:%f"))
#client.downloTrueadFiles("tmp", myDataSet)
#print("date and time =", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))





"""
count = 0
value = client.createNewFeatureValue("PSD")

value["subject"] = "AN05CH11"
value["value"] = random.random()
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
