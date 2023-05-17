from olMEGA_DataService_Client import olMEGA_DataService_Client
from datetime import datetime, timedelta
# import random
# from operator import itemgetter
from olMEGA_DataService_Tools import FeatureFile
#from olMEGA_DataService_Tools import acousticweighting as aw
#from olMEGA_DataService_Tools import freq2freqtransforms as freqt
#import mosqito as mq

import numpy as np
#import pandas as pd
import os
#import scipy.signal as sig

#import matplotlib.pyplot as plt




def get_all_participants(db):
    return db.executeQuery('SELECT DISTINCT Subject FROM EMA_datachunk')

def get_all_participants_for_study(db, study):
    sql_query_string = f'SELECT DISTINCT Subject FROM EMA_datachunk INNER JOIN EMA_study ON EMA_datachunk.Study_id = EMA_study.ID WHERE EMA_study.Name = "{study}"'
    return db.executeQuery(sql_query_string)

def get_all_studynames(db):
    return db.executeQuery('SELECT DISTINCT Name FROM EMA_study')
      

def get_all_questionaire_infos (db):
        sql_query_string = f'SELECT EMA_questionnaire.id, EMA_datachunk.Subject, EMA_questionnaire.Filename FROM EMA_questionnaire INNER JOIN EMA_datachunk ON EMA_questionnaire.DataChunk_id = EMA_datachunk.ID '
        #print (sql_query_string)
        return db.executeQuery(sql_query_string)

def get_all_questionaire_infos_for_study (db, study):
        sql_query_string = f'SELECT EMA_questionnaire.id, EMA_datachunk.Subject, EMA_questionnaire.Filename FROM EMA_questionnaire INNER JOIN EMA_datachunk ON EMA_questionnaire.DataChunk_id = EMA_datachunk.ID INNER JOIN EMA_study ON EMA_datachunk.Study_id = EMA_study.ID WHERE EMA_study.Name = "{study}"'
        #print (sql_query_string)
        return db.executeQuery(sql_query_string)


def get_all_questionaire_filenames_for_one_subject (db, participent_pseudoname):
        sql_query_string = f'SELECT EMA_questionnaire.id, EMA_questionnaire.SurveyFile FROM EMA_questionnaire INNER JOIN EMA_datachunk ON EMA_questionnaire.DataChunk_id = EMA_datachunk.ID WHERE EMA_datachunk.Subject == "{participent_pseudoname}" '
        #print (sql_query_string)
        return db.executeQuery(sql_query_string)

def get_questionaire_fillout_time(db, questionaire_id):
        sql_query_string = f'''SELECT EMA_questionnaire.Start
                        FROM EMA_questionnaire WHERE EMA_questionnaire.ID = "{questionaire_id}"'''

#        sql_query_string = f'''SELECT DISTINCT EMA_questionnaire.Start
#                        FROM EMA_answer INNER JOIN EMA_questionnaire on EMA_answer.Questionnaire_id = EMA_questionnaire.ID 
#                        INNER JOIN EMA_question on EMA_answer.Question_id = EMA_question.ID 
#                        WHERE EMA_answer.Questionnaire_id = "{questionaire_id}" AND 
 #                       EMA_question.QuestionKey = "527d415b-35f7-4c68-a09d-f8e18e192d2d"'''
                                
        query_answer = db.executeQuery(sql_query_string)
        if len(query_answer) == 0:
            print("A survey returns no start time: " + sql_query_string)
            return None
        questionaire_start_time = datetime.strptime(query_answer[0]['start'],'%Y-%m-%d %H:%M:%S')
        
        return questionaire_start_time

def get_chunk_at_time(db, participant, desired_time):
        time_string = datetime.strftime(desired_time - timedelta(seconds = 59, milliseconds=999),'%Y-%m-%d %H:%M:%S')
        time_string_delta = datetime.strftime(desired_time ,'%Y-%m-%d %H:%M:%S')
        sql_query_string = f'''SELECT * FROM EMA_datachunk WHERE EMA_datachunk.Subject  = "{participant}" AND 
                                EMA_datachunk.Start > "{time_string}" AND 
                                EMA_datachunk.Start <= "{time_string_delta}"'''
        
        result = db.executeQuery(sql_query_string)
        if not result:
            print (sql_query_string)
        return result

def get_chunks_for_time_interval(db, participant, start_time, end_time):
        time_string = datetime.strftime(start_time,'%Y-%m-%d %H:%M:%S')
        time_string_delta = datetime.strftime(end_time ,'%Y-%m-%d %H:%M:%S')
        sql_query_string = f'''SELECT * FROM EMA_datachunk WHERE EMA_datachunk.Subject  = "{participant}" AND 
                                EMA_datachunk.Start > "{time_string}" AND 
                                EMA_datachunk.Start <= "{time_string_delta}"'''
        return db.executeQuery(sql_query_string)

def get_chunks_and_filenames_for_time_interval(db, participant, start_time, end_time, file_extension):
        time_string = datetime.strftime(start_time,'%Y-%m-%d %H:%M:%S')
        time_string_delta = datetime.strftime(end_time ,'%Y-%m-%d %H:%M:%S')
        sql_query_string = f'''SELECT * FROM EMA_datachunk 
                                JOIN EMA_file ON EMA_datachunk.ID = EMA_file.DataChunk_id 
                                JOIN EMA_filetype ON EMA_file.FileType_id = EMA_filetype.ID 
                                WHERE EMA_datachunk.Subject  = "{participant}" AND 
                                EMA_datachunk.Start > "{time_string}" AND 
                                EMA_datachunk.Start <= "{time_string_delta}"
                                AND EMA_filetype.FileExtension = "{file_extension}" '''
        return db.executeQuery(sql_query_string)



def get_filedict_for_chunks(db, chunks, file_extension):
        file_dict = []
        for one_chunk in chunks:
                #print(one_chunk)
                
                sql_query_string = f'''SELECT Subject, Filename FROM EMA_datachunk 
                                JOIN EMA_file ON EMA_datachunk.ID = EMA_file.DataChunk_id 
                                JOIN EMA_filetype ON EMA_file.FileType_id = EMA_filetype.ID 
                                WHERE Subject = "{one_chunk['subject']}"
                                AND EMA_filetype.FileExtension = "{file_extension}" 
                                AND EMA_datachunk.ID = "{one_chunk['id']}" '''
                one_file_dict = db.executeQuery(sql_query_string)
                if len(one_file_dict) == 0:
                    print("the following SQL Query get no return:" + sql_query_string)
                    continue
                file_dict.append(one_file_dict[0])

        return file_dict

def get_correction_time_for_survey(db, survey_id):
    sql_query_string = f'''SELECT EMA_answer.AnswerKey, EMA_translations.text 
                    from EMA_answer inner join EMA_translations on EMA_answer.AnswerKey = EMA_translations.Key 
                    inner join EMA_question on EMA_answer.Question_id  = EMA_question.ID 
                    where EMA_question.QuestionKey = "527d415b-35f7-4c68-a09d-f8e18e192d2d" AND 
                    EMA_answer.Questionnaire_id = "{survey_id}" '''
    Answerkey_and_text = db.executeQuery(sql_query_string)
    if not Answerkey_and_text:
        return -1
    #print(Answerkey_and_text)
    if Answerkey_and_text[0]['answerkey'] == 'c849f5d9-1c18-406e-bbe0-e4c9695a9007':
        return 0
    if Answerkey_and_text[0]['answerkey'] == '517bd3d1-b560-4fa1-98e6-cbaacda87198':
        return 2
    if Answerkey_and_text[0]['answerkey'] == 'dca21356-8de7-46ae-b5b2-99ee2d2c3687':
        return 5
    if Answerkey_and_text[0]['answerkey'] == '010ebb68-6b4e-4f1e-8544-2c15bc2c1d40':
        return 10
    if Answerkey_and_text[0]['answerkey'] == '33023cc9-9723-4931-9500-f48d86706237':
        return 15
    if Answerkey_and_text[0]['answerkey'] == '1035d82e-e005-4d63-8368-9f02cbdd28e4':
        return 20
    if Answerkey_and_text[0]['answerkey'] == 'f0680666-c7b4-4f32-9f1e-27bdcf4d231d':
        return 30
    


def get_data_for_files_in_dict(db, file_dict, keep_files = False):
        db.downloadFiles("./tmp", file_dict, True)
        # load all files
        alldata_stacked = []
        for one_dict in file_dict:
                filename = "./tmp/" + one_dict['subject'] + '/' + one_dict['filename']
                featdata = FeatureFile.load(filename)
                data = featdata.data
                alldata_stacked.append(data)
                if not keep_files:
                        os.remove(filename)
                
        nrofchunks = len(alldata_stacked)
        size_of_data = alldata_stacked[0].shape
        alldata = np.stack(alldata_stacked, axis=0).reshape((nrofchunks*size_of_data[0],size_of_data[1]))
        return alldata, alldata_stacked, featdata.fs

def get_data_for_files_in_onedict(db, file_dict, keep_files = False):
        db.downloadFiles("./tmp", file_dict, True)
        # load all files
        filename = "./tmp/" + file_dict[0]['subject'] + '/' + file_dict[0]['filename']
        featdata = FeatureFile.load(filename)
        data = featdata.data
        if not keep_files:
            os.remove(filename)
                
        #nrofchunks = len(alldata_stacked)
        #size_of_data = alldata_stacked[0].shape
        #alldata = np.stack(alldata_stacked, axis=0).reshape((nrofchunks*size_of_data[0],size_of_data[1]))
        return data, featdata.fs

def load_data_for_files_in_dict(file_dict):
        # load all files
        alldata_stacked = []
        for one_dict in file_dict:
            filename = "./tmp/" + one_dict['subject'] + '/' + one_dict['filename']
            featdata = FeatureFile.load(filename)
            data = featdata.data
            alldata_stacked.append(data)
                
        nrofchunks = len(alldata_stacked)
        size_of_data = alldata_stacked[0].shape
        alldata = np.stack(alldata_stacked, axis=0).reshape((nrofchunks*size_of_data[0],size_of_data[1]))
        return alldata, alldata_stacked, featdata.fs

"""
SELECT EMA_answer.AnswerKey, EMA_translations.text from EMA_answer inner join EMA_translations on EMA_answer.AnswerKey = EMA_translations."Key" inner join EMA_question on EMA_answer.Question_id  = EMA_question.ID where EMA_question.QuestionKey = "527d415b-35f7-4c68-a09d-f8e18e192d2d" AND EMA_Answer.Questionnaire_id = "2d42eb30-fd9a-47c4-bcb2-1934e98f95bc" 

dca21356-8de7-46ae-b5b2-99ee2d2c3687    5
010ebb68-6b4e-4f1e-8544-2c15bc2c1d40    10
c849f5d9-1c18-406e-bbe0-e4c9695a9007    0
517bd3d1-b560-4fa1-98e6-cbaacda87198    2
1035d82e-e005-4d63-8368-9f02cbdd28e4    20
33023cc9-9723-4931-9500-f48d86706237    15
f0680666-c7b4-4f32-9f1e-27bdcf4d231d    30


"""

def get_alltimes_objective_data_for_one_subject_one_study(db, subject, study):
    sql_query_string = f'SELECT EMA_datachunk.Start, EMA_datachunk.End FROM EMA_datachunk INNER JOIN EMA_study ON EMA_datachunk.Study_id = EMA_study.ID WHERE EMA_study.Name = "{study}" AND EMA_datachunk.Subject = "{subject}" '
    #sql_query_string = f'SELECT EMA_datachunk.Start, EMA_datachunk.End FROM EMA_datachunk INNER JOIN EMA_study ON EMA_datachunk.Study_id = EMA_study.ID WHERE EMA_datachunk.Subject = "{subject}" AND EMA_study.Name = "{study}" '
    print (sql_query_string)
    all_dates = db.executeQuery(sql_query_string)
    return all_dates
    #find unique days



    # return days



      

