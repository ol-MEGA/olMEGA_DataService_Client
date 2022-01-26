"""
script and function to analyse ovd percent and RMS(a) for 5 minutes of data at the survey fill-out time- the given correction-time 

(c) J.Bitzer@Jade-hs
License: BSD 3-clause
Version 1.0.0 JB 19.01.2022
"""
from gettext import npgettext
from time import strptime
from olMEGA_DataService_Client import olMEGA_DataService_Client
from datetime import datetime, timedelta
import random
from operator import itemgetter
from olMEGA_DataService_Tools import FeatureFile
from olMEGA_DataService_Tools import acousticweighting as aw

import numpy as np
import pandas as pd
import os

client = olMEGA_DataService_Client.client(debug = True)

# some parameters
pre_analysis_time_in_min = 5
start_survey = 0
end_survey = -1 # set to -1 for all 

keep_feature_files = False

result_filename = "Results_Petra1"

#define resulting table
df = pd.DataFrame(columns=["subject", "Survey_Filename","Survey_Starttime", "Chunk_Starttime", "Correction_Time", "Samplerate",
                           f"is_valid_{pre_analysis_time_in_min}min", f"OVD_percent_{pre_analysis_time_in_min}min",f"RMSa_overall_{pre_analysis_time_in_min}min",
                           f"RMSa_OV_only_{pre_analysis_time_in_min}min",f"RMSa_without_OV_{pre_analysis_time_in_min}min" ] )

df[["Samplerate",f"is_valid_{pre_analysis_time_in_min}min", f"OVD_percent_{pre_analysis_time_in_min}min", f"RMSa_overall_{pre_analysis_time_in_min}min", 
    f"RMSa_OV_only_{pre_analysis_time_in_min}min", f"RMSa_without_OV_{pre_analysis_time_in_min}min"]]= df[[
        "Samplerate", f"is_valid_{pre_analysis_time_in_min}min", f"OVD_percent_{pre_analysis_time_in_min}min", 
        f"RMSa_overall_{pre_analysis_time_in_min}min", f"RMSa_OV_only_{pre_analysis_time_in_min}min", 
        f"RMSa_without_OV_{pre_analysis_time_in_min}min"]].astype(np.float32)




def get_all_participants(db):
        return db.executeQuery('SELECT DISTINCT Subject FROM EMA_datachunk')

def get_all_questionaire_infos (db):
        sql_query_string = f'SELECT EMA_questionnaire.id, EMA_datachunk.Subject, EMA_questionnaire.Filename FROM EMA_questionnaire INNER JOIN EMA_datachunk ON EMA_questionnaire.DataChunk_id = EMA_datachunk.ID '
        #print (sql_query_string)
        return db.executeQuery(sql_query_string)


def get_all_questionaire_filenames_for_one_subject (db, participent_pseudoname):
        sql_query_string = f'SELECT EMA_questionnaire.id, EMA_questionnaire.SurveyFile FROM EMA_questionnaire INNER JOIN EMA_datachunk ON EMA_questionnaire.DataChunk_id = EMA_datachunk.ID WHERE EMA_datachunk.Subject == "{participent_pseudoname}" '
        #print (sql_query_string)
        return db.executeQuery(sql_query_string)

def get_questionaire_fillout_time(db, questionaire_id):
        sql_query_string = f'''SELECT EMA_questionnaire.Start
                        FROM EMA_answer INNER JOIN EMA_questionnaire on EMA_answer.Questionnaire_id = EMA_questionnaire.ID 
                        INNER JOIN EMA_question on EMA_answer.Question_id = EMA_question.ID 
                        WHERE EMA_answer.Questionnaire_id = "{questionaire_id}" AND 
                        EMA_question.QuestionKey = "527d415b-35f7-4c68-a09d-f8e18e192d2d"'''
                                
        query_answer = db.executeQuery(sql_query_string)
        if len(query_answer) == 0:
            print("A survey returns no start time" + sql_query_string)
            return None
        questionaire_start_time = datetime.strptime(query_answer[0]['start'],'%Y-%m-%d %H:%M:%S')
        
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
                    EMA_Answer.Questionnaire_id = "{survey_id}" '''
    Answerkey_and_text = db.executeQuery(sql_query_string)
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
        return alldata, featdata.fs

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
     
all_participants = get_all_participants(client)


all_questionaires = get_all_questionaire_infos(client)
if end_survey == -1:
    end_survey = len(all_questionaires)

for survey_counter in range(start_survey,end_survey):
    print(f"{survey_counter} / {end_survey}")
    one_participant = all_questionaires[survey_counter]['subject']
    questionaire_id = all_questionaires[survey_counter]['id']
    survey_filename = all_questionaires[survey_counter]['filename']
    time_info = get_questionaire_fillout_time(client,questionaire_id)
    if time_info == None:
        continue
    correction_time = get_correction_time_for_survey(client, questionaire_id)

    #chunk = get_chunk_at_time(client, one_participant, time_info-timedelta(minutes=correction_time))
    chunk = get_chunk_at_time(client, one_participant, time_info)
    if len(chunk) == 0:
        continue
    
    chunk_start_time = datetime.strptime(chunk[0]['start'],'%Y-%m-%d %H:%M:%S')

    chunks = get_chunks_for_time_interval(client, one_participant,chunk_start_time-timedelta(minutes=pre_analysis_time_in_min), chunk_start_time)

    analysis_is_valid = len(chunks) == pre_analysis_time_in_min

    file_dict = get_filedict_for_chunks(client,chunks, 'psd')
    if len(file_dict) == 0:
        continue
    file_dict.sort(key= lambda d: d['filename']) 
    # client.downloadFiles("./tmp", file_dict, True)
    file_dictovd = get_filedict_for_chunks(client,chunks, 'ovd')
    if len(file_dictovd) == 0:
        continue
    
    file_dictovd.sort(key= lambda d: d['filename']) 

    psd_data, fs = get_data_for_files_in_dict(client,file_dict, keep_files=keep_feature_files)
    ovd_data, ovd_fs = get_data_for_files_in_dict(client,file_dictovd, keep_files=keep_feature_files)

    # Berechnungen in numpy

    n = [int(psd_data.shape[1] / 2), int(psd_data.shape[1] / 4)]
    Pxx = psd_data[:, n[0] : n[0] + n[1]]
    Pyy = psd_data[:, n[0] + n[1] : ]                    

    nr_of_frames, fft_size = Pxx.shape
    w,f = aw.get_fftweight_vector((fft_size-1)*2,fs,'a','lin')
    meanPSD = (((Pxx+Pyy)*0.5*fs)*w)*0.25 # this works because of broadcasting rules in python
    rms_psd = 10*np.log10(np.mean((meanPSD), axis=1)) # mean over frequency
    rms_psd_premin_all = np.mean(rms_psd)
    rms_psd_premin_OV = None
    rms_psd_premin_withoutOV = None
    OVD_percent = np.mean(ovd_data)
    if (OVD_percent>0 and len(rms_psd) == len(ovd_data)):
            rms_psd_premin_OV = np.mean(rms_psd[ovd_data[:,0] == 1])
    if (OVD_percent<1 and len(rms_psd) == len(ovd_data)):
            rms_psd_premin_withoutOV = np.mean(rms_psd[ovd_data[:,0] != 1])



    df.loc[survey_counter,"subject"] = one_participant
    df.loc[survey_counter,"Survey_Filename"] = survey_filename
    df.loc[survey_counter,"Survey_Starttime"] = time_info
    df.loc[survey_counter,"Chunk_Starttime"] = chunk_start_time
    df.loc[survey_counter,"Correction_Time"] = correction_time
    df.loc[survey_counter,"Samplerate"] = fs
    df.loc[survey_counter,f"is_valid_{pre_analysis_time_in_min}min"] = analysis_is_valid
    df.loc[survey_counter,f"OVD_percent_{pre_analysis_time_in_min}min"] = OVD_percent
    df.loc[survey_counter,f"RMSa_overall_{pre_analysis_time_in_min}min"] = rms_psd_premin_all
    df.loc[survey_counter,f"RMSa_OV_only_{pre_analysis_time_in_min}min"] = rms_psd_premin_OV
    df.loc[survey_counter,f"RMSa_without_OV_{pre_analysis_time_in_min}min"] = rms_psd_premin_withoutOV

#print(df.head())

df.to_csv(result_filename + '.csv')

import pyreadstat

pyreadstat.write_sav(df, result_filename+'.sav')


client.close()

print ("Done...")
pass
