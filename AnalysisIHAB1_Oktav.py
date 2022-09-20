"""
script and function to analyse ovd percent and RMS(a) for 5 minutes of data at the survey fill-out time- the given correction-time 

(c) J.Bitzer@Jade-hs
License: BSD 3-clause
Version 1.0.0 JB 19.01.2022
Version 1.1.0 JB 20.01.2022 added histogram analysis
"""
from gettext import npgettext
from time import strptime
from olMEGA_DataService_Client import olMEGA_DataService_Client
from datetime import datetime, timedelta
import random
from operator import itemgetter
from olMEGA_DataService_Tools import FeatureFile
from olMEGA_DataService_Tools import acousticweighting as aw
from olMEGA_DataService_Tools import freq2freqtransforms as freqt

import numpy as np
import pandas as pd
import os
import scipy.signal as sig

import matplotlib.pyplot as plt

client = olMEGA_DataService_Client.client(debug = True)

# some parameters
pre_analysis_time_in_min = 3
start_survey = 3
end_survey = 4 # set to -1 for all 

hist_min = 25
hist_max = 100

keep_feature_files = False

#result_filename = f"Results_EM1_OctavLevel{start_survey}_{end_survey}"
histogram_filename = f"Histo_Results_EM1_OctavLevel{start_survey}_{end_survey}"
if end_survey == -1:
#    result_filename = f"Results_EM1_OctavLevel{start_survey}_end"
    histogram_filename = f"Histo_Results_EM1_OctavLevel{start_survey}_end"

dummy, dummy2, f_nominal = freqt.get_spectrum_fractionaloctave_transformmatrix(1024,16000,125,4000,1)

#define table for histogram
entries = []
entries.append("subject")
entries.append("Survey_Filename")
entries.append(f"Level_Value_{pre_analysis_time_in_min}min")
for counter, fmid in enumerate(f_nominal):
    entries.append(f"Level_freq_all_{pre_analysis_time_in_min}min_Band{fmid}")
    entries.append(f"Level_freq_ov_{pre_analysis_time_in_min}min_Band{fmid}")
    entries.append(f"Level_freq_notov_{pre_analysis_time_in_min}min_Band{fmid}")
    
dfHist = pd.DataFrame(columns = entries)
dfHist[entries[3:]] = dfHist[entries[3:]].astype(np.int32)

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
hist_counter = 0     
all_participants = get_all_participants(client)


all_questionaires = get_all_questionaire_infos(client)
if end_survey == -1:
    end_survey = len(all_questionaires)

for survey_counter in range(start_survey,end_survey):
    print(f"{survey_counter} / {end_survey}")
    one_participant = all_questionaires[survey_counter]['subject']
    print(one_participant)
    questionaire_id = all_questionaires[survey_counter]['id']
    print(questionaire_id)
    survey_filename = all_questionaires[survey_counter]['filename']
    time_info = get_questionaire_fillout_time(client,questionaire_id)
    if time_info == None:
        print (survey_filename)
        continue
    correction_time = get_correction_time_for_survey(client, questionaire_id)
    
    exist_correction_time = True
    if (correction_time == -1):
        print("No correction time: ")
        exist_correction_time = False
        correction_time = 0

    #chunk = get_chunk_at_time(client, one_participant, time_info-timedelta(minutes=correction_time))
    chunk = get_chunk_at_time(client, one_participant, time_info)
    if len(chunk) == 0:
        print ("No data file at survey time: ")
        print (correction_time)
        print (survey_filename)
        continue
    
    chunk_start_time = datetime.strptime(chunk[0]['start'],'%Y-%m-%d %H:%M:%S')

    chunks = get_chunks_for_time_interval(client, one_participant,chunk_start_time-timedelta(minutes=pre_analysis_time_in_min), chunk_start_time)

    analysis_is_valid = len(chunks) == pre_analysis_time_in_min

    file_dict = get_filedict_for_chunks(client,chunks, 'psd')
    if len(file_dict) == 0:
        print("No PSD files in the desired 5 minutes")
        continue
    file_dict.sort(key= lambda d: d['filename']) 
    # client.downloadFiles("./tmp", file_dict, True)
    file_dictovd = get_filedict_for_chunks(client,chunks, 'ovd')
    if len(file_dictovd) == 0:
        print("No OVD files in the desired 5 minutes")
        continue
    
    file_dictovd.sort(key= lambda d: d['filename']) 

    psd_data, fs = get_data_for_files_in_dict(client,file_dict, keep_files=keep_feature_files)
    ovd_data, ovd_fs = get_data_for_files_in_dict(client,file_dictovd, keep_files=keep_feature_files)

    # Berechnungen in numpy

    n = [int(psd_data.shape[1] / 2), int(psd_data.shape[1] / 4)]
    Pxx = psd_data[:, n[0] : n[0] + n[1]]
    Pyy = psd_data[:, n[0] + n[1] : ]                    

    def removeStationaryTones(PSDSpectrum,fs):
        removed = 0
        if fs>10000:
            # look for disturbing tones at high frequencies and remove them
            analysis_percentile = 90
            perc_log = 10*np.log10(np.percentile(PSDSpectrum,analysis_percentile,axis = 0)) # percentile over time
            peaks, peaks_p = sig.find_peaks(perc_log,prominence=5, width=3)
            if len(peaks)>0:
                peaks = peaks[~(peaks<200)] # delete low freq maxima
                print(peaks)
                peaks_all = []
                for p in peaks:
                    peaks_ext = p + np.arange(-5,5)
                    peaks_all.extend(peaks_ext)
            
                PSDSpectrum[:,peaks_all] = 0
                removed = 1
        return PSDSpectrum, removed

    Pxx, flag = removeStationaryTones(Pxx,fs)
    Pyy, flag2 = removeStationaryTones(Pyy,fs)
    flag_disturb_tones_removed = flag or flag2
    
    nr_of_frames, fft_size = Pxx.shape
    if fs == 8000:
        magicPSDconvert = 0.4
        magicPSDconvertCorrect = 24
    elif fs == 24000:
        magicPSDconvert = 0.3
        magicPSDconvertCorrect = 28
    
    #w,f = aw.get_fftweight_vector((fft_size-1)*2,fs,weighting_func,'lin')
    octav_matrix, f_mid, f_nominal = freqt.get_spectrum_fractionaloctave_transformmatrix((fft_size-1)*2,fs,125,4000,1)

    octavPSD = (((Pxx+Pyy)*magicPSDconvert*fs/((fft_size-1)*2))@octav_matrix) # this works because of broadcasting rules in python
        
    # histogram analysis
    
    def get_histogram(data, hist_min = None, hist_max = None, nr_of_bins = -1):
        if hist_min is None:
            hist_min = np.min(data)
            
        if hist_max is None:
            hist_max = np.max(data)
    
        if nr_of_bins == -1:
            nr_of_bins = hist_max-hist_min
            binwidth = 1
        else:
            binwidth = (hist_max-hist_min)/nr_of_bins
            
        smaller_min = len(data[data < hist_min-binwidth*0.5])
        larger_max = len(data[data > hist_max-binwidth*0.5])
        result,bin_edges = np.histogram(data, bins = nr_of_bins, range=(hist_min-0.5,hist_max-0.5))
        return result, smaller_min, larger_max
    
    octaveLevel_all = 10*np.log10(np.mean((octavPSD), axis=0)) # mean over time
    hist_allfreq = []
    small_allfreq = []
    high_allfreq = []
    for freqcounter, freq in enumerate(f_nominal):
        hist_result, small_result, high_result = get_histogram(10*np.log10(octavPSD[:,freqcounter]),hist_min,hist_max)
        hist_allfreq.append(hist_result)
        small_allfreq.append(small_result)
        high_allfreq.append(high_result)
        
    OVD_percent = np.mean(ovd_data)
    # OV
    octaveLevel_ov = []
    hist_allfreq_ov = []
    small_allfreq_ov = []
    high_allfreq_ov = []

    if (OVD_percent>0 and octavPSD.shape[0] == len(ovd_data)):
        octav_psd_ov = octavPSD[ovd_data[:,0] == 1,:]
        for freqcounter, freq in enumerate(f_nominal):
            hist_result, small_result, high_result = get_histogram(10*np.log10(octav_psd_ov[:,freqcounter]),hist_min,hist_max)
            hist_allfreq_ov.append(hist_result)
            small_allfreq_ov.append(small_result)
            high_allfreq_ov.append(high_result)
        octaveLevel_OV = 10*np.log10(np.mean(octav_psd_ov,axis= 0))
    else:
        hist_allfreq_ov = list(np.zeros_like(hist_allfreq))
        small_allfreq_ov = list(np.zeros_like(small_allfreq))
        high_allfreq_ov = list(np.zeros_like(high_allfreq))
    # without OV
    octaveLevel_notov = []
    hist_allfreq_notov = []
    small_allfreq_notov = []
    high_allfreq_notov = []

    if (OVD_percent<1 and octavPSD.shape[0] == len(ovd_data)):
        octav_psd_notOV = octavPSD[ovd_data[:,0] != 1,:]
        for freqcounter, freq in enumerate(f_nominal):
            hist_result, small_result, high_result = get_histogram(10*np.log10(octav_psd_notOV[:,freqcounter]),hist_min,hist_max)
            hist_allfreq_notov.append(hist_result)
            small_allfreq_notov.append(small_result)
            high_allfreq_notov.append(high_result)
        octaveLevel_notov = 10*np.log10(np.mean(octav_psd_notOV,axis= 0))
    else:
        hist_allfreq_notov = list(np.zeros_like(hist_allfreq))
        small_allfreq_notov = list(np.zeros_like(small_allfreq))
        high_allfreq_notov = list(np.zeros_like(high_allfreq))

    def writeHistResults(participant, survey, binval, val_all, val_ov, val_notov):
        dfHist.loc[hist_counter,"subject"] = participant
        dfHist.loc[hist_counter,"Survey_Filename"] = survey
        dfHist.loc[hist_counter, f"Level_Value_{pre_analysis_time_in_min}min"] = binval
        for counter, fmid in enumerate(f_nominal):
            dfHist.loc[hist_counter, f"Level_freq_all_{pre_analysis_time_in_min}min_Band{fmid}"] = val_all[counter]
            dfHist.loc[hist_counter, f"Level_freq_ov_{pre_analysis_time_in_min}min_Band{fmid}"] = val_ov[counter]
            dfHist.loc[hist_counter, f"Level_freq_notov_{pre_analysis_time_in_min}min_Band{fmid}"] = val_notov[counter]
        
    writeHistResults(one_participant,survey_filename, hist_min-1, small_allfreq, small_allfreq_ov, small_allfreq_notov)

    hist_counter+=1
    # all hist value
    temp = np.array(hist_allfreq)
    hist_allfreq = list(temp.transpose())
    temp = np.array(hist_allfreq_ov)
    hist_allfreq_ov = list(temp.transpose())
    temp = np.array(hist_allfreq_notov)
    hist_allfreq_notov = list(temp.transpose())
    for hist_val in range(hist_min,hist_max):
#        if len(hist_result_ov) != 0:
#            ovhistentry =  hist_result_ov[hist_val-hist_min]
#        else:
#            ovhistentry = No
#        if len(hist_result_withouOV) != 0:
#            notov_histentry =  hist_result_withouOV[hist_val-hist_min]
#        else:
#            notov_histentry = None
        index = int(hist_val-hist_min)
        
        writeHistResults(one_participant,survey_filename, hist_val, hist_allfreq[index], hist_allfreq_ov[index], hist_allfreq_notov[index])
        hist_counter+=1
        
    #higher hist_max
    writeHistResults(one_participant,survey_filename, hist_max, high_allfreq, high_allfreq_ov, high_allfreq_notov)

    hist_counter+=1

dfHist.to_csv(histogram_filename + '.csv')

import pyreadstat

pyreadstat.write_sav(dfHist, histogram_filename+'.sav')


client.close()

print ("Done...")
pass
