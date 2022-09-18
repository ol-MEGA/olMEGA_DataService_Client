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
import mosqito as mq

import numpy as np
import pandas as pd
import os
import scipy.signal as sig

import matplotlib.pyplot as plt

client = olMEGA_DataService_Client.client(debug = True)

# some parameters
pre_analysis_time_in_min = 3
weighting_func = 'z'
start_survey = 0
end_survey = 4 # set to -1 for all 
fmax_oktavanalysis = 4000
hist_min = 25
hist_max = 100
dummy, dummy2, f_nominal = freqt.get_spectrum_fractionaloctave_transformmatrix(1024,16000,125,fmax_oktavanalysis,1)

keep_feature_files = False

result_filename = f"Results_EM1_{weighting_func}_weighting_{pre_analysis_time_in_min}min_{start_survey}_{end_survey}"
histogram_filename = f"Histo_Results_EM1__{weighting_func}_weighting_{pre_analysis_time_in_min}min_{start_survey}_{end_survey}"
if end_survey == -1:
    result_filename = f"Results_EM1__{weighting_func}_weighting_{pre_analysis_time_in_min}min_{start_survey}_end"
    histogram_filename = f"Histo_Results_EM1__{weighting_func}_weighting_{pre_analysis_time_in_min}min_{start_survey}_end"

#define resulting table for result
entries = []
entries.append("subject")
entries.append("Survey_Filename")
entries.append("Survey_Filename")
entries.append("Survey_Starttime")
entries.append("Chunk_Starttime")
entries.append("Correction_Time")
entries.append("Samplerate")
entries.append(f"is_valid_{pre_analysis_time_in_min}min")
entries.append(f"OVD_percent_{pre_analysis_time_in_min}min")
entries.append(f"RMS{weighting_func}_overall_{pre_analysis_time_in_min}min")
entries.append(f"RMS{weighting_func}_OV_only_{pre_analysis_time_in_min}min")
entries.append(f"RMS{weighting_func}_without_OV_{pre_analysis_time_in_min}min")
entries.append(f"RMS{weighting_func}_tones_removed{pre_analysis_time_in_min}min")
for counter, fmid in enumerate(f_nominal):
    entries.append(f"OctavLevel_mean_all_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}")
    entries.append(f"OctavLevel_var_all_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}")
    entries.append(f"OctavLevel_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}")
    entries.append(f"OctavLevel_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}")
    entries.append(f"OctavLevel_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}")
    entries.append(f"OctavLevel_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}")

entries.append(f"Loudness_mean_all_RMS{weighting_func}_{pre_analysis_time_in_min}min")
entries.append(f"Loudness_var_all_RMS{weighting_func}_{pre_analysis_time_in_min}min")
entries.append(f"Sharpness_mean_all_RMS{weighting_func}_{pre_analysis_time_in_min}min")
entries.append(f"Sharpness_var_all_RMS{weighting_func}_{pre_analysis_time_in_min}min")
entries.append(f"Loudness_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
entries.append(f"Loudness_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
entries.append(f"Sharpness_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
entries.append(f"Sharpness_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
entries.append(f"Loudness_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
entries.append(f"Loudness_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
entries.append(f"Sharpness_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
entries.append(f"Sharpness_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min")



df = pd.DataFrame(columns= entries)
df[entries[6:]] = df[entries[6:]].astype(np.float32)

entriesH = []
entriesH.append("subject")
entriesH.append("Survey_Filename")
entriesH.append(f"RMS_{weighting_func}_Value_{pre_analysis_time_in_min}min")
entriesH.append(f"RMS_{weighting_func}_freq_all_{pre_analysis_time_in_min}min")
entriesH.append(f"RMS_{weighting_func}_freq_OV_{pre_analysis_time_in_min}min")
entriesH.append(f"RMS_{weighting_func}_freq_withoutOV_{pre_analysis_time_in_min}min")

dfHist = pd.DataFrame(columns = entriesH)
dfHist[entriesH[3:]] = dfHist[entriesH[3:]].astype(np.int32)


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
    w,f = aw.get_fftweight_vector((fft_size-1)*2,fs,weighting_func,'lin')
    meanPSD = (((Pxx+Pyy)*0.5*fs)*w*w)*0.25 # this works because of broadcasting rules in python
        
    rms_psd = 10*np.log10(np.mean((meanPSD), axis=1)) # mean over frequency
    octav_matrix, f_mid, f_nominal = freqt.get_spectrum_fractionaloctave_transformmatrix((fft_size-1)*2,fs,125,fmax_oktavanalysis,1)
    octavLevel = (((Pxx+Pyy)*w*w*0.5*fs/((fft_size-1)*2))@octav_matrix) # this works because of broadcasting rules in python
    
    stereoPSD = (Pxx+Pyy)*0.5
    stereoPSD = np.sqrt(stereoPSD)*np.sqrt(fs/((fft_size-1)*2))
    stereoPSD = stereoPSD.transpose()
    if fs >= 32000:
        stereoPSD_final = stereoPSD
        freq_vec = np.linspace(0,fs/2, num = int(fft_size))
    elif fs>=16000 and fs < 32000:
        stereoPSD_final = np.zeros((2*stereoPSD.shape[0], stereoPSD.shape[1]))
        stereoPSD_final[0:stereoPSD.shape[0],:] = stereoPSD/np.sqrt(2)
        freq_vec = np.linspace(0,2*fs/2, num = 2*int(fft_size))
    elif fs>=8000 and fs < 16000:
        stereoPSD_final = np.zeros((4*stereoPSD.shape[0], stereoPSD.shape[1]))
        stereoPSD_final[0:stereoPSD.shape[0],:] = stereoPSD/2
        freq_vec = np.linspace(0,4*fs/2, num = 4*int(fft_size))
    else:
        print("Samplingrate to low")

    loudness_calibration_db = -90
    loudness_calibration = 10**(loudness_calibration_db/20)
    stereoPSD_final *= loudness_calibration        
    loudness,N_specific,bark_bands = mq.loudness_zwst_freq(stereoPSD_final, freq_vec)
    #stereoPSD_final = stereoPSD_final.reshape(513,)
    sharpness = mq.sharpness_din_from_loudness(loudness, N_specific)
    
    # todo build functions to save this histogram to the dataframe
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
    
    hist_result, smallRMS_all, highRMS_all = get_histogram(rms_psd,hist_min,hist_max)
    rms_psd_premin_all = np.mean(rms_psd)
    
    octavLevel_mean_all = np.mean(octavLevel)
    octavLevel_var_all = np.var(10*np.log10(octavLevel))
    
    loudness_mean_all = np.mean(loudness)
    loudness_var_all = np.var(loudness)
    sharpness_mean_all = np.mean(sharpness)
    sharpness_var_all = np.var(sharpness)
    
    OVD_percent = np.mean(ovd_data)
    # OV
    rms_psd_premin_OV = None
    smallRMS_ov = None
    highRMS_ov = None
    hist_result_ov = []
    octavLevel_mean_ov = None
    octavLevel_var_ov = None
    
    if (OVD_percent>0 and len(rms_psd) == len(ovd_data)):
        rms_psd_ov = rms_psd[ovd_data[:,0] == 1]
        hist_result_ov, smallRMS_ov, highRMS_ov = get_histogram(rms_psd_ov, hist_min, hist_max)
        rms_psd_premin_OV = np.mean(rms_psd_ov)

        octavLevel_ov = octavLevel[ovd_data[:,0] == 1]
        octavLevel_mean_ov = np.mean(octavLevel_ov)
        octavLevel_var_ov = np.var(10*np.log10(octavLevel_ov))

        loudness_ov = loudness[ovd_data[:,0] == 1]
        sharpness_ov = sharpness[ovd_data[:,0] == 1]
        loudness_mean_ov = np.mean(loudness_ov)
        loudness_var_ov = np.var(loudness_ov)
        sharpness_mean_ov = np.mean(sharpness_ov)
        sharpness_var_ov = np.var(sharpness_ov)
        

    # without OV
    rms_psd_premin_withoutOV = None
    smallRMS_withouOV = None
    highRMS_withouOV = None
    hist_result_withouOV = []
    octavLevel_mean_notov = None
    octavLevel_var_notov = None

    if (OVD_percent<1 and len(rms_psd) == len(ovd_data)):
        rms_psd_withoutOV = rms_psd[ovd_data[:,0] != 1]
        hist_result_withouOV, smallRMS_withouOV, highRMS_withouOV = get_histogram(rms_psd_withoutOV, hist_min, hist_max)
        rms_psd_premin_withoutOV = np.mean(rms_psd_withoutOV)
        octavLevel_notov = octavLevel[ovd_data[:,0] != 1]
        octavLevel_mean_notov = np.mean(octavLevel_notov)
        octavLevel_var_notov = np.var(10*np.log10(octavLevel_notov))
        loudness_notov = loudness[ovd_data[:,0] != 1]
        sharpness_notov = sharpness[ovd_data[:,0] != 1]
        loudness_mean_notov = np.mean(loudness_notov)
        loudness_var_notov = np.var(loudness_notov)
        sharpness_mean_notov = np.mean(sharpness_notov)
        sharpness_var_notov = np.var(sharpness_notov)


    def writeHistResults(participant, survey, binval, val_all, val_ov, val_notov):
        dfHist.loc[hist_counter,"subject"] = participant
        dfHist.loc[hist_counter,"Survey_Filename"] = survey
        dfHist.loc[hist_counter, f"RMS_{weighting_func}_Value_{pre_analysis_time_in_min}min"] = binval
        dfHist.loc[hist_counter, f"RMS_{weighting_func}_freq_all_{pre_analysis_time_in_min}min"] = val_all
        if val_ov is not None:
            dfHist.loc[hist_counter, f"RMS_{weighting_func}_freq_OV_{pre_analysis_time_in_min}min"] = val_ov
        if val_notov is not None:
            dfHist.loc[hist_counter, f"RMS_{weighting_func}_freq_withoutOV_{pre_analysis_time_in_min}min"] = val_notov
        
    writeHistResults(one_participant,survey_filename, hist_min-1, smallRMS_all, smallRMS_ov, smallRMS_withouOV)

    hist_counter+=1
    # all hist value
    for hist_val in range(hist_min,hist_max):
        if len(hist_result_ov) != 0:
            ovhistentry =  hist_result_ov[hist_val-hist_min]
        else:
            ovhistentry = None
        if len(hist_result_withouOV) != 0:
            notov_histentry =  hist_result_withouOV[hist_val-hist_min]
        else:
            notov_histentry = None
        
        writeHistResults(one_participant,survey_filename, hist_val, hist_result[hist_val-hist_min], ovhistentry, notov_histentry)
        
        hist_counter+=1
        
    #higher hist_max
    writeHistResults(one_participant,survey_filename, hist_max, highRMS_all, highRMS_ov, highRMS_withouOV)

    hist_counter+=1

    
    df.loc[survey_counter,"subject"] = one_participant
    df.loc[survey_counter,"Survey_Filename"] = survey_filename
    df.loc[survey_counter,"Survey_Starttime"] = time_info
    df.loc[survey_counter,"Chunk_Starttime"] = chunk_start_time
    
    if exist_correction_time:
        df.loc[survey_counter,"Correction_Time"] = correction_time
    else:
        df.loc[survey_counter,"Correction_Time"] = -1
    
    df.loc[survey_counter,"Samplerate"] = fs
    df.loc[survey_counter,f"is_valid_{pre_analysis_time_in_min}min"] = len(chunks)
    df.loc[survey_counter,f"OVD_percent_{pre_analysis_time_in_min}min"] = OVD_percent
    df.loc[survey_counter,f"RMS{weighting_func}_overall_{pre_analysis_time_in_min}min"] = rms_psd_premin_all
    df.loc[survey_counter,f"RMS{weighting_func}_OV_only_{pre_analysis_time_in_min}min"] = rms_psd_premin_OV
    df.loc[survey_counter,f"RMS{weighting_func}_without_OV_{pre_analysis_time_in_min}min"] = rms_psd_premin_withoutOV
    df.loc[survey_counter,f"RMS{weighting_func}_tones_removed{pre_analysis_time_in_min}min"] = flag_disturb_tones_removed

    for counter, fmid in enumerate(f_nominal):
        df.loc[survey_counter,f"OctavLevel_mean_all_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = 10*np.log10(octavLevel_mean_all)
        df.loc[survey_counter,f"OctavLevel_var_all_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = (octavLevel_var_all)
        if octavLevel_mean_ov is not None:
            df.loc[survey_counter,f"OctavLevel_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = 10*np.log10(octavLevel_mean_ov)
        else:
            df.loc[survey_counter,f"OctavLevel_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = None    
        df.loc[survey_counter,f"OctavLevel_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = (octavLevel_var_ov)
        if octavLevel_mean_notov is not None:
            df.loc[survey_counter,f"OctavLevel_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = 10*np.log10(octavLevel_mean_notov)
        else:
            df.loc[survey_counter,f"OctavLevel_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = None
        df.loc[survey_counter,f"OctavLevel_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = (octavLevel_var_notov)

    df.loc[survey_counter,f"Loudness_mean_all_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = loudness_mean_all
    df.loc[survey_counter,f"Loudness_var_all_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = loudness_var_all
    df.loc[survey_counter,f"Sharpness_mean_all_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = sharpness_mean_all
    df.loc[survey_counter,f"Sharpness_var_all_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = sharpness_var_all
    df.loc[survey_counter,f"Loudness_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = loudness_mean_ov
    df.loc[survey_counter,f"Loudness_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = loudness_var_ov
    df.loc[survey_counter,f"Sharpness_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = sharpness_mean_ov
    df.loc[survey_counter,f"Sharpness_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = sharpness_var_ov
    df.loc[survey_counter,f"Loudness_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = loudness_mean_notov
    df.loc[survey_counter,f"Loudness_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = loudness_var_notov
    df.loc[survey_counter,f"Sharpness_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = sharpness_mean_notov
    df.loc[survey_counter,f"Sharpness_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = sharpness_var_notov

#print(df.head())

df.to_csv(result_filename + '.csv')
dfHist.to_csv(histogram_filename + '.csv')

import pyreadstat

pyreadstat.write_sav(df, result_filename+'.sav')
pyreadstat.write_sav(dfHist, histogram_filename+'.sav')


client.close()

print ("Done...")
pass
