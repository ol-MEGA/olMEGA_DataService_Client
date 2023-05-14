"""
script and function to analyse of all objective data

(c) J.Bitzer@Jade-hs
License: BSD 3-clause
Version 1.0.0 JB 9.05.2023

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
import olMEGA_DataService_Client.olMegaQueries as olMQ

import mosqito as mq

import numpy as np
import pandas as pd
import os
import scipy.signal as sig

import matplotlib.pyplot as plt
import json

def get_unique_days_from_starttime_list(list_of_start_times):
    chunk_start_times = set()
    for kk in range(len(list_of_start_times)):
        chunk_start_times.add(datetime.strptime(list_of_start_times[kk]['start'],'%Y-%m-%d %H:%M:%S').date())
    
    return sorted(chunk_start_times)

def removeStationaryTones(PSDSpectrum,fs):
    removed = 0
    PSD_return = PSDSpectrum.copy()
    if fs>10000:
        # look for disturbing tones at high frequencies and remove them
        analysis_percentile = 90
        perc_log = 10*np.log10(np.percentile(PSDSpectrum,analysis_percentile,axis = 0)) # percentile over time
        peaks, peaks_p = sig.find_peaks(perc_log,prominence=5, width=3)
        if len(peaks)>0:
            peaks = peaks[~(peaks<200)] # delete low freq maxima
            #print(peaks)
            peaks_all = []
            for p in peaks:
                ext_max = np.min([5,512-p])
                peaks_ext = p + np.arange(-5,ext_max)
                peaks_all.extend(peaks_ext)
        
            PSD_return[:,peaks_all] = 0
            removed = 1
    return PSD_return, removed



client = olMEGA_DataService_Client.client(debug = True)
weighting_func = 'a'
keep_feature_files = True
fmax_oktavanalysis = 4000

# hist_min = 25
# hist_max = 100
# dummy, dummy2, f_nominal = freqt.get_spectrum_fractionaloctave_transformmatrix(1024,16000,125,fmax_oktavanalysis,1)


# result_filename = f"Results_EM1_{weighting_func}_weighting_{pre_analysis_time_in_min}min_{start_survey}_{end_survey}"
# histogram_filename = f"Histo_Results_EM1__{weighting_func}_weighting_{pre_analysis_time_in_min}min_{start_survey}_{end_survey}"
# if end_survey == -1:
#     result_filename = f"Results_EM1__{weighting_func}_weighting_{pre_analysis_time_in_min}min_{start_survey}_end"
#     histogram_filename = f"Histo_Results_EM1__{weighting_func}_weighting_{pre_analysis_time_in_min}min_{start_survey}_end"

# #define resulting table for result
# entries = []
# entries.append("subject")
# entries.append("Survey_Filename")
# entries.append("Survey_Starttime")
# entries.append("Chunk_Starttime")
# entries.append("Correction_Time")
# entries.append("Samplerate")
# entries.append(f"is_valid_{pre_analysis_time_in_min}min")
# entries.append(f"OVD_percent_{pre_analysis_time_in_min}min")
# entries.append(f"RMS{weighting_func}_overall_{pre_analysis_time_in_min}min")
# entries.append(f"RMS{weighting_func}_OV_only_{pre_analysis_time_in_min}min")
# entries.append(f"RMS{weighting_func}_without_OV_{pre_analysis_time_in_min}min")
# entries.append(f"RMS{weighting_func}_tones_removed{pre_analysis_time_in_min}min")
# for counter, fmid in enumerate(f_nominal):
#     entries.append(f"OctavLevel_mean_all_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}")
#     entries.append(f"OctavLevel_var_all_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}")
#     entries.append(f"OctavLevel_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}")
#     entries.append(f"OctavLevel_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}")
#     entries.append(f"OctavLevel_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}")
#     entries.append(f"OctavLevel_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}")

# entries.append(f"Loudness_mean_all_RMS{weighting_func}_{pre_analysis_time_in_min}min")
# entries.append(f"Loudness_var_all_RMS{weighting_func}_{pre_analysis_time_in_min}min")
# entries.append(f"Sharpness_mean_all_RMS{weighting_func}_{pre_analysis_time_in_min}min")
# entries.append(f"Sharpness_var_all_RMS{weighting_func}_{pre_analysis_time_in_min}min")
# entries.append(f"Loudness_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
# entries.append(f"Loudness_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
# entries.append(f"Sharpness_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
# entries.append(f"Sharpness_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
# entries.append(f"Loudness_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
# entries.append(f"Loudness_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
# entries.append(f"Sharpness_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min")
# entries.append(f"Sharpness_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min")



# df = pd.DataFrame(columns= entries)
# df[entries[5:]] = df[entries[5:]].astype(np.float32)

# entriesH = []
# entriesH.append("subject")
# entriesH.append("Survey_Filename")
# entriesH.append(f"RMS_{weighting_func}_Value_{pre_analysis_time_in_min}min")
# entriesH.append(f"RMS_{weighting_func}_freq_all_{pre_analysis_time_in_min}min")
# entriesH.append(f"RMS_{weighting_func}_freq_OV_{pre_analysis_time_in_min}min")
# entriesH.append(f"RMS_{weighting_func}_freq_withoutOV_{pre_analysis_time_in_min}min")

# dfHist = pd.DataFrame(columns = entriesH)
# dfHist[entriesH[3:]] = dfHist[entriesH[3:]].astype(np.int32)



#hist_counter = 0     
#all_participants = olMQ.get_all_participants(client)
all_studies = olMQ.get_all_studynames(client)

#study_name = 'EMA2'
for study_count, study_name in enumerate(all_studies):
    if (study_count != 0):
        continue
    
    print (study_name)
    #participants_EMA1 = olMQ.get_all_participants_for_study(client, study_name)
    participants_EMA = olMQ.get_all_participants_for_study(client, study_name["name"])

    for participant_count, participant in enumerate(participants_EMA):
        if (participant_count>2):
            continue

        print(participant)
        data = olMQ.get_alltimes_objective_data_for_one_subject_one_study(client, participant["subject"], study_name["name"])
        print(len(data))
        if (len(data) == 0):
            continue


        days_of_subject = get_unique_days_from_starttime_list(data)
        for chosen_day, oneday in enumerate(days_of_subject):
            print(chosen_day)
            if chosen_day > 1:
                continue
            chunks_psddata = olMQ.get_chunks_and_filenames_for_time_interval(client, participant["subject"],oneday, oneday + timedelta(hours=24),'psd')
            chunks_psddata.sort(key= lambda d: d['filename']) 

            chunks_ovddata = olMQ.get_chunks_and_filenames_for_time_interval(client, participant["subject"],oneday, oneday + timedelta(hours=24),'ovd')
            chunks_ovddata.sort(key= lambda d: d['filename']) 
            print('chunks da')

            #psd_data, psd_data_stacked, fs = olMQ.get_data_for_files_in_dict(client,chunks_psddata, keep_files=keep_feature_files)
            #ovd_data, ovd_data_stacked, ovd_fs = olMQ.get_data_for_files_in_dict(client,chunks_ovddata, keep_files=keep_feature_files)

            psd_data, psd_data_stacked, fs = olMQ.load_data_for_files_in_dict(chunks_psddata)
            ovd_data, ovd_data_stacked, ovd_fs = olMQ.load_data_for_files_in_dict(chunks_ovddata)

            n = [int(psd_data.shape[1] / 2), int(psd_data.shape[1] / 4)]
            #Pxx = psd_data[:, n[0] : n[0] + n[1]]
            #Pyy = psd_data[:, n[0] + n[1] : ]                    
            #Pxx, flag = removeStationaryTones(Pxx,fs)
            #Pyy, flag2 = removeStationaryTones(Pyy,fs)
            #flag_disturb_tones_removed = flag or flag2
                
            #nr_of_frames, fft_size = Pxx.shape
            
            magicPSDconvert = 1
            if fs == 8000: #legacy versions
                magicPSDconvert = 0.4
                magicPSDconvertCorrect = 24
            elif fs == 24000:
                magicPSDconvert = 0.3
                magicPSDconvertCorrect = 28


            results = []

            analyse_percentiles = [5, 30, 95, 99]

            for kk, oneminute in enumerate(psd_data_stacked):
                resultentry = {}

                if (kk < len(ovd_data_stacked)):
                    curOVD = ovd_data_stacked[kk]
                    OVD_percent = float(np.mean(curOVD))
                else:
                    curOVD = []
                    OVD_percent = -1.0

                resultentry.update({"subject": participant["subject"]})
                resultentry.update({"filename": chunks_psddata[kk]["filename"]})
                resultentry.update({"studyname": study_name["name"]})
                resultentry.update({"startdate": chunks_psddata[kk]["start"]})
                resultentry.update({"day": chosen_day})
                resultentry.update({"fs": fs})
                resultentry.update({"OVD_percent": OVD_percent})

                Pxx_one = oneminute[:, n[0] : n[0] + n[1]]
                Pyy_one = oneminute[:, n[0] + n[1] : ]                    
                Pxx_one, flag = removeStationaryTones(Pxx_one,fs)
                Pyy_one, flag2 = removeStationaryTones(Pyy_one,fs)
                flag_disturb_tones_removed = flag or flag2
                resultentry.update({"peakaudio_removed": flag_disturb_tones_removed})
                
                nr_of_frames, fft_size = Pxx_one.shape
                
                w,f = aw.get_fftweight_vector((fft_size-1)*2,fs,weighting_func,'lin')
                octav_matrix, f_mid, f_nominal = freqt.get_spectrum_fractionaloctave_transformmatrix((fft_size-1)*2,fs,125,fmax_oktavanalysis,1)
                

                meanPSD_one = (((Pxx_one+Pyy_one)*0.5*fs)*w*w)*magicPSDconvert # this works because of broadcasting rules in python magicPSDconvert magic number to get the correct results
                rms_psd = 10*np.log10(np.mean((meanPSD_one), axis=1)) # mean over frequency
# HERE: Audio Analyses                 
                resultentry.update({"meanPegel": np.mean(rms_psd)})
                resultentry.update({"varPegel": np.var(rms_psd)})
                minval = np.min(rms_psd)
                maxval = np.max(rms_psd)

                perc_one = np.percentile(rms_psd,analyse_percentiles)
                resultentry.update({"minPegel": minval})
                resultentry.update({"maxPegel": maxval})
                for perc_counter, oneperc in enumerate(analyse_percentiles):
                    resultentry.update({f"perc{oneperc}": perc_one[perc_counter]})

                octavPSD_one = (((Pxx_one+Pyy_one)*w*w*magicPSDconvert*fs/((fft_size-1)*2))@octav_matrix) # this works because of broadcasting rules in python
                octaveLevel_one = 10*np.log10(octavPSD_one) # mean over time
                for freqcounter, freq in enumerate(f_nominal):
                    #hist_result, small_result, high_result = get_histogram(10*np.log10(octavPSD[:,freqcounter]),hist_min,hist_max)
                    resultentry.update({f"meanOctav{freq}": np.mean(octaveLevel_one[:,freqcounter])})
                    resultentry.update({f"varOctav{freq}": np.var(octaveLevel_one[:,freqcounter])})
                    minval = np.min(octaveLevel_one[:,freqcounter])
                    maxval = np.max(octaveLevel_one[:,freqcounter])
                    perc_one = np.percentile(octaveLevel_one[:,freqcounter],analyse_percentiles)
                    resultentry.update({f"minPegelOctav{freq}": minval})
                    resultentry.update({f"maxPegelOctav{freq}": maxval})
                    for perc_counter, oneperc in enumerate(analyse_percentiles):
                        resultentry.update({f"percOctav{freq}_{oneperc}": perc_one[perc_counter]})

# only the ownvoice parts
                if (OVD_percent>0 and len(rms_psd) == len(curOVD)):
                    rms_psd_ov = rms_psd[curOVD[:,0] == 1]
                    resultentry.update({"meanPegel_ov": np.mean(rms_psd_ov)})
                    resultentry.update({"varPegel_ov": np.var(rms_psd_ov)})
                    minval = np.min(rms_psd_ov)
                    maxval = np.max(rms_psd_ov)
                    perc_one = np.percentile(rms_psd_ov,analyse_percentiles)
                    resultentry.update({"minPegel_ov": minval})
                    resultentry.update({"maxPegel_ov": maxval})
                    for perc_counter, oneperc in enumerate(analyse_percentiles):
                        resultentry.update({f"perc_ov{oneperc}": perc_one[perc_counter]})

                    octaveLevel_one_ov = octaveLevel_one[curOVD[:,0] == 1,:] # mean over time
                    for freqcounter, freq in enumerate(f_nominal):
                        #hist_result, small_result, high_result = get_histogram(10*np.log10(octavPSD[:,freqcounter]),hist_min,hist_max)
                        resultentry.update({f"meanOctav_ov{freq}": np.mean(octaveLevel_one_ov[:,freqcounter])})
                        resultentry.update({f"varOctav_ov{freq}": np.var(octaveLevel_one_ov[:,freqcounter])})
                        minval = np.min(octaveLevel_one_ov[:,freqcounter])
                        maxval = np.max(octaveLevel_one_ov[:,freqcounter])
                        perc_one = np.percentile(octaveLevel_one_ov[:,freqcounter],analyse_percentiles)
                        resultentry.update({f"minPegelOctav_ov{freq}": minval})
                        resultentry.update({f"maxPegelOctav_ov{freq}": maxval})
                        for perc_counter, oneperc in enumerate(analyse_percentiles):
                            resultentry.update({f"percOctav_ov{freq}_{oneperc}": perc_one[perc_counter]})


                else: # no OV
                    resultentry.update({"meanPegel_ov": -1.0})
                    resultentry.update({"varPegel_ov": -1.0})
                    resultentry.update({"minPegel_ov": -1.0})
                    resultentry.update({"maxPegel_ov": -1.0})
                    for perc_counter, oneperc in enumerate(analyse_percentiles):
                        resultentry.update({f"perc_ov{oneperc}": -1.0})

                    for freqcounter, freq in enumerate(f_nominal):
                        #hist_result, small_result, high_result = get_histogram(10*np.log10(octavPSD[:,freqcounter]),hist_min,hist_max)
                        resultentry.update({f"meanOctav_ov{freq}": -1.0})
                        resultentry.update({f"varOctav_ov{freq}": -1.0})
                        resultentry.update({f"minPegelOctav_ov{freq}": -1.0})
                        resultentry.update({f"maxPegelOctav_ov{freq}": -1.0})
                        for perc_counter, oneperc in enumerate(analyse_percentiles):
                            resultentry.update({f"percOctav_ov{freq}_{oneperc}": -1.0})

# only the without ownvoice parts
                if (OVD_percent<1 and len(rms_psd) == len(curOVD)):
                    rms_psd_notov = rms_psd[curOVD[:,0] != 1]
                    resultentry.update({"meanPegel_notov": np.mean(rms_psd_notov)})
                    resultentry.update({"varPegel_notov": np.var(rms_psd_notov)})
                    minval = np.min(rms_psd_notov)
                    maxval = np.max(rms_psd_notov)
                    perc_one = np.percentile(rms_psd_notov,analyse_percentiles)
                    resultentry.update({"minPegel_notov": minval})
                    resultentry.update({"maxPegel_notov": maxval})
                    for perc_counter, oneperc in enumerate(analyse_percentiles):
                        resultentry.update({f"perc_notov{oneperc}": perc_one[perc_counter]})

                    octaveLevel_one_notov = octaveLevel_one[curOVD[:,0] != 1,:] # mean over time
                    for freqcounter, freq in enumerate(f_nominal):
                        #hist_result, small_result, high_result = get_histogram(10*np.log10(octavPSD[:,freqcounter]),hist_min,hist_max)
                        resultentry.update({f"meanOctav_notov{freq}": np.mean(octaveLevel_one_notov[:,freqcounter])})
                        resultentry.update({f"varOctav_notov{freq}": np.var(octaveLevel_one_notov[:,freqcounter])})
                        minval = np.min(octaveLevel_one_notov[:,freqcounter])
                        maxval = np.max(octaveLevel_one_notov[:,freqcounter])
                        perc_one = np.percentile(octaveLevel_one_notov[:,freqcounter],analyse_percentiles)
                        resultentry.update({f"minPegelOctav_notov{freq}": minval})
                        resultentry.update({f"maxPegelOctav_notov{freq}": maxval})
                        for perc_counter, oneperc in enumerate(analyse_percentiles):
                            resultentry.update({f"percOctav_notov{freq}_{oneperc}": perc_one[perc_counter]})


                else: # no OV
                    resultentry.update({"meanPegel_notov": -1.0})
                    resultentry.update({"varPegel_notov": -1.0})
                    resultentry.update({"minPegel_notov": -1.0})
                    resultentry.update({"maxPegel_notov": -1.0})
                    for perc_counter, oneperc in enumerate(analyse_percentiles):
                        resultentry.update({f"perc_notov{oneperc}": -1.0})

                    for freqcounter, freq in enumerate(f_nominal):
                        #hist_result, small_result, high_result = get_histogram(10*np.log10(octavPSD[:,freqcounter]),hist_min,hist_max)
                        resultentry.update({f"meanOctav_notov{freq}": -1.0})
                        resultentry.update({f"varOctav_notov{freq}": -1.0})
                        resultentry.update({f"minPegelOctav_notov{freq}": -1.0})
                        resultentry.update({f"maxPegelOctav_notov{freq}": -1.0})
                        for perc_counter, oneperc in enumerate(analyse_percentiles):
                            resultentry.update({f"percOctav_notov{freq}_{oneperc}": -1.0})



                results.append(resultentry)


            filename = '.'
            filename = os.path.join(filename, 'results')
            filename = os.path.join(filename, study_name["name"])
            filename = os.path.join(filename, participant["subject"])
            filename = os.path.join(filename, f"{chosen_day}")

            os.makedirs(filename,mode=0o777, exist_ok=True)
            filename = os.path.join(filename, "PSD_all.json")
            with open(filename, 'w') as fout:
                json.dump(results, fout)

        print (days_of_subject)


# download psd and ovd files for one subject and one study

# define windowsize and hopsize (2 minutes and 1 minute)

# compute
# 












# all_questionaires = olMQ.get_all_questionaire_infos(client)
# if end_survey == -1:
#     end_survey = len(all_questionaires)

# for survey_counter in range(start_survey,end_survey):
#     print(f"{survey_counter} / {end_survey}")
#     one_participant = all_questionaires[survey_counter]['subject']
#     print(one_participant)
#     questionaire_id = all_questionaires[survey_counter]['id']
#     print(questionaire_id)
#     survey_filename = all_questionaires[survey_counter]['filename']
#     time_info = olMQ.get_questionaire_fillout_time(client,questionaire_id)
#     if time_info == None:
#         print (survey_filename)
#         continue
#     correction_time = olMQ.get_correction_time_for_survey(client, questionaire_id)
    
#     exist_correction_time = True
#     if (correction_time == -1):
#         print("No correction time: ")
#         exist_correction_time = False
#         correction_time = 0

#     #chunk = get_chunk_at_time(client, one_participant, time_info-timedelta(minutes=correction_time))
#     chunk = olMQ.get_chunk_at_time(client, one_participant, time_info)
#     if len(chunk) == 0:
#         print ("No data file at survey time: ")
#         print (correction_time)
#         print (survey_filename)
#         continue
    
#     chunk_start_time = datetime.strptime(chunk[0]['start'],'%Y-%m-%d %H:%M:%S')

#     chunks = olMQ.get_chunks_for_time_interval(client, one_participant,chunk_start_time-timedelta(minutes=pre_analysis_time_in_min), chunk_start_time)

#     analysis_is_valid = len(chunks) == pre_analysis_time_in_min

#     file_dict = olMQ.get_filedict_for_chunks(client,chunks, 'psd')
#     if len(file_dict) == 0:
#         print("No PSD files in the desired 5 minutes")
#         continue
#     file_dict.sort(key= lambda d: d['filename']) 
#     # client.downloadFiles("./tmp", file_dict, True)
#     file_dictovd = olMQ.get_filedict_for_chunks(client,chunks, 'ovd')
#     if len(file_dictovd) == 0:
#         print("No OVD files in the desired 5 minutes")
#         continue
    
#     file_dictovd.sort(key= lambda d: d['filename']) 

#     psd_data, fs = olMQ.get_data_for_files_in_dict(client,file_dict, keep_files=keep_feature_files)
#     ovd_data, ovd_fs = olMQ.get_data_for_files_in_dict(client,file_dictovd, keep_files=keep_feature_files)

#     # Berechnungen in numpy

#     n = [int(psd_data.shape[1] / 2), int(psd_data.shape[1] / 4)]
#     Pxx = psd_data[:, n[0] : n[0] + n[1]]
#     Pyy = psd_data[:, n[0] + n[1] : ]                    

#     def removeStationaryTones(PSDSpectrum,fs):
#         removed = 0
#         if fs>10000:
#             # look for disturbing tones at high frequencies and remove them
#             analysis_percentile = 90
#             perc_log = 10*np.log10(np.percentile(PSDSpectrum,analysis_percentile,axis = 0)) # percentile over time
#             peaks, peaks_p = sig.find_peaks(perc_log,prominence=5, width=3)
#             if len(peaks)>0:
#                 peaks = peaks[~(peaks<200)] # delete low freq maxima
#                 print(peaks)
#                 peaks_all = []
#                 for p in peaks:
#                     ext_max = np.min([5,512-p])
#                     peaks_ext = p + np.arange(-5,ext_max)
#                     peaks_all.extend(peaks_ext)
            
#                 PSDSpectrum[:,peaks_all] = 0
#                 removed = 1
#         return PSDSpectrum, removed

#     Pxx, flag = removeStationaryTones(Pxx,fs)
#     Pyy, flag2 = removeStationaryTones(Pyy,fs)
#     flag_disturb_tones_removed = flag or flag2
    
#     nr_of_frames, fft_size = Pxx.shape

#     if fs == 8000:
#         magicPSDconvert = 0.4
#         magicPSDconvertCorrect = 24
#     elif fs == 24000:
#         magicPSDconvert = 0.3
#         magicPSDconvertCorrect = 28


#     w,f = aw.get_fftweight_vector((fft_size-1)*2,fs,weighting_func,'lin')
#     meanPSD = (((Pxx+Pyy)*0.5*fs)*w*w)*magicPSDconvert # this works because of broadcasting rules in python magicPSDconvert magic number to get the correct results
        
#     rms_psd = 10*np.log10(np.mean((meanPSD), axis=1)) # mean over frequency
#     octav_matrix, f_mid, f_nominal = freqt.get_spectrum_fractionaloctave_transformmatrix((fft_size-1)*2,fs,125,fmax_oktavanalysis,1)
#     octavLevel = (((Pxx+Pyy)*w*w*magicPSDconvert*fs/((fft_size-1)*2))@octav_matrix) # this works because of broadcasting rules in python
    
#     if fs == 8000: #very magic numbers here
#         stereoPSD = (Pxx+Pyy)*0.5
#     elif fs == 24000:
#         stereoPSD = (Pxx+Pyy)*0.5*0.35
    
#     stereoPSD = np.sqrt(stereoPSD)*np.sqrt(fs/((fft_size-1)*2))
#     stereoPSD = stereoPSD.transpose()
#     if fs >= 32000:
#         stereoPSD_final = stereoPSD
#         freq_vec = np.linspace(0,fs/2, num = int(fft_size))
#     elif fs>=16000 and fs < 32000:
#         stereoPSD_final = np.zeros((2*stereoPSD.shape[0], stereoPSD.shape[1]))
#         stereoPSD_final[0:stereoPSD.shape[0],:] = stereoPSD/np.sqrt(2)
#         freq_vec = np.linspace(0,2*fs/2, num = 2*int(fft_size))
#     elif fs>=8000 and fs < 16000:
#         stereoPSD_final = np.zeros((4*stereoPSD.shape[0], stereoPSD.shape[1]))
#         stereoPSD_final[0:stereoPSD.shape[0],:] = stereoPSD/2
#         freq_vec = np.linspace(0,4*fs/2, num = 4*int(fft_size))
#     else:
#         print("Samplingrate to low")

#     loudness_calibration_db = -89
#     loudness_calibration = 10**(loudness_calibration_db/20)
#     stereoPSD_final *= loudness_calibration        
#     loudness,N_specific,bark_bands = mq.loudness_zwst_freq(stereoPSD_final, freq_vec)
#     #stereoPSD_final = stereoPSD_final.reshape(513,)
#     sharpness = mq.sharpness_din_from_loudness(loudness, N_specific)
    
#     # todo build functions to save this histogram to the dataframe
#     def get_histogram(data, hist_min = None, hist_max = None, nr_of_bins = -1):
#         if hist_min is None:
#             hist_min = np.min(data)
            
#         if hist_max is None:
#             hist_max = np.max(data)
    
#         if nr_of_bins == -1:
#             nr_of_bins = hist_max-hist_min
#             binwidth = 1
#         else:
#             binwidth = (hist_max-hist_min)/nr_of_bins
            
#         smaller_min = len(data[data < hist_min-binwidth*0.5])
#         larger_max = len(data[data > hist_max-binwidth*0.5])
#         result,bin_edges = np.histogram(data, bins = nr_of_bins, range=(hist_min-0.5,hist_max-0.5))
#         return result, smaller_min, larger_max
    
#     hist_result, smallRMS_all, highRMS_all = get_histogram(rms_psd,hist_min,hist_max)
#     rms_psd_premin_all = np.mean(rms_psd)
    
#     octavLevel_mean_all = np.mean(octavLevel, axis=0)
#     octavLevel_var_all = np.var(10*np.log10(octavLevel), axis = 0)
    
#     loudness_mean_all = np.mean(loudness)
#     loudness_var_all = np.var(loudness)
#     sharpness_mean_all = np.mean(sharpness)
#     sharpness_var_all = np.var(sharpness)
    
#     OVD_percent = np.mean(ovd_data)
#     # OV
#     rms_psd_premin_OV = None
#     smallRMS_ov = None
#     highRMS_ov = None
#     hist_result_ov = []
#     octavLevel_mean_ov = None
#     octavLevel_var_ov = None
    
#     if (OVD_percent>0 and len(rms_psd) == len(ovd_data)):
#         rms_psd_ov = rms_psd[ovd_data[:,0] == 1]
#         hist_result_ov, smallRMS_ov, highRMS_ov = get_histogram(rms_psd_ov, hist_min, hist_max)
#         rms_psd_premin_OV = np.mean(rms_psd_ov)

#         octavLevel_ov = octavLevel[ovd_data[:,0] == 1]
#         octavLevel_mean_ov = np.mean(octavLevel_ov, axis = 0)
#         octavLevel_var_ov = np.var(10*np.log10(octavLevel_ov), axis = 0)

#         loudness_ov = loudness[ovd_data[:,0] == 1]
#         sharpness_ov = sharpness[ovd_data[:,0] == 1]
#         loudness_mean_ov = np.mean(loudness_ov)
#         loudness_var_ov = np.var(loudness_ov)
#         sharpness_mean_ov = np.mean(sharpness_ov)
#         sharpness_var_ov = np.var(sharpness_ov)
        

#     # without OV
#     rms_psd_premin_withoutOV = None
#     smallRMS_withouOV = None
#     highRMS_withouOV = None
#     hist_result_withouOV = []
#     octavLevel_mean_notov = None
#     octavLevel_var_notov = None

#     if (OVD_percent<1 and len(rms_psd) == len(ovd_data)):
#         rms_psd_withoutOV = rms_psd[ovd_data[:,0] != 1]
#         hist_result_withouOV, smallRMS_withouOV, highRMS_withouOV = get_histogram(rms_psd_withoutOV, hist_min, hist_max)
#         rms_psd_premin_withoutOV = np.mean(rms_psd_withoutOV)
#         octavLevel_notov = octavLevel[ovd_data[:,0] != 1]
#         octavLevel_mean_notov = np.mean(octavLevel_notov, axis = 0)
#         octavLevel_var_notov = np.var(10*np.log10(octavLevel_notov), axis = 0)
#         loudness_notov = loudness[ovd_data[:,0] != 1]
#         sharpness_notov = sharpness[ovd_data[:,0] != 1]
#         loudness_mean_notov = np.mean(loudness_notov)
#         loudness_var_notov = np.var(loudness_notov)
#         sharpness_mean_notov = np.mean(sharpness_notov)
#         sharpness_var_notov = np.var(sharpness_notov)


#     def writeHistResults(participant, survey, binval, val_all, val_ov, val_notov):
#         dfHist.loc[hist_counter,"subject"] = participant
#         dfHist.loc[hist_counter,"Survey_Filename"] = survey
#         dfHist.loc[hist_counter, f"RMS_{weighting_func}_Value_{pre_analysis_time_in_min}min"] = binval
#         dfHist.loc[hist_counter, f"RMS_{weighting_func}_freq_all_{pre_analysis_time_in_min}min"] = val_all
#         if val_ov is not None:
#             dfHist.loc[hist_counter, f"RMS_{weighting_func}_freq_OV_{pre_analysis_time_in_min}min"] = val_ov
#         if val_notov is not None:
#             dfHist.loc[hist_counter, f"RMS_{weighting_func}_freq_withoutOV_{pre_analysis_time_in_min}min"] = val_notov
        
#     writeHistResults(one_participant,survey_filename, hist_min-1, smallRMS_all, smallRMS_ov, smallRMS_withouOV)

#     hist_counter+=1
#     # all hist value
#     for hist_val in range(hist_min,hist_max):
#         if len(hist_result_ov) != 0:
#             ovhistentry =  hist_result_ov[hist_val-hist_min]
#         else:
#             ovhistentry = None
#         if len(hist_result_withouOV) != 0:
#             notov_histentry =  hist_result_withouOV[hist_val-hist_min]
#         else:
#             notov_histentry = None
        
#         writeHistResults(one_participant,survey_filename, hist_val, hist_result[hist_val-hist_min], ovhistentry, notov_histentry)
        
#         hist_counter+=1
        
#     #higher hist_max
#     writeHistResults(one_participant,survey_filename, hist_max, highRMS_all, highRMS_ov, highRMS_withouOV)

#     hist_counter+=1

    
#     df.loc[survey_counter,"subject"] = one_participant
#     df.loc[survey_counter,"Survey_Filename"] = survey_filename
#     df.loc[survey_counter,"Survey_Starttime"] = time_info
#     df.loc[survey_counter,"Chunk_Starttime"] = chunk_start_time
    
#     if exist_correction_time:
#         df.loc[survey_counter,"Correction_Time"] = correction_time
#     else:
#         df.loc[survey_counter,"Correction_Time"] = -1
    
#     df.loc[survey_counter,"Samplerate"] = fs
#     df.loc[survey_counter,f"is_valid_{pre_analysis_time_in_min}min"] = len(chunks)
#     df.loc[survey_counter,f"OVD_percent_{pre_analysis_time_in_min}min"] = OVD_percent
#     df.loc[survey_counter,f"RMS{weighting_func}_overall_{pre_analysis_time_in_min}min"] = rms_psd_premin_all
#     df.loc[survey_counter,f"RMS{weighting_func}_OV_only_{pre_analysis_time_in_min}min"] = rms_psd_premin_OV
#     df.loc[survey_counter,f"RMS{weighting_func}_without_OV_{pre_analysis_time_in_min}min"] = rms_psd_premin_withoutOV
#     df.loc[survey_counter,f"RMS{weighting_func}_tones_removed{pre_analysis_time_in_min}min"] = flag_disturb_tones_removed

#     for counter, fmid in enumerate(f_nominal):
#         df.loc[survey_counter,f"OctavLevel_mean_all_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = 10*np.log10(octavLevel_mean_all[counter])
#         df.loc[survey_counter,f"OctavLevel_var_all_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = (octavLevel_var_all[counter])
#         if octavLevel_mean_ov is not None:
#             df.loc[survey_counter,f"OctavLevel_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = 10*np.log10(octavLevel_mean_ov[counter])
#             df.loc[survey_counter,f"OctavLevel_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = (octavLevel_var_ov[counter])
#         else:
#             df.loc[survey_counter,f"OctavLevel_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = None    
#             df.loc[survey_counter,f"OctavLevel_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = None
#         if octavLevel_mean_notov is not None:
#             df.loc[survey_counter,f"OctavLevel_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = 10*np.log10(octavLevel_mean_notov[counter])
#             df.loc[survey_counter,f"OctavLevel_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = (octavLevel_var_notov[counter])
#         else:
#             df.loc[survey_counter,f"OctavLevel_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = None
#             df.loc[survey_counter,f"OctavLevel_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min_Band{fmid}"] = None


#     df.loc[survey_counter,f"Loudness_mean_all_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = loudness_mean_all
#     df.loc[survey_counter,f"Loudness_var_all_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = loudness_var_all
#     df.loc[survey_counter,f"Sharpness_mean_all_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = sharpness_mean_all
#     df.loc[survey_counter,f"Sharpness_var_all_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = sharpness_var_all
#     df.loc[survey_counter,f"Loudness_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = loudness_mean_ov
#     df.loc[survey_counter,f"Loudness_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = loudness_var_ov
#     df.loc[survey_counter,f"Sharpness_mean_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = sharpness_mean_ov
#     df.loc[survey_counter,f"Sharpness_var_ov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = sharpness_var_ov
#     df.loc[survey_counter,f"Loudness_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = loudness_mean_notov
#     df.loc[survey_counter,f"Loudness_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = loudness_var_notov
#     df.loc[survey_counter,f"Sharpness_mean_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = sharpness_mean_notov
#     df.loc[survey_counter,f"Sharpness_var_notov_RMS{weighting_func}_{pre_analysis_time_in_min}min"] = sharpness_var_notov

# #print(df.head())

# df.to_csv(result_filename + '.csv')
# dfHist.to_csv(histogram_filename + '.csv')

# import pyreadstat

# pyreadstat.write_sav(df, result_filename+'.sav')
# pyreadstat.write_sav(dfHist, histogram_filename+'.sav')


client.close()

print ("Done...")
pass
