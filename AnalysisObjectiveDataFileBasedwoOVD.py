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
import scipy.fft as sfft

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
        peaks, peaks_p = sig.find_peaks(perc_log,prominence=4, width=3)
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

def get_modulationanalysis(data, analyse_bands = [0, 0.25, 0.5, 1.0, 2.0, 4.0]):
    mod_spec = np.fft.fft((data-np.mean(data))/np.std(data))
    pos_freqs = int(len(mod_spec)/2+1)
#freq_vec = np.linspace(0,4,num=pos_freqs)
    mod_freq_bands_index = np.array(analyse_bands)/4.0 * pos_freqs
    energ = []
    for bands in range(len(mod_freq_bands_index)-1):
        en = np.sum(np.abs(mod_spec[int(mod_freq_bands_index[bands]):int(mod_freq_bands_index[bands+1])]))
        energ.append(en)
    return np.abs(mod_spec[:pos_freqs]), energ


client = olMEGA_DataService_Client.client(debug = True)
weighting_func = 'z'
keep_feature_files = True
fmax_oktavanalysis = 4000

#all_participants = olMQ.get_all_participants(client)
all_studies = olMQ.get_all_studynames(client)

#study_name = 'EMA2'
for study_count, study_name in enumerate(all_studies):
    if (study_count < 3):
        continue
    
    print (study_name)
    #participants_EMA1 = olMQ.get_all_participants_for_study(client, study_name)
    participants_EMA = olMQ.get_all_participants_for_study(client, study_name["name"])

    for participant_count, participant in enumerate(participants_EMA):
        #if (participant_count<2 and study_count<1):
        #    continue

        print(participant)
        data = olMQ.get_alltimes_objective_data_for_one_subject_one_study(client, participant["subject"], study_name["name"])
        print(len(data))
        if (len(data) == 0):
            continue


        days_of_subject = get_unique_days_from_starttime_list(data)
        for chosen_day, oneday in enumerate(days_of_subject):
            print(chosen_day)
#            if chosen_day > 1:
#                continue
            chunks_psddata = olMQ.get_chunks_and_filenames_for_time_interval(client, participant["subject"],oneday, oneday + timedelta(hours=24),'psd')
            chunks_psddata.sort(key= lambda d: d['filename']) 

#            chunks_ovddata = olMQ.get_chunks_and_filenames_for_time_interval(client, participant["subject"],oneday, oneday + timedelta(hours=24),'ovd')
#            chunks_ovddata.sort(key= lambda d: d['filename']) 
            #ovd_data, ovd_data_stacked, ovd_fs = olMQ.get_data_for_files_in_dict(client,chunks_ovddata, keep_files=keep_feature_files)
            print('chunks da')
            results = []

            for chunk_counter, onechunk in enumerate (chunks_psddata):
                print(onechunk["filename"])
                help = []
                help.append(onechunk)
                psd_data, fs = olMQ.get_data_for_files_in_onedict(client,help, keep_files=keep_feature_files)

                #psd_data, psd_data_stacked, fs = olMQ.load_data_for_files_in_dict(chunks_psddata)
                #ovd_data, ovd_data_stacked, ovd_fs = olMQ.load_data_for_files_in_dict(chunks_ovddata)

                n = [int(psd_data.shape[1] / 2), int(psd_data.shape[1] / 4)]
            
                magicPSDconvert = 1
                if fs == 8000: #legacy versions
                    magicPSDconvert = 0.4
                    magicPSDconvertCorrect = 24
                elif fs == 24000:
                    magicPSDconvert = 0.3
                    magicPSDconvertCorrect = 28



                analyse_percentiles = [5, 30, 65, 95, 99]
                analyse_modbands = [0, 0.25, 0.5, 1.0, 2.0, 4.0]

            
                resultentry = {}

                curOVD = []
                OVD_percent = -1.0

                resultentry.update({"subject": participant["subject"]})
                resultentry.update({"filename": onechunk["filename"]})
                resultentry.update({"studyname": study_name["name"]})
                resultentry.update({"startdate": onechunk["start"]})
                resultentry.update({"day": chosen_day})
                resultentry.update({"fs": fs})
                resultentry.update({"OVD_percent": OVD_percent})

                Pxx_one = psd_data[:, n[0] : n[0] + n[1]]
                Pyy_one = psd_data[:, n[0] + n[1] : ]      
                #fig, ax = plt.subplots()
                #ax.plot(10*np.log10(np.mean(Pxx_one,0)))              
                Pxx_one, flag = removeStationaryTones(Pxx_one,fs)
                #ax.plot(10*np.log10(np.mean(Pxx_one,0)))
                #if (flag):
                #    plt.show()

                Pyy_one, flag2 = removeStationaryTones(Pyy_one,fs)
                flag_disturb_tones_removed = flag or flag2
                resultentry.update({"peakaudio_removed": flag_disturb_tones_removed})
                
                nr_of_frames, fft_size = Pxx_one.shape
                
                w,f = aw.get_fftweight_vector((fft_size-1)*2,fs,weighting_func,'lin')
                wa,f = aw.get_fftweight_vector((fft_size-1)*2,fs,'a','lin')
                octav_matrix, f_mid, f_nominal = freqt.get_spectrum_fractionaloctave_transformmatrix((fft_size-1)*2,fs,125,fmax_oktavanalysis,1)
                

                meanPSD_one = (((Pxx_one+Pyy_one)*0.5*fs)*wa*wa)*magicPSDconvert # this works because of broadcasting rules in python magicPSDconvert magic number to get the correct results
                rms_lin = np.mean((meanPSD_one), axis=1)
                rms_psd = 10*np.log10(rms_lin) # mean over frequency
# HERE: Audio Analyses                 


                resultentry.update({"meanPegel": 10*np.log10(np.mean(rms_lin)+ np.finfo(float).eps)})
                resultentry.update({"varPegel": 10*np.log10(np.var(rms_lin) + np.finfo(float).eps)})
                minval = np.min(rms_psd)
                maxval = np.max(rms_psd)

                perc_one = np.percentile(rms_psd,analyse_percentiles)
                resultentry.update({"minPegel": minval})
                resultentry.update({"maxPegel": maxval})
                for perc_counter, oneperc in enumerate(analyse_percentiles):
                    resultentry.update({f"perc{oneperc}": perc_one[perc_counter]})

                modspec, energ = get_modulationanalysis(rms_lin,analyse_modbands)
                for modcounter in range(len(energ)):
                    resultentry.update({f'modBand_{analyse_modbands[modcounter]}_{analyse_modbands[modcounter+1]}_full': energ[modcounter]})
                    

                octavPSD_one = (((Pxx_one+Pyy_one)*w*w*magicPSDconvert*fs/((fft_size-1)*2))@octav_matrix) # this works because of broadcasting rules in python
                octaveLevel_one = 10*np.log10(octavPSD_one) # mean over time

                for freqcounter, freq in enumerate(f_nominal):
                    modspec, energ = get_modulationanalysis(octavPSD_one[:,freqcounter],analyse_modbands)
                    #hist_result, small_result, high_result = get_histogram(10*np.log10(octavPSD[:,freqcounter]),hist_min,hist_max)
                    resultentry.update({f"meanOctav{freq}": 10*np.log10(np.mean(octavPSD_one[:,freqcounter])+ np.finfo(float).eps)})
                    resultentry.update({f"varOctav{freq}": 10*np.log10(np.var(octavPSD_one[:,freqcounter]) + np.finfo(float).eps)})
                    minval = np.min(octaveLevel_one[:,freqcounter])
                    maxval = np.max(octaveLevel_one[:,freqcounter])
                    perc_one = np.percentile(octaveLevel_one[:,freqcounter],analyse_percentiles)
                    resultentry.update({f"minPegelOctav{freq}": minval})
                    resultentry.update({f"maxPegelOctav{freq}": maxval})
                    for perc_counter, oneperc in enumerate(analyse_percentiles):
                        resultentry.update({f"percOctav{freq}_{oneperc}": perc_one[perc_counter]})

                    for modcounter in range(len(energ)):
                        resultentry.update({f'modBand_{analyse_modbands[modcounter]}_{analyse_modbands[modcounter+1]}_Octav{freq}': energ[modcounter]})


# only the ownvoice parts
                if (OVD_percent>0 and len(rms_psd) == len(curOVD)):
                    rms_psd_ov = rms_psd[curOVD[:,0] == 1]
                    rms_lin_ov = rms_lin[curOVD[:,0] == 1]
                    resultentry.update({"meanPegel_ov": 10*np.log10(np.mean(rms_lin_ov)+ np.finfo(float).eps)})
                    resultentry.update({"varPegel_ov": 10*np.log10(np.var(rms_lin_ov) + np.finfo(float).eps)})
                    minval = np.min(rms_psd_ov)
                    maxval = np.max(rms_psd_ov)
                    perc_one = np.percentile(rms_psd_ov,analyse_percentiles)
                    resultentry.update({"minPegel_ov": minval})
                    resultentry.update({"maxPegel_ov": maxval})
                    for perc_counter, oneperc in enumerate(analyse_percentiles):
                        resultentry.update({f"perc_ov{oneperc}": perc_one[perc_counter]})

                    octaveLevel_one_ov = octaveLevel_one[curOVD[:,0] == 1,:] # mean over time
                    octavPSD_one_ov = octavPSD_one[curOVD[:,0] == 1,:] # mean over time
                    for freqcounter, freq in enumerate(f_nominal):
                        #hist_result, small_result, high_result = get_histogram(10*np.log10(octavPSD[:,freqcounter]),hist_min,hist_max)
                        resultentry.update({f"meanOctav_ov{freq}": 10*np.log10(np.mean(octavPSD_one_ov[:,freqcounter])+ np.finfo(float).eps)})
                        resultentry.update({f"varOctav_ov{freq}": 10*np.log10(np.var(octavPSD_one_ov[:,freqcounter]) + np.finfo(float).eps)})
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
                    rms_lin_notov = rms_lin[curOVD[:,0] != 1]
                    resultentry.update({"meanPegel_notov": 10*np.log10(np.mean(rms_lin_notov)+ np.finfo(float).eps)})
                    resultentry.update({"varPegel_notov": 10*np.log10(np.var(rms_lin_notov) + np.finfo(float).eps)})
                    minval = np.min(rms_psd_notov)
                    maxval = np.max(rms_psd_notov)
                    perc_one = np.percentile(rms_psd_notov,analyse_percentiles)
                    resultentry.update({"minPegel_notov": minval})
                    resultentry.update({"maxPegel_notov": maxval})
                    for perc_counter, oneperc in enumerate(analyse_percentiles):
                        resultentry.update({f"perc_notov{oneperc}": perc_one[perc_counter]})

                    octaveLevel_one_notov = octaveLevel_one[curOVD[:,0] != 1,:] # mean over time
                    octavPSD_one_notov = octavPSD_one[curOVD[:,0] != 1,:] # mean over time
                    for freqcounter, freq in enumerate(f_nominal):
                        #hist_result, small_result, high_result = get_histogram(10*np.log10(octavPSD[:,freqcounter]),hist_min,hist_max)
                        resultentry.update({f"meanOctav_notov{freq}": 10*np.log10(np.mean(octavPSD_one_notov[:,freqcounter])+ np.finfo(float).eps)})
                        resultentry.update({f"varOctav_notov{freq}": 10*np.log10(np.var(octavPSD_one_notov[:,freqcounter]) + np.finfo(float).eps)})
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



client.close()

print ("Done...")


#                fig, ax = plt.subplots(nrows=2)
#                ax[0].plot(octavPSD_one[:,0])
                
                
#                analyse_modbands = [0, 0.25, 0.5, 1.0, 2.0, 4.0]
#                modspec, energ = get_modulationanalysis(octavPSD_one[:,0],analyse_modbands)
#                ax[1].plot(modspec)
#                print(energ)
