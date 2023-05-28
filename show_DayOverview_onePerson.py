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


def find_file_recursiv(start_dir, seach_string):
    pys = []
    for p, d, f in os.walk(start_dir):
        for file in f:
            if file.endswith(seach_string):
                pys.append(os.path.join(p,file))
    return pys

def get_continous_chunk_data_and_time(one_day, extract_param, min_len = 5):
#print (one_day_data)

    extracted_data_oneday = one_day[extract_param].to_numpy()
    starttime_list = one_day["startdate"].to_list()
    starttime_vec = []
    deltatime_vec = []
    lastTime = datetime(year=2000, month=1,day=1, hour=0, minute=0,second=0)
    for kk, onetimestring in enumerate(starttime_list):
        dt = datetime.strptime(onetimestring,'%Y-%m-%d %H:%M:%S')
        dt = dt.replace(year = 2000, month = 1, day = 1)
        starttime_vec.append(dt)
        deltatime = dt - lastTime
        lastTime = dt
        deltatime_vec.append(int(deltatime.total_seconds()))
    

    delta = np.array(deltatime_vec)
    index = np.where(delta > min_len*60)[0] # *60 da delta in seconds
    index = np.append(index, len(starttime_vec))
    if (len(index) > 0):
        oldindex = 0
        cont_chunks_data = []
        cont_chunks_time = []
        for counter, index_nr in enumerate(index):
            if index_nr - oldindex < min_len:
                oldindex = index_nr    
                continue
            else:
                startindex = oldindex+1
                endindex = index_nr-1
                data = extracted_data_oneday[startindex:endindex]
                timevec = starttime_vec[startindex:endindex]
                cont_chunks_data.append(data)
                cont_chunks_time.append(timevec)
                oldindex = index_nr 

    return cont_chunks_data, cont_chunks_time

def find_robust_minmax(data):
    robust_min_maxval = np.nanpercentile(data[np.isfinite(data)],[2, 99])
    #if (np.isneginf(robust_min_maxval[0])):
    #    robust_min_maxval[0] = data(np.is)
    return robust_min_maxval


startdir = './results'
#study = 'EMA2'
#variable = "varPegel"
#variable = "OVD_percent"

startdir = os.path.join(startdir)

list_of_resultfiles = find_file_recursiv(startdir,'.json')
all_df_list = []

#for file_counter, onefilename in enumerate(list_of_resultfiles):
for file_counter, onefilename in enumerate(list_of_resultfiles):
    with open(onefilename, 'r') as fout:
        data = json.load(fout)

    #onedict = data[0]

    df = pd.DataFrame(data, index=list(range(len(data))))
    all_df_list.append(df)

all_df = pd.concat(all_df_list)
allkeys = data[0].keys()
study_name = 'EMA2'

subject = 'xxxxxx'
starttime=datetime(2000, 1, 1, 6, 0), # the one that doesn't change
stoptime=datetime(2000, 1, 1, 23, 59) # the latest datetime in your dataset


# filtern nach subject und study

# iterieren ueber Tage

# plot spektrogram (1 min), minimum (5% percentil), mean, maximum (95%)

# 200 Parameter (wichtig ist OVS)



