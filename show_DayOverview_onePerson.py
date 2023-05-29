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
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
from mpl_toolkits.axes_grid1 import make_axes_locatable        

import json
def adjust_lightness(color, amount=0.5):
    import matplotlib.colors as mc
    import colorsys
    try:
        c = mc.cnames[color]
    except:
        c = color
    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], max(0, min(1, amount * c[1])), c[2])

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

def get_continous_chunk_data_and_time_and_OVS(one_day, extract_param, min_len = 5):
#print (one_day_data)

    extracted_data_oneday = one_day[extract_param].to_numpy()
    extracted_OVS_oneday = one_day["OVD_percent"].to_numpy()
    starttime_list = one_day["startdate"].to_list()
    starttime_vec = []
    deltatime_vec = []
    ovs_vec = []
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
        cont_chunks_ovs = []
        for counter, index_nr in enumerate(index):
            if index_nr - oldindex < min_len:
                oldindex = index_nr    
                continue
            else:
                startindex = oldindex+1
                endindex = index_nr-1
                data = extracted_data_oneday[startindex:endindex]
                ovs = extracted_OVS_oneday[startindex:endindex]
                timevec = starttime_vec[startindex:endindex]
                cont_chunks_data.append(data)
                cont_chunks_time.append(timevec)
                cont_chunks_ovs.append(ovs)
                oldindex = index_nr 

    return cont_chunks_data, cont_chunks_time, cont_chunks_ovs


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
starttime=datetime(2000, 1, 1, 6, 0), # the one that doesn't change
stoptime=datetime(2000, 1, 1, 23, 59) # the latest datetime in your dataset


study_name = 'EMA1'

onesubject = 'EK06DI26'

studies = all_df["studyname"].unique()
subjects = all_df["subject"].unique()

for study_counter, study_name in enumerate(studies):
    for subject_counter, onesubject in enumerate(subjects):

        # filtern nach subject und study
        data_onesubject = all_df.query(f"subject=='{onesubject}'")
        data_onesubject_onestudy = data_onesubject.query(f"studyname=='{study_name}'")

        # iterieren ueber Tage
        days = data_onesubject_onestudy["day"].unique()
        lightning = 0.7
        pdf_filename = f'{onesubject}_{study_name}_EachDay.pdf'
        #text = f"Study = {study}, All Subjects (different colors), {variable}, all days (different brightness per color) "
        outpdf = PdfPages(pdf_filename)

        for day_counter in range(len(days)):
            one_day_data =  data_onesubject_onestudy.query(f"day=={day_counter}")
            
            # day spectrogram
            variable = "Mean_spectrum"
            cont_chunks_data, cont_chunks_time, cont_chunks_ovs =  get_continous_chunk_data_and_time_and_OVS(one_day_data,variable)
            if (len (cont_chunks_data) == 0):
                continue
            fs = one_day_data["fs"].to_list()[0]
            
            x_lims = mdates.date2num([datetime(2000, 1, 1, 6, 0),datetime(2000, 1, 1, 23, 59)])
            x_time_axis = [datetime(2000, 1, 1, 6, 0) + timedelta(minutes=x)
                            for x in range(18*60)]
            
            #all_dates = np.stack(x_time_axis, axis=0).reshape((len(x_time_axis)))
            all_dates = np.array(x_time_axis)
            fft_size = len(cont_chunks_data[0][0])
            plot_matrix = np.ones((fft_size, len(x_time_axis)))*(-100)
            for chunk_counter, chunk_data in enumerate(cont_chunks_data):
                start_time = cont_chunks_time[chunk_counter][0]
                end_time = cont_chunks_time[chunk_counter][-1]
                idx = np.where(np.logical_and((all_dates>start_time), (all_dates<end_time)))
                if (len(idx[0]) <= 2):
                    continue

                start_pixel = idx[0][0]
                allpsd = np.stack(chunk_data, axis=0)

                end_pixel = start_pixel + len(chunk_data)
                if (end_pixel>=len(x_time_axis)):
                    continue
                plot_matrix[:,start_pixel:end_pixel] = allpsd.transpose()

            fig, ax = plt.subplots()
            freq_vec = np.linspace(start=0, stop=fs/2, num = fft_size)
                #hax = ax[0].pcolormesh(all_dates[idx2], freq_vec, 10*np.log10(alldata_psd[idx2,:].transpose()), vmin=20, vmax = 100)
                
            hax = ax.imshow(plot_matrix,extent =[x_lims[0],x_lims[1], 0, fs/2], vmin=30, vmax = 90, aspect = 'auto', origin = 'lower')
            ax.xaxis_date()
            #divider = make_axes_locatable(ax)
            #cax = divider.append_axes("right", size="5%", pad=0.05)

            #plt.colorbar(hax, cax=ax)        
                #plt.axis((None, None, 0, 200))
            ax.set_ylabel('Frequency [Hz]')
                #plt.xlabel('Time [sec]')
                #plt.show()
                #ax.plot(10*np.log10(np.mean(meanPSD_one, 0)))
            xfmt = mdates.DateFormatter('%H:%M')
            ax.xaxis.set_major_formatter(xfmt)
            ax.tick_params(axis='x', rotation=45)

            titletext = f'{onesubject}_{study_name}'
            ax.set_title(titletext)

            outpdf.savefig(fig) # ohne () saves the current figure
            #plt.show()

            # OVS percentile and pegel
            variable = 'meanPegel'
            cont_chunks_data, cont_chunks_time, cont_chunks_ovs =  get_continous_chunk_data_and_time_and_OVS(one_day_data,variable)

            fig, ax = plt.subplots(nrows = 2)
            for chunk_counter, chunk_data in enumerate(cont_chunks_data):
                ax[0].plot(cont_chunks_time[chunk_counter], cont_chunks_ovs[chunk_counter],  color = 'b')
                ax[1].plot(cont_chunks_time[chunk_counter], chunk_data,  color = 'b')
            
            global_data = data_onesubject_onestudy[variable].to_numpy()
            
            robust_min_maxval = find_robust_minmax(global_data)
            
            ax[0].set_xlim(
                xmin=datetime(2000, 1, 1, 6, 0), # the one that doesn't change
                xmax=datetime(2000, 1, 1, 23, 59) # the latest datetime in your dataset
            )
            ax[0].set_xticks([])
            ax[0].set_ylim(
                ymin= 0,
                ymax= 1)

            ax[1].set_xlim(
                xmin=datetime(2000, 1, 1, 6, 0), # the one that doesn't change
                xmax=datetime(2000, 1, 1, 23, 59) # the latest datetime in your dataset
            )
            titletext = f'{onesubject}_{study_name}_OVS[%]'
            ax[0].set_title(titletext)
            ax[1].set_ylim(
                ymin= np.round(30),
                ymax= np.round(100)
            )
            titletext = f'{onesubject}_{variable}_{study_name}'
            ax[1].set_title(titletext)
            ax[1].xaxis.set_major_formatter(xfmt)
            ax[1].tick_params(axis='x', rotation=45)

            outpdf.savefig(fig) # ohne () saves the current figure
            #plt.show()
            # percentile curves [all in one plot]
            variable = 'perc5'
            cont_chunks_data5, cont_chunks_time, cont_chunks_ovs =  get_continous_chunk_data_and_time_and_OVS(one_day_data,variable)
            variable = 'perc30'
            cont_chunks_data30, cont_chunks_time, cont_chunks_ovs =  get_continous_chunk_data_and_time_and_OVS(one_day_data,variable)
            variable = 'perc65'
            cont_chunks_data65, cont_chunks_time, cont_chunks_ovs =  get_continous_chunk_data_and_time_and_OVS(one_day_data,variable)
            variable = 'perc95'
            cont_chunks_data95, cont_chunks_time, cont_chunks_ovs =  get_continous_chunk_data_and_time_and_OVS(one_day_data,variable)

            fig, ax = plt.subplots()
            for chunk_counter, chunk_data in enumerate(cont_chunks_data5):
                ax.plot(cont_chunks_time[chunk_counter], chunk_data,  color = 'b', linewidth=0.5)
                ax.plot(cont_chunks_time[chunk_counter], cont_chunks_data30[chunk_counter],  color = adjust_lightness('b',0.7), linewidth=0.5)
                ax.plot(cont_chunks_time[chunk_counter], cont_chunks_data65[chunk_counter],  color = adjust_lightness('r',0.7), linewidth=0.5)
                ax.plot(cont_chunks_time[chunk_counter], cont_chunks_data95[chunk_counter],  color = 'r', linewidth=0.5)

            ax.set_xlim(
                xmin=datetime(2000, 1, 1, 6, 0), # the one that doesn't change
                xmax=datetime(2000, 1, 1, 23, 59) # the latest datetime in your dataset
            )
            ax.set_ylim(
                ymin= np.round(30),
                ymax= np.round(100)
            )
            titletext = f'{onesubject}_Percentile_{study_name}'
            ax.set_title(titletext)
            ax.xaxis.set_major_formatter(xfmt)
            ax.tick_params(axis='x', rotation=45)

            outpdf.savefig(fig) # ohne () saves the current figure


        outpdf.close()


# plot spektrogram (1 min), minimum (5% percentil), mean, maximum (95%)

# 200 Parameter (wichtig ist OVS)



