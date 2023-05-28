"""
script and function to convert json dump files of objective data to pandas frames
(c) J.Bitzer@Jade-hs
License: BSD 3-clause
Version 1.0.0 JB 12.05.2023

dict_keys(['subject', 'filename', 'studyname', 'startdate', 'day', 'fs', 'OVD_percent', 'peakaudio_removed', 'meanPegel', 'varPegel', 'minPegel', 'maxPegel', 'perc5', 'perc30', 'perc95', 'perc99', 'meanOctav125', 'varOctav125', 'minPegelOctav125', 'maxPegelOctav125', 'percOctav125_5', 'percOctav125_30', 'percOctav125_95', 'percOctav125_99', 'meanOctav250', 'varOctav250', 'minPegelOctav250', 'maxPegelOctav250', 'percOctav250_5', 'percOctav250_30', 'percOctav250_95', 'percOctav250_99', 'meanOctav500', 'varOctav500', 'minPegelOctav500', 'maxPegelOctav500', 'percOctav500_5', 'percOctav500_30', 'percOctav500_95', 'percOctav500_99', 'meanOctav1000', 'varOctav1000', 'minPegelOctav1000', 'maxPegelOctav1000', 'percOctav1000_5', 'percOctav1000_30', 'percOctav1000_95', 'percOctav1000_99', 'meanOctav2000', 'varOctav2000', 'minPegelOctav2000', 'maxPegelOctav2000', 'percOctav2000_5', 'percOctav2000_30', 'percOctav2000_95', 'percOctav2000_99', 'meanOctav4000', 'varOctav4000', 'minPegelOctav4000', 'maxPegelOctav4000', 'percOctav4000_5', 'percOctav4000_30', 'percOctav4000_95', 'percOctav4000_99', 'meanPegel_ov', 'varPegel_ov', 'minPegel_ov', 'maxPegel_ov', 'perc_ov5', 'perc_ov30', 'perc_ov95', 'perc_ov99', 'meanOctav_ov125', 'varOctav_ov125', 'minPegelOctav_ov125', 'maxPegelOctav_ov125', 'percOctav_ov125_5', 'percOctav_ov125_30', 'percOctav_ov125_95', 'percOctav_ov125_99', 'meanOctav_ov250', 'varOctav_ov250', 'minPegelOctav_ov250', 'maxPegelOctav_ov250', 'percOctav_ov250_5', 'percOctav_ov250_30', 'percOctav_ov250_95', 'percOctav_ov250_99', 'meanOctav_ov500', 'varOctav_ov500', 'minPegelOctav_ov500', 'maxPegelOctav_ov500', 'percOctav_ov500_5', 'percOctav_ov500_30', 'percOctav_ov500_95', 'percOctav_ov500_99', 'meanOctav_ov1000', 'varOctav_ov1000', 'minPegelOctav_ov1000', 'maxPegelOctav_ov1000', 'percOctav_ov1000_5', 'percOctav_ov1000_30', 'percOctav_ov1000_95', 'percOctav_ov1000_99', 'meanOctav_ov2000', 'varOctav_ov2000', 'minPegelOctav_ov2000', 'maxPegelOctav_ov2000', 'percOctav_ov2000_5', 'percOctav_ov2000_30', 'percOctav_ov2000_95', 'percOctav_ov2000_99', 'meanOctav_ov4000', 'varOctav_ov4000', 'minPegelOctav_ov4000', 'maxPegelOctav_ov4000', 'percOctav_ov4000_5', 'percOctav_ov4000_30', 'percOctav_ov4000_95', 'percOctav_ov4000_99', 'meanPegel_notov', 'varPegel_notov', 'minPegel_notov', 'maxPegel_notov', 'perc_notov5', 'perc_notov30', 'perc_notov95', 'perc_notov99', 'meanOctav_notov125', 'varOctav_notov125', 'minPegelOctav_notov125', 'maxPegelOctav_notov125', 'percOctav_notov125_5', 'percOctav_notov125_30', 'percOctav_notov125_95', 'percOctav_notov125_99', 'meanOctav_notov250', 'varOctav_notov250', 'minPegelOctav_notov250', 'maxPegelOctav_notov250', 'percOctav_notov250_5', 'percOctav_notov250_30', 'percOctav_notov250_95', 'percOctav_notov250_99', 'meanOctav_notov500', 'varOctav_notov500', 'minPegelOctav_notov500', 'maxPegelOctav_notov500', 'percOctav_notov500_5', 'percOctav_notov500_30', 'percOctav_notov500_95', 'percOctav_notov500_99', 'meanOctav_notov1000', 'varOctav_notov1000', 'minPegelOctav_notov1000', 'maxPegelOctav_notov1000', 'percOctav_notov1000_5', 'percOctav_notov1000_30', 'percOctav_notov1000_95', 'percOctav_notov1000_99', 'meanOctav_notov2000', 'varOctav_notov2000', 'minPegelOctav_notov2000', 'maxPegelOctav_notov2000', 'percOctav_notov2000_5', 'percOctav_notov2000_30', 'percOctav_notov2000_95', 'percOctav_notov2000_99', 'meanOctav_notov4000', 'varOctav_notov4000', 'minPegelOctav_notov4000', 'maxPegelOctav_notov4000', 'percOctav_notov4000_5', 'percOctav_notov4000_30', 'percOctav_notov4000_95', 'percOctav_notov4000_99'])
"""
#from gettext import npgettext
#from time import strptime
#from olMEGA_DataService_Client import olMEGA_DataService_Client
#from datetime import datetime, timedelta
#import random
#from operator import itemgetter
#from olMEGA_DataService_Tools import FeatureFile
#from olMEGA_DataService_Tools import acousticweighting as aw
#from olMEGA_DataService_Tools import freq2freqtransforms as freqt
#import olMEGA_DataService_Client.olMegaQueries as olMQ

#import mosqito as mq

import numpy as np
import pandas as pd
import os
import scipy.signal as sig
import matplotlib.pyplot as plt
import json
from datetime import datetime, timedelta

from time import strptime
import matplotlib.dates as md
from matplotlib.backends.backend_pdf import PdfPages

xfmt = md.DateFormatter('%H:%M')

def adjust_lightness(color, amount=0.5):
    import matplotlib.colors as mc
    import colorsys
    try:
        c = mc.cnames[color]
    except:
        c = color
    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], max(0, min(1, amount * c[1])), c[2])

def find_file_recursiv(start_dir, seach_string):
    pys = []
    for p, d, f in os.walk(start_dir):
        for file in f:
            if file.endswith(seach_string):
                pys.append(os.path.join(p,file))
    return pys

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
#variable = "perc5"
#variable2 = "perc95"
variable = "minPegel"
variable2 = "maxPegel"

#startdir = os.path.join(startdir,study)

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
#global_data = all_df[variable].to_numpy()

#robust_min_maxval = np.percentile(global_data,[2, 99])


subjects = all_df["subject"].unique()

#newcolor= ['r', 'b']

pdf_filename = f'{variable2}_-_{variable}_vs_OVS_EachSubjects.pdf'
#text = f"Study = {study}, All Subjects (different colors), {variable}, all days (different brightness per color) "
outpdf = PdfPages(pdf_filename)
NUM_COLORS = 9 # adjust to max days
cm = plt.get_cmap('Accent')
newcolor=[cm(1.*i/(NUM_COLORS-1)) for i in range(NUM_COLORS)]

for subject_counter, onesubject in enumerate(subjects):
    color_counter = 0
    fig, ax = plt.subplots(nrows=4)
    fig.set_size_inches(w=8, h=14)
    data_onesubject = all_df.query(f"subject=='{onesubject}'")
    studies = data_onesubject["studyname"].unique()
    data_onesubject_onestudy = data_onesubject.query(f"studyname=='EMA1'")
    days = data_onesubject_onestudy["day"].unique()

    for day_counter in range(len(days)):
        one_day_data =  data_onesubject_onestudy.query(f"day=={day_counter}")
    
        cont_chunks_data, cont_chunks_time, cont_chunks_ovs =  get_continous_chunk_data_and_time_and_OVS(one_day_data,variable)
        cont_chunks_data2, cont_chunks_time2, cont_chunks_ovs2 =  get_continous_chunk_data_and_time_and_OVS(one_day_data,variable2)

        for chunk_counter, chunk_data in enumerate(cont_chunks_data):
            ax[0].plot(cont_chunks_time[chunk_counter], cont_chunks_ovs[chunk_counter],  color = newcolor[color_counter], alpha = 0.5)
            ax[1].plot(cont_chunks_time[chunk_counter], cont_chunks_data2[chunk_counter]-chunk_data,  color = newcolor[color_counter], alpha = 0.5)
        color_counter += 1


    global_data = data_onesubject_onestudy[variable].to_numpy()
    global_data2 = data_onesubject_onestudy[variable2].to_numpy()
    final_data = global_data2-global_data
    robust_min_maxval = find_robust_minmax(final_data)
    
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
    titletext = f'{onesubject}_OVS[%]_EMA1'
    ax[0].set_title(titletext)
    ax[1].set_ylim(
        ymin= np.round(0.95*robust_min_maxval[0]),
        ymax= np.round(1.05*robust_min_maxval[1])
    )
    titletext = f'{onesubject}_{variable2}-{variable}'
    ax[1].set_title(titletext)
    ax[1].xaxis.set_major_formatter(xfmt)
    ax[1].tick_params(axis='x', rotation=45)
    if (len(studies) < 2):
        outpdf.savefig(fig) # ohne () saves the current figure
        continue

    color_counter = 0
    data_onesubject_onestudy = data_onesubject.query(f"studyname=='EMA2'")
    days = data_onesubject_onestudy["day"].unique()

    for day_counter in range(len(days)):
        one_day_data =  data_onesubject_onestudy.query(f"day=={day_counter}")
    
        cont_chunks_data, cont_chunks_time, cont_chunks_ovs =  get_continous_chunk_data_and_time_and_OVS(one_day_data,variable)
        cont_chunks_data2, cont_chunks_time2, cont_chunks_ovs2 =  get_continous_chunk_data_and_time_and_OVS(one_day_data,variable2)

        for chunk_counter, chunk_data in enumerate(cont_chunks_data):
            ax[2].plot(cont_chunks_time[chunk_counter], cont_chunks_ovs[chunk_counter],  color = newcolor[color_counter], alpha = 0.5)
            ax[3].plot(cont_chunks_time[chunk_counter], cont_chunks_data2[chunk_counter]-chunk_data,  color = newcolor[color_counter], alpha = 0.5)
        color_counter += 1

    global_data = data_onesubject_onestudy[variable].to_numpy()
    global_data2 = data_onesubject_onestudy[variable2].to_numpy()

    final_data = global_data2-global_data
    robust_min_maxval = find_robust_minmax(final_data)
    ax[2].set_xlim(
        xmin=datetime(2000, 1, 1, 6, 0), # the one that doesn't change
        xmax=datetime(2000, 1, 1, 23, 59) # the latest datetime in your dataset
    )
    ax[2].set_xticks([])
    ax[2].set_ylim(
        ymin= 0,
        ymax= 1)

    ax[3].set_xlim(
        xmin=datetime(2000, 1, 1, 6, 0), # the one that doesn't change
        xmax=datetime(2000, 1, 1, 23, 59) # the latest datetime in your dataset
    )
    titletext = f'{onesubject}_OVS[%]_EMA2'
    ax[2].set_title(titletext)
    ax[3].set_ylim(
        ymin= np.round(0.95*robust_min_maxval[0]),
        ymax= np.round(1.05*robust_min_maxval[1])
    )
    titletext = f'{onesubject}_{variable2}-{variable}'
    ax[3].set_title(titletext)
    ax[3].xaxis.set_major_formatter(xfmt)
    ax[3].tick_params(axis='x', rotation=45)
    

    #outpdf.attach_note(text, positionRect=[0, 0, 20, 20])
    outpdf.savefig(fig) # ohne () saves the current figure

outpdf.close()

#fs = all_df.loc[0]["fs"]
#print(fs)

#dynamics = df["perc95"].to_numpy() - df["perc5"].to_numpy()

#ax[0].set_xticks([])

#pdf_filename = 'figure1.pdf'
#outpdf = PdfPages(pdf_filename)
#outpdf.savefig(fig) # ohne () saves the current figure
#outpdf.attach_note(text, positionRect=[-100, -100, 0, 0])
#outpdf.close()



#ratio = 1.0
#xleft, xright = ax.get_xlim()
#ybottom, ytop = ax.get_ylim()
#ax.set_aspect(abs((xright-xleft)/(ybottom-ytop))*ratio)

# falls in subplots
#https://stackoverflow.com/questions/38938454/python-saving-multiple-subplot-figures-to-pdf?noredirect=1&lq=1
