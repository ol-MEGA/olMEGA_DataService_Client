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

startdir = './results'

list_of_resultfiles = find_file_recursiv(startdir,'.json')
all_df_list = []
#for file_counter, onefilename in enumerate(list_of_resultfiles):
onefilename = list_of_resultfiles[5]

with open(onefilename, 'r') as fout:
    data = json.load(fout)
    df = pd.DataFrame(data, index=list(range(len(data))))

fs = df.loc[0]["fs"]
print(fs)

pegelday = df["meanPegel"].to_numpy()

dynamics = df["perc95"].to_numpy() - df["perc5"].to_numpy()

starttime_list = df["startdate"].to_list()

starttime_vec = []
for kk, onetimestring in enumerate(starttime_list):
    starttime_vec.append(datetime.strptime(onetimestring,'%Y-%m-%d %H:%M:%S'))
        

fig, ax = plt.subplots(nrows=2)

NUM_COLORS = 20 # adjust to subjects

# lighter for EMA1
# darker for EMA2
# alpha lets see, what looks good


cm = plt.get_cmap('gist_rainbow')
newcolor=[cm(1.*i/NUM_COLORS) for i in range(NUM_COLORS)]
ax[0].plot(dynamics, color = adjust_lightness(newcolor[1],1.4), alpha = 0.5)
ax[0].set_xticks([])
ax[1].plot(starttime_vec, pegelday,  color = adjust_lightness(newcolor[4],1.4), alpha = 0.5)
#ax.set_xticklabels(ax.get_xticks(), rotation = 45)
ax[1].xaxis.set_major_formatter(xfmt)
ax[1].tick_params(axis='x', rotation=45)
plt.show()



