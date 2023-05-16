"""
script and function to convert json dump files of objective data to pandas frames
(c) J.Bitzer@Jade-hs
License: BSD 3-clause
Version 1.0.0 JB 12.05.2023

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
import pyreadstat


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
for file_counter, onefilename in enumerate(list_of_resultfiles):
    with open(onefilename, 'r') as fout:
        data = json.load(fout)

    #onedict = data[0]

    df = pd.DataFrame(data, index=list(range(len(data))))
    all_df_list.append(df)

all_df = pd.concat(all_df_list)

result_filename = os.path.join(startdir,'AllResults')
all_df.to_csv(result_filename + '.csv')

pyreadstat.write_sav(all_df, result_filename+'.sav')
