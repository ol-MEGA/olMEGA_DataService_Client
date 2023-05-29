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
import json
import pyreadstat

xfmt = mdates.DateFormatter('%H:%M:%S')
def removeStationaryTones(PSDSpectrum, fs):
    removed = 0
    PSD_return = PSDSpectrum.copy()
    if fs > 10000:
        # look for disturbing tones at high frequencies and remove them
        analysis_percentile = 90
        # percentile over time
        perc_log = 10*np.log10(np.percentile(PSDSpectrum,
                               analysis_percentile, axis=0))
        peaks, peaks_p = sig.find_peaks(perc_log, prominence=4, width=3)
        if len(peaks) > 0:
            peaks = peaks[~(peaks < 200)]  # delete low freq maxima
            # print(peaks)
            peaks_all = []
            for p in peaks:
                ext_max = np.min([5, 512-p])
                peaks_ext = p + np.arange(-5, ext_max)
                peaks_all.extend(peaks_ext)

            PSD_return[:, peaks_all] = 0
            removed = 1
    return PSD_return, removed


def find_file_recursiv(start_dir, seach_string):
    pys = []
    for p, d, f in os.walk(start_dir):
        for file in f:
            if file.endswith(seach_string):
                pys.append(os.path.join(p, file))
    return pys


def find_feature_type_in_list_of_filenames(filenames_list, featuretype='PSD'):
    final_list = []
    for onefilename in filenames_list:
        if featuretype in onefilename:
            final_list.append(onefilename)
    return final_list


def load_data_for_files_in_filelist(filenames_list):
    # load all files
    alldata_stacked = []

    for one_filename in filenames_list:
        feat_dict = {}
        featdata = FeatureFile.load(one_filename)
        if featdata == None:
            continue
        feat_dict.update({"fs": featdata.fs})
        feat_dict.update({"data": featdata.data})
        feat_dict.update({"start": featdata.StartTime})
        feat_dict.update({"start_datetime": datetime.strptime(
            featdata.StartTime[:-3], '%y%m%d_%H%M%S')})
        # data = featdata.data
        alldata_stacked.append(feat_dict)
    return alldata_stacked

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


startdir = '/home/bitzer/Nextcloud/Shares/olMEGA_Normmessungen/Messsignale/'

#anal_dir = 'ihab_converted/'
anal_dir = 'olmega/'

anal_times_8k = [datetime(2023,5,15,12,18,47), datetime(2023,5,15,12,19,59),datetime(2023,5,15,12,21,13),
                 datetime(2023,5,15,12,30,48),datetime(2023,5,15,12,32,8),datetime(2023,5,15,12,33,31),
                 datetime(2023,5,15,12,36,31),datetime(2023,5,15,12,38,32),datetime(2023,5,15,12,40,30),
                 datetime(2023,5,15,12,42,6),datetime(2023,5,15,12,43,29),datetime(2023,5,15,13,15,31),datetime(2023,5,22,13,54,39)]

anal_times_16k = [datetime(2023,5,22,15,11,59), datetime(2023,5,22,15,13,51),datetime(2023,5,22,15,15,49),
                  datetime(2023,5,22,15,17,51),datetime(2023,5,22,15,19,52),datetime(2023,5,22,15,22,4),
                  datetime(2023,5,22,15,23,59),datetime(2023,5,22,15,27,5),datetime(2023,5,22,15,30,13),
                  datetime(2023,5,22,15,33,22),datetime(2023,5,22,15,37,18),datetime(2023,5,22,15,39,15),datetime(2023,5,22,15,42,19),]

anal_times_24k = [datetime(2023,5,15,12,55,8), datetime(2023,5,15,12,56,17),datetime(2023,5,15,12,57,27),
                 datetime(2023,5,15,12,58,49),datetime(2023,5,15,13,0,5),datetime(2023,5,15,13,1,19),
                 datetime(2023,5,15,13,2,29),datetime(2023,5,15,13,3,44),datetime(2023,5,15,13,4,58),
                 datetime(2023,5,15,13,6,8),datetime(2023,5,15,13,12,52),datetime(2023,5,22,13,58,37)]

anal_signalnames_8k = ['ISTS_55dB','ISTS_65dB', 'ISTS_70dB', 'ISTS70db_ICRA765dB', 'IFNoise', '1/f_50_100_10k', 
                    '1/f_50_200_8k', '1/f_50_200_5k', '1/f_50_1000', 'Orchestra', 'Piano', 'ISTS70dB_IF68dB', 'Autobahn']
anal_signalnames_16k = ['ISTS_65dB','ISTS_55dB', 'ISTS_70dB', 'ISTS70dB_IF68dB','ISTS70db_ICRA765dB', 'IFNoise', '1/f_50_100_10k', 
                    '1/f_50_200_8k', '1/f_50_200_5k', '1/f_50_1000', 'Orchestra', 'Piano',  'Autobahn']
anal_signalnames_24k = ['ISTS_55dB', 'ISTS_65dB', 'ISTS70db_ICRA765dB', 'IFNoise', '1/f_50_100_10k', 
                    '1/f_50_200_8k', '1/f_50_200_5k', '1/f_50_1000', 'Orchestra', 'Piano',  'Autobahn']
anal_length = timedelta(seconds=50)




allfeatfiles = find_file_recursiv(os.path.join(startdir, anal_dir), '.feat')

psd_files = find_feature_type_in_list_of_filenames(allfeatfiles, 'PSD')
psd_files.sort()

alldata = load_data_for_files_in_filelist(psd_files)

fullspectra = []

used_fs = 16000
all_dates = []
all_psd = []
all_octav_psd = []
for oneblock in alldata:
    if (oneblock["fs"] == used_fs):
        data = oneblock["data"]
        n = [int(data.shape[1] / 2), int(data.shape[1] / 4)]

        magicPSDconvert = 1
        if oneblock["fs"] == 8000:  # legacy versions
            magicPSDconvert = 0.75
            magicPSDconvertCorrect = 24
        elif oneblock["fs"] == 24000:
            magicPSDconvert = 4
            magicPSDconvertCorrect = 28

        Pxx_one = data[:, n[0]: n[0] + n[1]]
        Pyy_one = data[:, n[0] + n[1]:]

        nr_of_frames, fft_size = Pxx_one.shape
        base = oneblock["start_datetime"]
        date_list = [base + timedelta(milliseconds=x*125)
                     for x in range(nr_of_frames)]
        all_dates.append(date_list)

        w, f = aw.get_fftweight_vector(
            (fft_size-1)*2, oneblock["fs"], 'z', 'lin')
        wa, f = aw.get_fftweight_vector(
            (fft_size-1)*2, oneblock["fs"], 'a', 'lin')
        octav_matrix, f_mid, f_nominal = freqt.get_spectrum_fractionaloctave_transformmatrix(
            (fft_size-1)*2, oneblock["fs"], 125, 4000, 1)

        # this works because of broadcasting rules in python magicPSDconvert magic number to get the correct results
        meanPSD_one = (
            ((Pxx_one+Pyy_one)*0.5*oneblock["fs"])*wa*wa)*magicPSDconvert

        # fig, ax = plt.subplots()
        # #fig.set_size_inches(w=8, h=14)
        # freq_vec = np.linspace(start=0, stop=used_fs/2, num = fft_size)
        # hax = ax.pcolormesh(date_list, freq_vec, 10*np.log10(meanPSD_one.transpose()), vmin=10, vmax = 80)
        # from mpl_toolkits.axes_grid1 import make_axes_locatable        
        # divider = make_axes_locatable(ax)
        # cax = divider.append_axes("right", size="5%", pad=0.05)

        # plt.colorbar(hax, cax=cax)        
        # ax.xaxis.set_major_formatter(xfmt)
        # ax.tick_params(axis='x', rotation=45)
        # plt.show()




        all_psd.append(meanPSD_one)

        octavPSD_one = (((Pxx_one+Pyy_one)*w*w*magicPSDconvert*used_fs/((fft_size-1)*2))@octav_matrix) # this works because of broadcasting rules in python


        all_octav_psd.append(octavPSD_one)
        #rms_lin = np.mean((meanPSD_one), axis=1)
        #mean_PSD_time = 10 * np.log10(np.mean((meanPSD_one), axis=0) + np.finfo(float).eps)
        #rms_psd = 10*np.log10(rms_lin)  # mean over frequency

nrofchunks = len(all_psd)
size_of_data = all_psd[0].shape
alldata_psd = np.stack(all_psd, axis=0).reshape((nrofchunks*size_of_data[0],size_of_data[1]))
all_dates = np.stack(all_dates, axis=0).reshape((nrofchunks*len(all_dates[0])))

size_of_data = all_octav_psd[0].shape
alloctavdata_psd = np.stack(all_octav_psd, axis=0).reshape((nrofchunks*size_of_data[0],size_of_data[1]))

pdf_filename = f'Testsignals_Analysis{used_fs}.pdf'
outpdf = PdfPages(pdf_filename)

testsignals_allresults = []

if (used_fs == 8000):
    anal_list = anal_times_8k
if (used_fs == 16000):
    anal_list = anal_times_16k
if (used_fs == 24000):
    anal_list = anal_times_24k


for one_analcounter, one_analtime in enumerate(anal_list):
    idx = np.logical_and((all_dates>one_analtime), (all_dates<(one_analtime+anal_length)))
    idx2 = np.where(idx)[0]

    psd_toanalyse = alldata_psd[idx2,:]
    psd_octav_toananlyse = alloctavdata_psd[idx2,:]
    analyse_percentiles = [5, 30,65, 95, 99]
    analyse_modbands = [0, 0.25, 0.5, 1.0, 2.0, 4.0]


    resultentry = {}
    resultentry.update({"subject": "Testsignals"})
    if (used_fs == 8000):
        resultentry.update({"Testsignal": anal_signalnames_8k[one_analcounter]})
    if (used_fs == 16000):
        resultentry.update({"Testsignal": anal_signalnames_16k[one_analcounter]})
    if (used_fs == 24000):
        resultentry.update({"Testsignal": anal_signalnames_24k[one_analcounter]})
    resultentry.update({"studyname": "Testsignals"})
    #resultentry.update({"startdate": one_analtime })
    resultentry.update({"fs": used_fs})
    resultentry.update({"OVD_percent": 0})
    rms_lin = np.mean((psd_toanalyse), axis=1)
    #mean_PSD_time = 10*np.log10(np.mean((psd_toanalyse), axis=0) +  np.finfo(float).eps)

    #resultentry.update({"Mean_spectrum": mean_PSD_time})
    rms_psd = 10*np.log10(rms_lin) # mean over frequency
    
    if (used_fs == 8000):
        print( anal_signalnames_8k[one_analcounter])
    if (used_fs == 16000):
        print( anal_signalnames_16k[one_analcounter])
    if (used_fs == 24000):
        print( anal_signalnames_24k[one_analcounter])

    print (f'MeanPegel: {10*np.log10(np.mean(rms_lin)+ np.finfo(float).eps)}')
    print (f'MeanPegelvialog: {(np.mean(rms_psd)+ np.finfo(float).eps)}')

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

    
    octavPSD_one = psd_octav_toananlyse
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


    # this is all plotting
    fig, ax = plt.subplots(nrows=2)
    fig.set_size_inches(w=8, h=14)
    freq_vec = np.linspace(start=0, stop=used_fs/2, num = fft_size)
    #hax = ax[0].pcolormesh(all_dates[idx2], freq_vec, 10*np.log10(alldata_psd[idx2,:].transpose()), vmin=20, vmax = 100)
    x_lims = mdates.date2num([all_dates[idx2[0]],all_dates[idx2[-1]]])
    
    hax = ax[0].imshow(10*np.log10(alldata_psd[idx2,:].transpose()),extent =[x_lims[0],x_lims[1], 0, used_fs/2], vmin=20, vmax = 100, aspect = 'auto', origin = 'lower')
    ax[0].xaxis_date()
    from mpl_toolkits.axes_grid1 import make_axes_locatable        
    divider = make_axes_locatable(ax[0])
    cax = divider.append_axes("right", size="5%", pad=0.05)

    plt.colorbar(hax, cax=cax)        
    #plt.axis((None, None, 0, 200))
    ax[0].set_ylabel('Frequency [Hz]')
    if (used_fs == 8000):
        ax[0].set_title(anal_signalnames_8k[one_analcounter])
    if (used_fs == 16000):
        ax[0].set_title(anal_signalnames_16k[one_analcounter])
    if (used_fs == 24000):
        ax[0].set_title(anal_signalnames_24k[one_analcounter])
    #plt.xlabel('Time [sec]')
    #plt.show()
    #ax.plot(10*np.log10(np.mean(meanPSD_one, 0)))
    ax[0].xaxis.set_major_formatter(xfmt)
    ax[0].tick_params(axis='x', rotation=45)
    #plt.show()
    outpdf.savefig(fig)
    testsignals_allresults.append(resultentry)


filename = os.path.join('.', f"PSD_testsignals{used_fs}.json")
with open(filename, 'w') as fout:
    json.dump(testsignals_allresults, fout)

outpdf.close()
df = pd.DataFrame(testsignals_allresults, index=list(range(len(testsignals_allresults))))

result_filename = os.path.join('.',f"PSD_testsignals{used_fs}.sav")

pyreadstat.write_sav(df, result_filename)
#print(psd_files)


# samplingFrequency = 100000
#     s1 = np.arange(start=1, stop=12000001, dtype=float)
#     data = np.random.randn(4000000)
#     zeros = np.zeros(4000000)
#     s2 = np.append(zeros, data)
#     s2 = np.append(s2, zeros)
#     print("Size of s1 array: ", s1.size, "size of s2 array: ", s2.size)

#     # Generate some timestamp looking things
#     today = datetime.datetime.now()
#     time_stamps = np.full(s1.shape, today, dtype=object)
#     for index, time_offset in enumerate(s1):
#         time_stamps[index] = time_stamps[index] + datetime.timedelta(microseconds=time_offset)

#     # Convert datetimes to matplotlib dates for plotting
#     time_stamps_dates = mdates.date2num(time_stamps)

#     # Plot
#     fig = plt.figure()
#     ax1, ax2 = fig.subplots(2, 1, sharex=True)
#     ax1.plot(time_stamps_dates, s2)
#     ax2.xaxis_date()
#     date_format = mdates.DateFormatter('%H:%M:%S.%f')
#     ax1.xaxis.set_major_formatter(date_format)
#     ax1.set_xlabel('Time')
#     ax1.set_ylabel('Amplitude')

#     spec, freq, time, imageAxis = ax2.specgram(s2, Fs=100000, NFFT=1024,
#                                                xextent=(time_stamps_dates[0], time_stamps_dates[-1]))
#     ax2.set_xlabel('Time')
#     ax2.set_ylabel('Frequency')

#     plt.show()
