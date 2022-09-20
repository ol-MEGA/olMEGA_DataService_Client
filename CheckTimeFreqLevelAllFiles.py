from olMEGA_DataService_Tools import acousticweighting as aw
from olMEGA_DataService_Tools import freq2freqtransforms as freqt
from olMEGA_DataService_Tools import FeatureFile as ff

import numpy as np
import pandas as pd
import scipy.signal as sig
import scipy.fft

import matplotlib.pyplot as plt
import soundfile as sf
import mosqito as mq


# calibrierung loudness
insig = '/home/bitzer/SoftDev/Python/acousticmeasures/MoSQITo/validations/sq_metrics/loudness_zwst/input/ISO_532_1/Test signal 3 (1 kHz 60 dB).wav'
sinsig, samplerate = sf.read(insig)
signal = sinsig*2 * 2 ** 0.5

gain_loudness_db = 14
gain_loudness = 10**(gain_loudness_db/20)

signal *= gain_loudness

loud_sig,c,d = mq.loudness_zwst(signal,samplerate)
print(f'loudness: {loud_sig}')
sharp_sig = mq.sharpness_din_st(signal,samplerate)
print(f'sharpness: {sharp_sig}')


rmstime = np.sqrt(np.mean(signal*signal))
print(f'rms time: {20*np.log10(rmstime)}')





insig_calib_114db = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/09192022/refMic_calib_sinus_114dB.wav'
insig_sin_0db = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/refMic_sinus1kHz_0.05.wav'

calib_signal, fs = sf.read(insig_calib_114db)

calib_SPL = 114
calib_dBFS = 10*np.log10(np.mean(calib_signal[:, 1]**2))
calib_const_dBFS = calib_SPL-calib_dBFS

refMic_signal, fs = sf.read(insig_sin_0db)
level_refMic_signal_dBFS = 10*np.log10(np.mean(refMic_signal[:, 1]**2))
level_refMic_signal_SPL = level_refMic_signal_dBFS + calib_const_dBFS

fs = 24000
is_sin_signal = False

if fs == 24000 and is_sin_signal == True:
    file_system_sinus = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/System1gelb2022_0920/Sys1_Aku_Data/000208_20220920_144853429.wav'
    feat_rmsfile = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/System1gelb2022_0920//Sys1_features/RMS_000208_20220920_144853429.feat'
    feat_psdfile = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/System1gelb2022_0920/Sys1_features/PSD_000208_20220920_144853429.feat'
elif fs == 8000 and is_sin_signal == True:
    file_system_sinus = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/System1gelb2022_0920/Sys1_Aku_Data/000230_20220920_150438203.wav'
    feat_rmsfile = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/System1gelb2022_0920//Sys1_features/RMS_000230_20220920_150438203.feat'
    feat_psdfile = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/System1gelb2022_0920/Sys1_features/PSD_000230_20220920_150438203.feat'
elif fs == 24000 and is_sin_signal == False:
    file_system_sinus = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/System1gelb2022_0920/Sys1_Aku_Data/000213_20220920_145353672.wav'
    feat_rmsfile = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/System1gelb2022_0920//Sys1_features/RMS_000213_20220920_145353672.feat'
    feat_psdfile = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/System1gelb2022_0920/Sys1_features/PSD_000213_20220920_145353672.feat'
elif fs == 8000 and is_sin_signal == False:
    file_system_sinus = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/System1gelb2022_0920/Sys1_Aku_Data/000228_20220920_150238194.wav'
    feat_rmsfile = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/System1gelb2022_0920//Sys1_features/RMS_000228_20220920_150238194.feat'
    feat_psdfile = '/home/bitzer/Nextcloud/Shares/CalibolMEGA/System1gelb2022_0920/Sys1_features/PSD_000228_20220920_150238194.feat'


system_signal, fs = sf.read(file_system_sinus)
level_system_signal_dBFS_0 = 10*np.log10(np.mean(system_signal[:, 0]**2))
level_system_signal_dBFS_1 = 10*np.log10(np.mean(system_signal[:, 1]**2))

CalibConst_Sys_0 = calib_const_dBFS - \
    (level_system_signal_dBFS_0 - level_refMic_signal_dBFS)
CalibConst_Sys_1 = calib_const_dBFS - \
    (level_system_signal_dBFS_1- level_refMic_signal_dBFS)

Calib_gain_0 = 10**(CalibConst_Sys_0/20)
Calib_gain_1 = 10**(CalibConst_Sys_1/20)

# RMS FEat
rmsfeat = ff.load(feat_rmsfile)
rms_data = rmsfeat.data.copy()
rms_data[:,0] *= Calib_gain_0
rms_data[:,1] *= Calib_gain_1
rms_meanvals = 20*np.log10(np.mean(rms_data, axis=0))
# scheint auch zu stimmen (Übereinstimmung mit ) 

# PSD FEat

psdfile = ff.load(feat_psdfile)
psd_data = psdfile.data.copy()
fs = psdfile.fs
n = [int(psd_data.shape[1] / 2), int(psd_data.shape[1] / 4)]
Pxx = psd_data[:, n[0] : n[0] + n[1]]
Pyy = psd_data[:, n[0] + n[1] : ]
Pxx *= Calib_gain_0*Calib_gain_0
Pyy *= Calib_gain_1*Calib_gain_1

if fs == 8000:
    magicPSDconvert = 0.4
    magicPSDconvertCorrect = 24
elif fs == 24000:
    magicPSDconvert = 0.3
    magicPSDconvertCorrect = 28


nr_of_frames, fft_size = Pxx.shape                    
w,f = aw.get_fftweight_vector((fft_size-1)*2,fs,'z','lin')
meanPSD = (((Pxx)*fs)*w*w)*magicPSDconvert # this works because of broadcasting rules in python magicPSDconvert magic number to get the correct results
rms_psd = 10*np.log10(np.mean((meanPSD), axis=1)) # mean over frequency
meanPSD2 = (((Pyy)*fs)*w*w)*magicPSDconvert # this works because of broadcasting rules in python
rms_psd2 = 10*np.log10(np.mean((meanPSD2), axis=1)) # mean over frequency
print(f'rms freq_old: {np.mean(rms_psd[:-2],)}')
print(f'rms freq_old: {np.mean(rms_psd2[:-2],)}')



rmsfreq = np.sqrt(np.sum(Pxx)*fs/((fft_size-1)*2))/magicPSDconvertCorrect # 24 magic number
print(f'rms freq_correct: {20*np.log10(rmsfreq)}')
rmsfreq2 = np.sqrt(np.sum(Pyy)*fs/((fft_size-1)*2))/magicPSDconvertCorrect
print(f'rms freq_correct: {20*np.log10(rmsfreq2)}')



meanPSD = (((Pxx+Pyy)*0.5*fs)*w*w)*magicPSDconvert # this works because of broadcasting rules in python
    
rms_psd = 10*np.log10(np.mean((meanPSD), axis=1)) # mean over frequency
octav_matrix, f_mid, f_nominal = freqt.get_spectrum_fractionaloctave_transformmatrix((fft_size-1)*2,fs,125,4000,1)
octavLevel = (((Pxx+Pyy)*w*w*magicPSDconvert*fs/((fft_size-1)*2))@octav_matrix) # this works because of broadcasting rules in python
print(f'rms Oktavpegel freq: {10*np.log10(octavLevel)}')

if fs == 8000: #very magic numbers here
    stereoPSD = (Pxx+Pyy)*0.5
elif fs == 24000:
    stereoPSD = (Pxx+Pyy)*0.5*0.35

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

loudness_calibration_db = -89
loudness_calibration = 10**(loudness_calibration_db/20)
stereoPSD_final *= loudness_calibration        
loudness,N_specific,bark_bands = mq.loudness_zwst_freq(stereoPSD_final, freq_vec)
#stereoPSD_final = stereoPSD_final.reshape(513,)
print(f'loudness: {loudness}')
sharpness = mq.sharpness_din_from_loudness(loudness, N_specific)
print(f'sharpness: {sharpness}')

exit()



print(level_refMic_signal_SPL)


fig, ax = plt.subplots()
ax.plot(system_signal)
plt.show()


# callib_val = 93.2 wird noch nicht genutzt
f0 = 1000 # Diese Frequenz als Bsp auf 200 aendern
fft_len = 1024  # hiervon unabhaengig
fs = 16000 # hiervon unabhaengig
len_s = 1
len_samples = len_s*fs

# generate sinus
kk = np.linspace(0,len_samples,len_samples)
signal = np.sin(2*np.pi*f0/fs*kk)
#fs = samplerate
# signal = sinsig*2 * 2 ** 0.5
fig, ax = plt.subplots()
# ax.plot(signal)
# plt.show()
gain_db = 0
gain = 10**(gain_db/20)
signal *= gain
# compute PSD with window
freq_vec, Pss = sig.welch(signal, fs, nperseg=fft_len,
                          noverlap=fft_len/2, nfft=fft_len)

ax.plot(freq_vec, 10*np.log10(Pss))
# plt.show()

# compute RMS in time
rmstime = np.sqrt(np.mean(signal*signal))
print(f'rms time: {20*np.log10(rmstime)}')

# compute RMS in freq
rmsfreq = np.sqrt(np.sum(Pss)*fs/fft_len)
print(f'rms freq: {20*np.log10(rmsfreq)}')

# compute a_weigted RMS time
b, a = aw.get_weight_coefficients(fs)
signal_a = sig.lfilter(b, a, signal)
rmstime_a = np.sqrt(np.mean(signal_a*signal_a))
print(f'rms(a) time: {20*np.log10(rmstime_a)}')


# compute a_weigted RMS freq
w, f = aw.get_fftweight_vector(fft_len, fs, 'a', 'lin')
# this works because of broadcasting rules in python

rmsfreq_a = np.sqrt(np.sum((Pss*w*w)*fs/fft_len))
print(f'rms(a) freq: {20*np.log10(rmsfreq_a)}')

# compute octav RMS
octav_matrix, f_mid, f_nominal = freqt.get_spectrum_fractionaloctave_transformmatrix(
    fft_len, fs, 125, 4000, 1)
# this works because of broadcasting rules in python
octavPegel = ((Pss*fs/fft_len))@octav_matrix
print(f'rms Oktavpegel freq: {10*np.log10(octavPegel)}')

Pss = np.sqrt(Pss)*np.sqrt(fs/fft_len)/gain
Pss = Pss.reshape(Pss.shape[0], 1)
if fs >= 32000:
    Pss_final = Pss
    freq_vec = np.linspace(0, fs/2, num=int(fft_len/2+1))
elif fs >= 16000 and fs < 32000:
    Pss_final = np.zeros((2*Pss.shape[0], Pss.shape[1]))
    Pss_final[0:Pss.shape[0], :] = Pss/np.sqrt(2)
    freq_vec = np.linspace(0, 2*fs/2, num=2*int(fft_len/2+1))
elif fs >= 8000 and fs < 16000:
    Pss_final = np.zeros((4*Pss.shape[0], Pss.shape[1]))
    Pss_final[0:Pss.shape[0], :] = Pss/2
    freq_vec = np.linspace(0, 4*fs/2, num=4*int(fft_len/2+1))
else:
    print("Samplingrate to low")

an, bn, c = mq.loudness_zwst_freq(Pss_final, freq_vec)
#a,b,c = mq.loudness_zwst_freq(np.sqrt(Pss.transpose), freq_vec)
print(an)

n = len(signal)
spec = np.abs(2 / np.sqrt(2) / n * scipy.fft.fft(signal)[0:n//2])
freqs = scipy.fft.fftfreq(n, 1/fs)[0:n//2]
# Compute Loudness
N, N_specific, bark_axis = mq.loudness_zwst_freq(spec, freqs)
print(N)
a, b, c = mq.loudness_zwst(signal, fs)
print(a)

S = mq.sharpness_din_freq(spec, freqs)
print(S)
Pss_final = Pss_final.reshape(513,)
S2 = mq.sharpness_din_freq(Pss_final, freq_vec)
print(S2)
