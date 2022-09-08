from olMEGA_DataService_Tools import acousticweighting as aw
from olMEGA_DataService_Tools import freq2freqtransforms as freqt

import numpy as np
import pandas as pd
import scipy.signal as sig
import scipy.fft

import matplotlib.pyplot as plt
import soundfile as sf
import mosqito as mq

insig = '/home/bitzer/SoftDev/Python/acousticmeasures/MoSQITo/validations/sq_metrics/loudness_zwst/input/ISO_532_1/Test signal 3 (1 kHz 60 dB).wav'

sinsig, samplerate = sf.read(insig)

# callib_val = 93.2 wird noch nicht genutzt
#f0 = 1000 # Diese Frequenz als Bsp auf 200 aendern
fft_len = 1024 # hiervon unabhaengig
#fs = 16000 # hiervon unabhaengig
#len_s = 1
#len_samples = len_s*fs

# generate sinus
#kk = np.linspace(0,len_samples,len_samples)
#signal = np.sin(2*np.pi*f0/fs*kk)
fs = samplerate
signal = sinsig*2 * 2 ** 0.5
fig, ax = plt.subplots()
#ax.plot(signal)
#plt.show()
gain_db = 0
gain = 10**(gain_db/20)
signal *= gain
# compute PSD with window
freq_vec, Pss = sig.welch(signal,fs,nperseg=fft_len, noverlap=fft_len/2,nfft = fft_len)
ax.plot(freq_vec,10*np.log10(Pss))
#plt.show()

# compute RMS in time
rmstime = np.sqrt(np.mean(signal*signal))
print(f'rms time: {20*np.log10(rmstime)}')

# compute RMS in freq
rmsfreq = np.sqrt(np.sum(Pss)*fs/fft_len)
print(f'rms freq: {20*np.log10(rmsfreq)}')

# compute a_weigted RMS time
b,a = aw.get_weight_coefficients(fs)
signal_a = sig.lfilter(b,a,signal)
rmstime_a = np.sqrt(np.mean(signal_a*signal_a))
print(f'rms(a) time: {20*np.log10(rmstime_a)}')


# compute a_weigted RMS freq
w,f = aw.get_fftweight_vector(fft_len,fs,'a','lin')
rmsfreq_a = np.sqrt(np.sum((Pss*w*w)*fs/fft_len)) # this works because of broadcasting rules in python
print(f'rms(a) freq: {20*np.log10(rmsfreq_a)}')
        
# compute octav RMS
octav_matrix, f_mid, f_nominal = freqt.get_spectrum_fractionaloctave_transformmatrix(fft_len,fs,125,4000,1)
octavPegel = ((Pss*fs/fft_len)@octav_matrix) # this works because of broadcasting rules in python
print(f'rms Oktavpegel freq: {10*np.log10(octavPegel)}')

Pss = np.sqrt(Pss)*np.sqrt(fs/fft_len)/gain
Pss = Pss.reshape(Pss.shape[0],1)
if fs >= 32000:
    Pss_final = Pss
    freq_vec = np.linspace(0,fs/2, num = int(fft_len/2+1))
elif fs>=16000 and fs < 32000:
    Pss_final = np.zeros((2*Pss.shape[0], Pss.shape[1]))
    Pss_final[0:Pss.shape[0],:] = Pss/np.sqrt(2)
    freq_vec = np.linspace(0,2*fs/2, num = 2*int(fft_len/2+1))
elif fs>=8000 and fs < 16000:
    Pss_final = np.zeros((4*Pss.shape[0], Pss.shape[1]))
    Pss_final[0:Pss.shape[0],:] = Pss/2
    freq_vec = np.linspace(0,4*fs/2, num = 4*int(fft_len/2+1))
else:
    print("Samplingrate to low")
    
an,bn,c = mq.loudness_zwst_freq(Pss_final, freq_vec)
#a,b,c = mq.loudness_zwst_freq(np.sqrt(Pss.transpose), freq_vec)
print(an)

n = len(signal)
spec = np.abs(2 / np.sqrt(2) / n * scipy.fft.fft(signal)[0:n//2])
freqs = scipy.fft.fftfreq(n, 1/fs)[0:n//2]
# Compute Loudness
N, N_specific, bark_axis = mq.loudness_zwst_freq(spec, freqs)
print(N)
a,b,c = mq.loudness_zwst(signal,fs)
print(a)

S = mq.sharpness_din_freq(spec, freqs)
print(S)
Pss_final = Pss_final.reshape(513,)
S2 = mq.sharpness_din_freq(Pss_final, freq_vec)
print(S2)

