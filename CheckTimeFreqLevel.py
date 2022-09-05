from olMEGA_DataService_Tools import acousticweighting as aw
from olMEGA_DataService_Tools import freq2freqtransforms as freqt

import numpy as np
import pandas as pd
import scipy.signal as sig
import matplotlib.pyplot as plt

# callib_val = 93.2 wird noch nicht genutzt
f0 = 1000 # Diese Frequenz als Bsp auf 200 aendern
fft_len = 2048 # hiervon unabhaengig
fs = 16000 # hiervon unabhaengig
len_s = 1
len_samples = len_s*fs

# generate sinus
kk = np.linspace(0,len_samples,len_samples)
signal = np.sin(2*np.pi*f0/fs*kk)

fig, ax = plt.subplots()
#ax.plot(signal)
#plt.show()

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

