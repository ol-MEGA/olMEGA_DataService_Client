from olMEGA_DataService_Tools import acousticweighting as aw
from olMEGA_DataService_Tools import freq2freqtransforms as freqt

import numpy as np
import pandas as pd
import scipy.signal as sig
import matplotlib.pyplot as plt

callib_val = 93.2
f0 = 1000
fft_len = 1024
fs = 16000
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

# compute a_weigted RMS


# compute octav RMS


# compute onethird RMS  

