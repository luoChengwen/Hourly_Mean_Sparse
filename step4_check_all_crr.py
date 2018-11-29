import sys
import os
import scipy.stats.stats as stats
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import copy
from numpy import NaN
# FixFlex = 'fixed_seg'
FixFlex = 'flexible_seg'

path = '/Users/daisy/Dropbox/test_different_time_slot/Report_com/participant_1/' + FixFlex + '/'
# path = '/Users/daisy/Dropbox/test_different_time_slot/Report_com/participant_2/'+ FixFlex + '/'
file_list=[]
each_file = []
for each_file in os.listdir(path):  #
    if each_file.endswith('.pickle'):
        each_file = each_file
        file_list.append(each_file)
file_list = list(set(file_list))

bi_Continuous_hourlyM = pd.read_pickle(path + 'bi_Continuous_hourlyM.pickle')
allD = copy.deepcopy(bi_Continuous_hourlyM).set_index(['TBlock'])

for each_file in file_list:
        DName = each_file.split('.pickle')[0]
        if DName != 'bi_Continuous_hourlyM':
            data = pd.read_pickle(path + each_file)
            vars()[DName] = data
            plt.figure()
            plt.ylim(0, 1)
            plt.plot(allD['bi'], label='continuous')
            CompareD = copy.deepcopy(eval(DName)).set_index(['TBlock'])
            plt.plot(CompareD['bi'], label=DName)
            plt.legend()
            plt.show()
            m = pd.merge(allD,eval(DName),how='left',left_on='TBlock',right_on='TBlock')
            m = m.set_index(['TBlock'])
            m_drpNA = m.dropna()

            print(stats.pearsonr(m_drpNA['bi_x'],m_drpNA['bi_y']),DName,len(m_drpNA))