import numpy as np
import pandas as pd
import json
import datetime
from datetime import timedelta
import copy
from numpy import NaN
import os
import matplotlib.pyplot as plt
import scipy.stats.stats as stats

# FixFlex = 'flexible'
FixFlex = 'fixed'
conti_path = '/Users/daisy/Dropbox/test_different_time_slot/raw_sample/participant_2/'
path4 = '/Users/daisy/Dropbox/test_different_time_slot/raw_truncated/test/par_2/' + FixFlex + '/'
savef = '/Users/daisy/Dropbox/test_different_time_slot/Report_com/test/par_2/' + FixFlex + '/'
path = savef
#
# conti_path = '/Users/daisy/Dropbox/test_different_time_slot/raw_sample/participant_1/'
# path4 = '/Users/daisy/Dropbox/test_different_time_slot/raw_truncated/test/par_1/' + FixFlex + '/'
# savef = '/Users/daisy/Dropbox/test_different_time_slot/Report_com/test/par_1/' + FixFlex + '/'
# path = savef

def hms_to_seconds(t):
    h, m, s = str(t).split(":")
    h = int(h.split(" ")[2])
    m, s = [int(i) for i in [m, s]]
    return 3600 * h + 60 * m + s


file = os.listdir(conti_path)
for i in file:
    if i.endswith('_BI_output.json'):
        with open(conti_path + i) as f:
            rawBI = json.load(f)
rawBI = pd.DataFrame.from_dict(rawBI)
TBCal = 'bi'
rawPartial = rawBI[[TBCal, 'seg_start', 'seg_end']].dropna(how='all')
rawPartial['seg_end'] = rawPartial['seg_end'].apply(
    lambda dt: datetime.datetime.fromtimestamp(dt))
rawPartial['seg_start'] = rawPartial['seg_start'].apply(
    lambda dt: datetime.datetime.fromtimestamp(dt))
rawPartial['Var'] = rawPartial['seg_end'] - rawPartial['seg_start']
TBlock = []



FirstT = rawPartial['seg_start'].iloc[0]  # next
remainT = int(str(FirstT.replace(second=0, hour=0)).rsplit(':')[1])
EndT = rawPartial['seg_end'].iloc[-1]
EndTR = EndT.replace(second=0, minute=0) + timedelta(minutes=remainT)
if EndTR < EndT:
    EndTR = EndT + timedelta(minutes=60)
TimeRng = pd.date_range(start=str(FirstT), end=str(EndTR), freq='1H')
TIndex = 0

TimeToCompareS = FirstT
TimeToCompareE = rawPartial['seg_end'].iloc[0]
# while TimeToCompareE < EndT:
for j in TimeRng:
    try:
        while TimeToCompareS < j:
            TBlock.append(j)
            TIndex += 1
            TimeToCompareS = rawPartial['seg_start'].iloc[TIndex]
            TimeToCompareE = rawPartial['seg_end'].iloc[TIndex]
            continue
    except Exception as e:
        print('outside of the loop')

E = list(rawPartial['seg_end'])
if len(TBlock) > len(E):
    TBlock = TBlock[:-1]
    TBlock[-1] = 'Sep'

for i in np.arange(0, len(TBlock), 1):
    if TBlock[i] != 'Sep':
        if TBlock[i] < E[i]:
            TBlock[i] = 'Sep'

rawPartial['TBlock'] = TBlock
pd.options.mode.chained_assignment = None

# for TBCal in categories:
s2 = rawPartial.iloc[1]  # could be any value
for i in np.arange(0, len(rawPartial), 1):
    if rawPartial.iloc[i, 4] == 'Sep':
        temp = rawPartial.iloc[i, :]
        FHalfT = rawPartial.iloc[i - 1, 4] - rawPartial.iloc[i, 1]
        SHalfT = rawPartial.iloc[i, 2] - rawPartial.iloc[i - 1, 4]
        rawPartial.iloc[i, 2] = rawPartial.iloc[i - 1, 4]
        rawPartial.iloc[i, 3] = FHalfT
        rawPartial.iloc[i, 4] = rawPartial.iloc[i - 1, 4]
        s2[TBCal] = temp[TBCal]
        s2['seg_start'] = rawPartial.iloc[i - 1, 4]
        s2['seg_end'] = temp['seg_end']
        s2['Var'] = SHalfT
        s2['TBlock'] = rawPartial.iloc[i + 1, 4]
        rawPartial = rawPartial.append(s2)

rawPartial = rawPartial.sort_values(by='seg_start').set_index(keys=np.arange(0, len(rawPartial), 1))
# rawPartial.to_csv('rawPartial_calculated.csv', sep=',')

for i in np.arange(0, len(rawPartial), 1):
    rawPartial.loc[:, "Var"][i] = hms_to_seconds(rawPartial.loc[:, "Var"][i])

'''
This is a test
'''

rawPartial['TBlock'][rawPartial['TBlock'] == 'Sep'] = NaN

# for i in np.arange(0, len(TruncatedPartial), 1):
#     if rawPartial['TBlock'].iloc[i] == 'Sep':
#         rawPartial['TBlock'].iloc[i] = NaN
rawPartial.dropna(subset=['TBlock'])

# rawPartial.to_csv('rawPartial_test1.csv', sep=',')
a = rawPartial.groupby(['TBlock']).sum(axis=1)

w = []
for i in sorted(set(a.index)):
    Clu = rawPartial[rawPartial['TBlock'] == i]
    Clu = Clu.dropna(how='any')
    if len(Clu) > 0:
        weighted_mean = sum(Clu[TBCal] * Clu['Var']) / sum(Clu['Var'])
        weig_3 = weighted_mean ** (1.0 / 3)
        w.append(weig_3)
        # print(' Clu not emp')
    else:
        weig_3 = NaN
        # print(' Clu emp')
        w.append(weig_3)
newData = copy.deepcopy(a[TBCal]).reset_index()
newData[TBCal] = w

fname = savef + TBCal + '_Continuous_hourlyM.csv'
fname2 = savef + TBCal + '_Continuous_hourlyM.pickle'
newData.to_csv(fname, sep=',')
newData.to_pickle(fname2)

del rawBI, rawPartial, newData, fname
'''
# This is to generate truncated data
  step1 - get truncated data

  step 2 - test function to get truncated BI

 # read truncated data
'''


file = os.listdir(path4)
for each_file in file:
    if each_file.endswith('_BI_output.json'):
        with open(path4 + each_file) as f:
            truncated_bi = json.load(f)

TruncatedDF = pd.DataFrame.from_dict(truncated_bi)

TruncatedPartial = TruncatedDF[[TBCal, 'seg_start', 'seg_end']].dropna(how='all')
TruncatedPartial['seg_end'] = TruncatedPartial['seg_end'].apply(
    lambda dt: datetime.datetime.fromtimestamp(dt))
TruncatedPartial['seg_start'] = TruncatedPartial['seg_start'].apply(
    lambda dt: datetime.datetime.fromtimestamp(dt))
TruncatedPartial['Var'] = TruncatedPartial['seg_end'] - TruncatedPartial['seg_start']

FirstT = TruncatedPartial['seg_start'].iloc[0]  # next
EndT = TruncatedPartial['seg_end'].iloc[-1]

print(FirstT, EndTR)

TimeToCompareS = FirstT
TimeToCompareE = TruncatedPartial['seg_end'].iloc[0]
TBlock = []
TIndex = 0

for j in TimeRng:
    try:
        while TimeToCompareS < j:
            TBlock.append(j)
            TIndex += 1
            TimeToCompareS = TruncatedPartial['seg_start'].iloc[TIndex]
            TimeToCompareE = TruncatedPartial['seg_end'].iloc[TIndex]
            continue
    except Exception as e:
        print('outside of the loop')

E = list(TruncatedPartial['seg_end'])
if len(TBlock) != len(E):

    # print('last time peirod: start', TruncatedPartial['seg_start'].iloc[-1])
    # print('last time peirod: end', TruncatedPartial['seg_end'].iloc[-1])
    if len(TBlock) > len(E):
        print('check')
        for i in np.arange(0, len(TBlock) - 1, 1):
            if TBlock[i] < E[i]:
                TBlock[i] = 'Sep'
        TBlock = TBlock[:(len(TBlock) - 1)]
    elif len(TBlock) == len(E):
        for i in np.arange(0, len(TBlock), 1):
            if TBlock[i] < E[i]:
                TBlock[i] = 'Sep'
    else:
        print('error')

TruncatedPartial['TBlock'] = TBlock
# TruncatedPartial.to_csv('TruncatedPartial_test.csv')
pd.options.mode.chained_assignment = None

s2 = TruncatedPartial.iloc[1]  # could be any value
for i in np.arange(0, len(TruncatedPartial), 1):
    if TruncatedPartial.iloc[i, 4] == 'Sep':
        temp = TruncatedPartial.iloc[i, :]
        if i > 0:
            FHalfT = TruncatedPartial.iloc[i - 1, 4] - TruncatedPartial.iloc[i, 1]
            SHalfT = TruncatedPartial.iloc[i, 2] - TruncatedPartial.iloc[i - 1, 4]
            TruncatedPartial.iloc[i, 2] = TruncatedPartial.iloc[i - 1, 4]
            TruncatedPartial.iloc[i, 3] = FHalfT
            TruncatedPartial.iloc[i, 4] = TruncatedPartial.iloc[i - 1, 4]
            s2[TBCal] = temp[TBCal]
            s2['seg_start'] = TruncatedPartial.iloc[i - 1, 4]
            s2['seg_end'] = temp['seg_end']
            s2['Var'] = SHalfT
            s2['TBlock'] = TruncatedPartial.iloc[i + 1, 4]
            TruncatedPartial = TruncatedPartial.append(s2)
        else:
            TruncatedPartial.iloc[i, 2] = TimeRng[0]
            TruncatedPartial.iloc[i, 3] = TruncatedPartial.iloc[i, 1] - TruncatedPartial.iloc[i, 2]
            TruncatedPartial.iloc[i, 4] = TimeRng[0]
            s2['seg_start'] = TimeRng[0]
            s2['seg_end'] = TruncatedPartial.iloc[i, 2]
            s2['Var'] = s2['seg_end'] - s2['seg_start']
            s2['TBlock'] = TimeRng[1]
            TruncatedPartial = TruncatedPartial.append(s2)
print('error might occur here')


TruncatedPartial.dropna(axis=0, subset=['TBlock'])
TruncatedPartial = TruncatedPartial.sort_values(by='seg_start').set_index(
    keys=np.arange(0, len(TruncatedPartial), 1))

for i in np.arange(0, len(TruncatedPartial), 1):
    TruncatedPartial.loc[:, "Var"][i] = hms_to_seconds(TruncatedPartial.loc[:, "Var"][i])
'''
This is a test
'''
for i in np.arange(0, len(TruncatedPartial), 1):
    if TruncatedPartial['TBlock'].iloc[i] == 'Sep':
        a = a + 1
        TruncatedPartial['TBlock'].iloc[i] = NaN
# TruncatedPartial[]
# TruncatedPartial.to_csv('TruncatedPartial_test1.csv', sep=',')
a = TruncatedPartial.groupby(['TBlock']).sum(axis=1)
w = []
# print('is it here ???')

for m in sorted(set(a.index)):
    Clu = TruncatedPartial[TruncatedPartial['TBlock'] == m]
    Clu = Clu.dropna(how='any')
    if len(Clu) > 0:
        weighted_mean = sum(Clu[TBCal] * Clu['Var']) / sum(Clu['Var'])
        weig_3 = weighted_mean ** (1.0 / 3)
        w.append(weig_3)
        print(' Clu not emp')
    else:
        weig_3 = NaN
        print(' Clu emp')
        w.append(weig_3)
newData = copy.deepcopy(a[TBCal]).reset_index()
newData[TBCal] = w
k = '2in5'


bi_Continuous_hourlyM = pd.read_pickle(path + 'bi_Continuous_hourlyM.pickle')
allD = copy.deepcopy(bi_Continuous_hourlyM).set_index(['TBlock'])

data = newData
plt.figure()
plt.ylim(0, 1)
plt.plot(allD['bi'], label='continuous')
CompareD = copy.deepcopy(data).set_index(['TBlock'])
plt.plot(CompareD['bi'],label=k)
plt.legend()
plt.show()
m = pd.merge(allD,data,how='left',left_on='TBlock',right_on='TBlock')
m = m.set_index(['TBlock'])
m_drpNA = m.dropna()

print(stats.pearsonr(m_drpNA['bi_x'],m_drpNA['bi_y']),k, path4.split('/')[-2],len(m_drpNA))

