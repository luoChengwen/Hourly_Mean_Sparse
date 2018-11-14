import numpy as np
import pandas as pd
import json
import datetime
from datetime import timedelta
import copy
from numpy import NaN
import os


path1 = '/Users/daisy/Dropbox/test_different_time_slot/raw_truncated_test/'
path2 = '/Users/daisy/Dropbox/test_different_time_slot/Report_com/'

def hms_to_seconds(t):
    h, m, s = str(t).split(":")
    h = int(h.split(" ")[2])
    m, s = [int(i) for i in [m, s]]
    return 3600 * h + 60 * m + s
file = os.listdir(path1)
# FixFlex = ['flexible_seg','fixed_seg']
FixFlex = ['flexible_seg']
for FixFlex in FixFlex:
    for each_file in file:
        if each_file.startswith('CAP'):
        # if each_file.startswith('5b1'):
            print(each_file)
            pathTemp = path1 + each_file + '/' + FixFlex + '/'
            subfold = os.listdir(pathTemp)
            TBCal = 'bi'
            # freq = [1, 2, 3, 5, 10, 999]
            freq = [1, 2]
            timeseg = [t * 60 for t in freq]
            for e in timeseg:
                print(e)

                if e == 60:
                    pathTemp2 = path1 + each_file + '/' + 'flexible_seg' + '/'
                    with open(pathTemp2 + each_file.split('_')[0] + '_' + str(e) + '_BI_output.json') as f:
                        rawBI = json.load(f)
                    rawBI = pd.DataFrame.from_dict(rawBI)
                    rawPartial = rawBI[[TBCal, 'seg_start', 'seg_end']]
                    print('before_60',len(rawPartial))
                    rawPartial = rawPartial[rawPartial['bi'].notna()]
                    print('after_60', len(rawPartial))
                    rawPartial['seg_end'] = rawPartial['seg_end'].apply(
                        lambda dt: datetime.datetime.fromtimestamp(dt))
                    rawPartial['seg_start'] = rawPartial['seg_start'].apply(
                        lambda dt: datetime.datetime.fromtimestamp(dt))
                    rawPartial['Var'] = rawPartial['seg_end'] - rawPartial['seg_start']
                    FirstT = rawPartial['seg_start'].iloc[0]  # next
                    remainT = int(str(FirstT.replace(second=0, hour=0)).rsplit(':')[1])
                    EndT = rawPartial['seg_end'].iloc[-1]
                    EndTR = EndT.replace(second=0, minute=0) + timedelta(minutes=remainT)
                    if EndTR < EndT:
                        EndTR = EndT + timedelta(minutes=60)
                    TimeRng = pd.date_range(start=str(FirstT), end=str(EndTR), freq='1H')
                elif e > 60 and e < 700:
                    with open(pathTemp + each_file.split('_')[0] + '_' + str(e) + '_BI_output.json') as f:
                        rawBI = json.load(f)
                        rawBI = pd.DataFrame.from_dict(rawBI)
                        TBCal = 'bi'
                        rawPartial = rawBI[[TBCal, 'seg_start', 'seg_end']]

                        rawPartial = rawPartial[rawPartial['bi'].notna()]


                        rawPartial['seg_end'] = rawPartial['seg_end'].apply(
                            lambda dt: datetime.datetime.fromtimestamp(dt))
                        rawPartial['seg_start'] = rawPartial['seg_start'].apply(
                            lambda dt: datetime.datetime.fromtimestamp(dt))
                        rawPartial['Var'] = rawPartial['seg_end'] - rawPartial['seg_start']
                else:
                    with open(pathTemp + each_file.split('_')[0] + '_2in5raw_sample.json_BI_output.json') as f:
                        rawBI = json.load(f)
                        rawBI = pd.DataFrame.from_dict(rawBI)
                        TBCal = 'bi'
                        rawPartial = rawBI[[TBCal, 'seg_start', 'seg_end']]
                        # print('bf2', len(rawPartial))
                        rawPartial = rawPartial[rawPartial['bi'].notna()]
                        # print('after2', len(rawPartial))

                        rawPartial['seg_end'] = rawPartial['seg_end'].apply(
                            lambda dt: datetime.datetime.fromtimestamp(dt))
                        rawPartial['seg_start'] = rawPartial['seg_start'].apply(
                            lambda dt: datetime.datetime.fromtimestamp(dt))
                        rawPartial['Var'] = rawPartial['seg_end'] - rawPartial['seg_start']

                TBlock = []
                TIndex = 0
                leng = len(rawPartial)
                if leng>0:
                    TimeToCompareS = rawPartial['seg_start'].iloc[0]
                    TimeToCompareE = rawPartial['seg_end'].iloc[0]
                    rawPartial = rawPartial[rawPartial['seg_start'] < TimeRng[-1]]

                    for j in TimeRng:
                        try:
                            while TimeToCompareS < j and TIndex < leng:
                                TBlock.append(j)
                                TIndex += 1
                                TimeToCompareS = rawPartial['seg_start'].iloc[TIndex]
                                TimeToCompareE = rawPartial['seg_end'].iloc[TIndex]
                        except Exception:
                            print('outside of the loop')

                    E = list(rawPartial['seg_end'])
                    if len(TBlock) > len(E):
                        TBlock = TBlock[:-1]
                        TBlock[-1] = 'Sep'

                    for w in np.arange(0, len(TBlock), 1):
                        if TBlock[w] != 'Sep':
                            if TBlock[w] < E[w]:
                                TBlock[w] = 'Sep'

                    rawPartial['TBlock'] = TBlock
                    pd.options.mode.chained_assignment = None
                    # for TBCal in categories:
                    s2 = rawPartial.iloc[0]  # could be any value
                    s1 = copy.deepcopy(s2)

                    ParD = copy.deepcopy(rawPartial)
                    Sep = pd.DataFrame()
                    newP = pd.DataFrame()
                    try:
                        for item in np.arange(0,(len(ParD)*2),1):
                            if ParD['TBlock'].iloc[item] != 'Sep':
                                newP = newP.append(ParD.iloc[item,:])
                            elif ParD['TBlock'].iloc[item] == 'Sep':
                                temp = ParD.iloc[item, :]
                                tempM = int(str(TimeRng[0].replace(second=0,hour=0)).split(':')[1])
                                tempET1 = ParD['seg_start'].iloc[item].replace(minute=0) + timedelta(minutes=tempM)
                                tempET2 = tempET1 + timedelta(minutes=60)
                                s1['bi'] = copy.deepcopy(ParD['bi'].iloc[item])
                                s2['bi'] = copy.deepcopy(s1['bi'])
                                s1['seg_start'] = copy.deepcopy(ParD['seg_start'].iloc[item])

                                if ParD['seg_start'].iloc[item] > tempET1 and ParD['seg_start'].iloc[item] < tempET2:
                                    s1['seg_end'] = copy.deepcopy(tempET2)
                                    s1['Var'] = s1['seg_end'] - s1['seg_start']  # Var
                                    s1['TBlock'] = copy.deepcopy(tempET2)   # TBlock
                                    newP = newP.append(s1)
                                    s2['seg_start'] = copy.deepcopy(tempET2) # second start
                                    s2['seg_end'] = temp['seg_end']  # second end
                                    s2['Var'] = s2['seg_end'] - s2['seg_start']
                                    s2['TBlock'] = tempET2 + timedelta(minutes=60)
                                    newP = newP.append(s2)
                                    item += 1

                                elif ParD['seg_start'].iloc[item] < tempET1:
                                    s1['seg_end'] = copy.deepcopy(tempET1) #changed
                                    s1['Var'] = s1['seg_end'] - s1['seg_start']  # Var
                                    s1['TBlock'] = copy.deepcopy(tempET1)  # changed TBlock
                                    newP = newP.append(s1)
                                    s2['seg_start'] = copy.deepcopy(tempET1)  # second start
                                    s2['seg_end'] = temp['seg_end']  # second end
                                    s2['Var'] = s2['seg_end'] - s2['seg_start']
                                    s2['TBlock'] = tempET2
                                    newP = newP.append(s2)
                                    item += 1
                    except:
                        print('aa')

                    newP = newP.sort_values(by='seg_start').set_index(keys=np.arange(0, len(newP), 1))
                    for d in np.arange(0, len(newP), 1):
                        newP.loc[:, "Var"][d] = hms_to_seconds(newP.loc[:, "Var"][d])

                    a = newP.groupby(['TBlock']).sum(axis=1)
                    w = []
                    for l in sorted(set(a.index)):
                        Clu = newP[newP['TBlock'] == l]
                        Clu = Clu.dropna(how='any')

                        if len(Clu) > 0:
                            overall = (Clu[TBCal] * Clu['Var']).sum()
                            totalT = Clu['Var'].sum()
                            weighted_mean = overall / totalT
                            weig_3 = weighted_mean ** (1.0 / 3)
                            w.append(weig_3)
                        else:
                            weig_3 = NaN
                            w.append(weig_3)

                    newData = copy.deepcopy(a[TBCal]).reset_index()
                    newData[TBCal] = w
                    path3 = os.path.join(path2, each_file.split('_')[0]) + '/' + FixFlex + '/'
                    if not os.path.exists(path3):
                        os.makedirs(path3)
                    if e < 700:
                        fname = path3 + each_file + '_' + str(e) + '.csv'
                        fname2 = path3 + each_file + '_' + str(e) + '.pickle'
                    else:
                        fname = path3 + each_file + '_2in5.csv'
                        fname2 = path3 + each_file + '_2in5.pickle'
                    newData.to_csv(fname, sep=',')
                    newData.to_pickle(fname2)
                else:
                    continue