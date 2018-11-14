import pandas as pd
import copy
import os
import matplotlib.pyplot as plt
import scipy.stats.stats as stats


path1 = '/Users/daisy/Desktop/test_different_time_slot/raw_truncated_test/'
path2 = '/Users/daisy/Dropbox/test_different_time_slot/Report_com/'
file = os.listdir(path2)
FixFlex = ['fixed_seg', 'flexible_seg']
for FixFlex in FixFlex:
    FinalR = []
    for each in file:
        if each.startswith('CAP'):
            print(each)
            contipath = path2 + each + '/flexible_seg/'
            bi_Continuous_hourlyM = pd.read_pickle(
                contipath + each + '_60.pickle')
            allD = copy.deepcopy(bi_Continuous_hourlyM)
            if FixFlex == 'fixed_seg':
                freq = [2, 3, 5, 10, 999]
            else:
                freq = [2, 3, 5, 999]
                # freq = [2]
            timeseg = [i * 60 for i in freq]
            pathN = path2 + each + '/' + FixFlex + '/'
            for timeF in timeseg:
                if timeF < 700:
                    data = pd.read_pickle(pathN + each + '_' + str(timeF) + '.pickle')
                    VarN = 'OneM_every_' + str(timeF) + 's'
                elif timeF > 10000:
                    data = pd.read_pickle(pathN + each + '_2in5.pickle')
                    VarN = 'TwoM_every_5M'
                else:
                    pass
                vars()[VarN] = data
                m = pd.merge(allD, eval(VarN), how='left', left_on='TBlock', right_on='TBlock')
                m = m.set_index(['TBlock'])
                m_drpNA = m.dropna()
                crr = stats.pearsonr(m_drpNA['bi_x'],m_drpNA['bi_y'])
                fig = plt.figure()
                plt.plot(m['bi_x'])
                plt.plot(m['bi_y'])
                fig.savefig(path2 + each + '/' + VarN)
                plt.close(fig)
                Results = [each,crr[0],crr[1],FixFlex,len(m_drpNA),VarN]
                FinalR.append(Results)
                del crr,m,Results,data,VarN,m_drpNA

    FinalR = pd.DataFrame(FinalR)
    FinalR.columns = ['ID', 'r', 'p', 'f','overlapH', 'condition']
    FinalR.to_csv(path2 + FixFlex + 'outputResults.csv')
    FinalR.to_pickle(path2 + FixFlex + 'outputResults.pickle')
