import numpy as np
import os
import re
import json
from matplotlib import gridspec
from numpy import trapz
import pandas as pd
import re
import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from itertools import chain
from matplotlib.ticker import FuncFormatter
# bi_merge.groupby('hour').mean()
# def(data):
#     tmp = pd.DataFrame()
#     tmp['timestamp'] = np.array(data[:, 0])
#     tmp['condition'] = np.array(data[:, 1])
#     tmp['bi'] = np.array(data[:, 2])
#     tmp.groupby('timestamp').mean()
path="/Users/daisy/Desktop/test_different_time_slot/ROC/hourly_data/"
path2 = "/Users/daisy/Desktop/test_different_time_slot/ROC/"
# files = []
#
# for each_file in os.listdir(path):
#     if each_file.startswith('CAP001') and each_file.endswith('hour.csv'):
#         files.append(each_file)
#         print('Find input file: ' + each_file)
# forder=[1,3,5,10,15,30]
# filen=[]
# for i in forder:
#     if i<5:
#         e=  'flexible' + str(i) + 'T_BI_output_1hour.csv'
#         filen.append(e)
#         e= 'fixed' + str(i) + 'T_BI_output_1hour.csv'
#         filen.append(e)
#     else:
#         e = 'fixed' + str(i) + 'T_BI_output_1hour.csv'
#         filen.append(e)
#
# for m in filen:
#     file_list = []
#     combined_data=[]
#     count=0
#     for each_file in files:
#         if each_file.endswith(m):
#             print(each_file)
#             file_list.append(each_file)
#             data=pd.read_csv(path + each_file)
#             if count==0:
#                 combined_data=np.array(data[['hour','condition','hourly_bi']])
#                 count+=1
#             else:
#                 combined_data = np.vstack((combined_data,np.array(data[['hour','condition','hourly_bi']])))
#     vars() [m.split('.')[0]+'_list'] = file_list
#     vars() [m.split('.')[0]+'_data'] = combined_data
#
#
# ordered_files=[]
#
# participantID = set([x.split('_')[0] for x in files])
#
# ordered_files = []
# for i in forder:
#     for ind in participantID:
#         if i<5:
#             e= 'CAP001' + ind + '_flexible' + str(i) + 'T_BI_output_1hour.csv'
#             ordered_files.append(e)
#             e= 'CAP001' + ind + '_fixed' + str(i) + 'T_BI_output_1hour.csv'
#             ordered_files.append(e)
#         else:
#             e = 'CAP001' + ind + '_fixed' + str(i) + 'T_BI_output_1hour.csv'
#             ordered_files.append(e)
#     fig = plt.figure()
#     tmp_legend = []
#     plt.subplot(111)
#         # plt.style.use('thirtyeightseven')
#     for each_file in ordered_files:
#         print(each_file)
#         data = pd.read_csv(path + each_file)
#         plt.plot(pd.to_datetime(data.hour),data.hourly_bi)
#         l = each_file.split('_')[0]+'_'+each_file.split('_')[1]
#         tmp_legend += ['%s' % l]
#     startT = data.loc[data.condition==1,'hour'].values[0]
#     endT = data.loc[data.condition==1,'hour'].values[-1]
#     plt.axvline(x=startT,ls="--")
#     plt.axvline(x=endT,ls="--")
#     plt.legend(tmp_legend)
#     plt.xlabel("time")
#     plt.ylabel("bi_index")
#     plt.savefig(path+ 'hourly_bi_plot/' + each_file.split('_')[0]+'_hourlybi.png', dpi=300)
#     plt.close(fig)



participantID=['004','057','003','021','024','014']
# ordered_files=[]
# ordered_files2=[]
forder=[1,3,5,10,15,30]
new_ordered=[]
fig = plt.figure()
with PdfPages(path + 'pdftest.pdf') as pdf:
    for ind in participantID:
        ordered_files = []
        ordered_files2 = []
        for i in forder:
            if i<5:
                # print(i)
                e= 'CAP001' + ind + '_flexible' + str(i) + 'T_BI_output_1hour.csv'
                print(e)
                ordered_files.append(e)
                e= 'CAP001' + ind + '_fixed' + str(i) + 'T_BI_output_1hour.csv'
                ordered_files.append(e)

                f = 'CAP001' + ind + '_flexible' + str(i) + 'T_ROC_bi.csv'
                ordered_files2.append(f)
                f = 'CAP001' + ind + '_fixed' + str(i) + 'T_ROC_bi.csv'
                ordered_files2.append(f)
            else:
                e = 'CAP001' + ind + '_fixed' + str(i) + 'T_BI_output_1hour.csv'
                ordered_files.append(e)

                f = 'CAP001' + ind + '_fixed' + str(i) + 'T_ROC_bi.csv'
                ordered_files2.append(f)


        new_ordered =ordered_files + ordered_files2

        tmp_legend = []
        fig = plt.figure()
        gs = gridspec.GridSpec(2, 1, height_ratios=[3, 1])
        # plt.subplot(111)
        #
        # fig2 = plt.figure()
        # plt.subplot(111)
        #
        #
        # plt.style.use('thirtyeightseven')
        for each_file in new_ordered:

            if len(each_file)> 35:
                data = pd.read_csv(path + each_file)
                print(each_file)
                ax0=plt.subplot(gs[0])
                # plt.plot(pd.to_datetime(data.hour), data.hourly_bi)
                ax0.plot(pd.to_datetime(data.hour), data.hourly_bi)
                l = each_file.split('_')[0] + '_' + each_file.split('_')[1]
                tmp_legend += ['%s' % l]
                startT = data.loc[data.condition == 1, 'hour'].values[0]
                endT = data.loc[data.condition == 1, 'hour'].values[-1]
            # plt.legend(tmp_legend)
            # plt.xlabel("time")
            # plt.ylabel("bi_index")
            # pdf.savefig(fig)
            # plt.close(fig).plot(pd.to_datetime(data.hour), data.hourly_bi)
            #
            elif len(each_file)< 32:
                data = pd.read_csv(path2+ each_file)
                print(each_file)
                ax1=plt.subplot(gs[1])
                ax1.plot(data['FP'], data['TP'], '-x', linewidth=1.5, markersize=2, )
                area = trapz(y=data['TP'][::-1], x=data['FP'][::-1])
                bi_thres = data.loc[data['area'].idxmax(), 'bi_criteria']
                a = each_file.split('_')[0] + each_file.split('_')[1]
                tmp_legend += ['ROC curve (BI) (AUC = %0.2f) %s' % (area, a)]


        ax0.axvline(x=startT,ls="--")
        ax0.axvline(x=endT,ls="--")
        plt.legend(tmp_legend)
        # plt.xlabel("time")
        # plt.ylabel("bi_index")
        pdf.savefig(fig)
        plt.close(fig)
    # plt.savefig(path+ 'hourly_bi_plot

    #
    #     # plt.style.use('thirtyeightseven')
    #     for each_file in ordered_files:
    #         print(each_file)
    #         data = pd.read_csv(path + each_file)
    #         plt.plot(pd.to_datetime(data.hour),data.hourly_bi)
    #         l = each_file.split('_')[0]+'_'+each_file.split('_')[1]
    #         tmp_legend += ['%s' % l]
    #     startT = data.loc[data.condition==1,'hour'].values[0]
    #     endT = data.loc[data.condition==1,'hour'].values[-1]
    #     plt.axvline(x=startT,ls="--")
    #     plt.axvline(x=endT,ls="--")
    #     plt.legend(tmp_legend)
    #     plt.xlabel("time")
    #     plt.ylabel("bi_index")
    #     pdf.savefig(fig)
    #     plt.close(fig)
    # # plt.savefig(path+ 'hourly_bi_plot/' + each_file.split('_')[0]+'_hourlybi.pdf', dpi=300)
