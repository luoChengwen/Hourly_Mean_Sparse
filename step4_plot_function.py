# -*- coding: utf-8 -*-
"""
Created on Tue Feb 13 09:28:48 2018

@author: Biofourmis

Usage:
    plot_function.py <raw_file> <segment_file> <prediction_file> [--mode=<mode>] [--device-type=<device-type>] [(-d | --debug)]
    plot_function.py (-h | --help)
    plot_function.py --version

Options:
    -h --help               Show this screen
    --version               Show version
    --mode=<mode>           User mode or Debug mode, [default: user]
    --device-type=<device-type>     biovotion or vital-connect, [default: biovotion]
    -d --debug              Debug mode, print all args

"""

from datetime import date
import datetime
import time
import pickle
import os
import json
import re
import sys

import matplotlib as mpl
mpl.use('Agg')

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import pytz
# from docopt import docopt

#from .__version__ import __version__



# round time function
def roundTime(dt=None, roundTo=60):
    """Round a datetime object to any time laps in seconds
    dt : datetime.datetime object, default now.
    roundTo : Closest number of seconds to round to, default 1 minute.
    Author: Thierry Husson 2012 - Use it as you want but don't blame me.
    """
    if dt == None:
        dt = datetime.datetime.now()
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds + roundTo / 2) // roundTo * roundTo
    return dt + datetime.timedelta(0, rounding - seconds, -dt.microsecond)


def prepare_data_and_plot(preprocessed, segments, predictions, columns, timezone, mode='user'):
    preprocessed_data = pd.DataFrame.from_dict(preprocessed)
    segments = pd.DataFrame.from_dict(segments)
    predictions = pd.DataFrame.from_dict(predictions)
    output_bi = pd.DataFrame.merge(segments, predictions,
                                   on=['seg_start', 'seg_end', 'label'],
                                   how='outer', suffixes=('', '_e'))

    # fix bug, output_bi may contain a column (bi, hr, rr ...) with full of None
    # need to convert to np.nan for plotting
    return plot_dailyreport(preprocessed_data.replace({None:np.nan}),
                            output_bi.replace({None:np.nan}),
                            columns, timezone, mode)


def plot_seg(seg_time_begin, seg_time_end, y1, y2, ax, label=0):
    """ plot line segments
       seg_time_begin : datetime.datetime object, default now.
       seg_time_end : Closest number of seconds to round to, default 1 minute.
       y1 : real value from segments
       y2 : predicted value from segments
       ax : axis property
   """
    if label == 0:
        # real value show in blue  ,lw=2
        x1 = ax.hlines(y1, seg_time_begin, seg_time_end,
                       linestyle='solid', colors='blue')
        # estimate value show in orange   ,lw=2
        x2 = ax.hlines(y2, seg_time_begin, seg_time_end,
                       linestyle='solid', colors='orange')
        ax.fill_between([seg_time_begin, seg_time_end], y1, y2,where=y2 >= y1, color='red') # estimate value > real value shade area in red
        ax.fill_between([seg_time_begin, seg_time_end], y1, y2,where=y2 <= y1, color='green')  # real value > estimate value shade area in green
    else:  # junk data detected
        # junk data real value show in red  ,lw=2
        x1 = ax.hlines(y1, seg_time_begin, seg_time_end,
                       linestyle='solid', colors='red')
        # estimate value show in orange   ,lw=2
        x2 = ax.hlines(y2, seg_time_begin, seg_time_end,
                       linestyle='solid', colors='orange')

    return x1, x2


def plot_dailyreport(preprocessed_data, output_bi, columns, timezone, mode='user'):
    """ plot daily report
           preprocessed_data : raw measurement data
           output_bi: biovital index computed output data
           each_file: name of each id on filename
           mode: default to user mode, accept other value like: debug
   """

    preprocessed_data = preprocessed_data[columns]
    preprocessed_data['timestamp'] = pd.to_datetime(
        preprocessed_data['timestamp'], unit='s')  # Minghao
    preprocessed_data['timestamp'] = preprocessed_data['timestamp'].dt.tz_localize(
        'UTC').dt.tz_convert(timezone)  # Minghao
    preprocessed_data['timestamp'] = preprocessed_data['timestamp'].apply(
        lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute))  # Minghao

    output_bi['seg_start'] = pd.to_datetime(
        output_bi['seg_start'], unit='s')  # Minghao
    output_bi['seg_start'] = output_bi['seg_start'].dt.tz_localize(
        'UTC').dt.tz_convert(timezone)  # Minghao
    # output_bi['seg_start'] = output_bi['seg_start'].apply(
    #    lambda dt: datetime.datetime.fromtimestamp(dt, pytz.timezone('UTC')))
    output_bi['seg_start'] = output_bi['seg_start'].apply(lambda dt: datetime.datetime(
        dt.year, dt.month, dt.day, dt.hour, dt.minute))  # Minghao

    output_bi['seg_end'] = pd.to_datetime(
        output_bi['seg_end'], unit='s')  # Minghao
    output_bi['seg_end'] = output_bi['seg_end'].dt.tz_localize(
        'UTC').dt.tz_convert(timezone)  # Minghao
    output_bi['seg_end'] = output_bi['seg_end'].apply(lambda dt: datetime.datetime(
        dt.year, dt.month, dt.day, dt.hour, dt.minute))  # Minghao
    # output_bi['seg_end'] = output_bi['seg_end'].apply(
    #    lambda dt: datetime.datetime.fromtimestamp(dt, pytz.timezone('UTC')))

    #output_bi.loc[:, 'bi'] *= 1.6

    #preprocessed_data = preprocessed_data[['timestamp', 'act', 'hr', 'rr', 'pos']]
    #output_bi = output_bi[['seg_start', 'seg_end', 'act', 'hr', 'rr','pos', 'bi']]
    #output_bi = output_bi[['seg_start', 'seg_end', 'act', 'hr', 'rr', 'act_e', 'hr_e', 'rr_e', 'pos', 'bi']]
    preprocessed_data = preprocessed_data.reindex(columns=(
        ['timestamp'] + list([a for a in preprocessed_data.columns if a != 'timestamp'])))
    if 'label' in preprocessed_data.columns:
        preprocessed_data = preprocessed_data.reindex(columns=(
            list([a for a in preprocessed_data.columns if a != 'label']) + ['label']))
    #output_bi = output_bi[['seg_start','seg_end','act','hr','rr','bpw','act_e','hr_e','rr_e','bpw_e', 'bi']]
    output_bi = output_bi.reindex(columns=(['seg_start', 'seg_end'] + list(
        [a for a in output_bi.columns if a != 'seg_start' and a != 'seg_end'])))
    output_bi = output_bi.reindex(
        columns=(list([a for a in output_bi.columns if a != 'bi']) + ['bi']))

   # output_bi.to_csv(os.path.join(db_path, 'data_output', each_file+'_BI.csv'), index = False)

    preprocessed_data = preprocessed_data.dropna(thresh=4)

    rawname_list = preprocessed_data.columns.tolist()
    name_list = output_bi.columns.tolist()

    # preprocessed_data[rawname_list[0]]=pd.to_datetime(preprocessed_data[rawname_list[0]])


    day = list(preprocessed_data[rawname_list[0]].apply(
        lambda dt: roundTime(datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute),
                             roundTo=60 * 60 * 24)))  # round time to date

    def date_to_str(x): return x.strftime('%Y-%m-%d')
    day = [date_to_str(x) for x in day]  # receive list of days string
    pd.options.mode.chained_assignment = None
    #pp = PdfPages('report/' + each_file + 'test_report.pdf')
    figs = []
    ds = sorted(list(set(day)))  # get all days
    for i in range(len(ds)):
        min_t = datetime.datetime.fromtimestamp(
            int(time.mktime(time.strptime(ds[i], "%Y-%m-%d"))) - 12 * 60 * 60)
        # min_t.strftime('%Y-%m-%d %H:%M:%S')
        max_t = datetime.datetime.fromtimestamp(
            int(time.mktime(time.strptime(ds[i], "%Y-%m-%d"))) + 12 * 60 * 60)
        # max_t.strftime('%Y-%m-%d %H:%M:%S')
        # index=np.where(np.array(day)==ds[i])
        # raw_tmp=preprocessed_data.iloc[index[0].tolist()]#extract raw data in selected range
        # raw_tmp = raw_tmp.set_index('timestamp', drop=False)
        # raw_tmp=raw_tmp.resample(Tseg).mean()
        # raw_tmp['timestamp']=raw_tmp.index

        raw_tmp = preprocessed_data.loc[
            (preprocessed_data['timestamp'] >= min_t) & (preprocessed_data['timestamp'] <= max_t)]
        # raw_tmp = raw_tmp.set_index('timestamp', drop=False)
        raw_tmp.set_index('timestamp', drop=False, inplace=True)
        raw_tmp = raw_tmp.resample(Tseg).mean()
        raw_tmp.loc[:, 'timestamp'] = raw_tmp.index

        output_bi_temp = output_bi.loc[(output_bi[name_list[1]] >= min_t) & (
            output_bi[name_list[0]] <= max_t)]  # extract output data in selected range
        # print(ds[i], min_t, max_t)
        # print(raw_tmp.head(5))
        # print(output_bi_temp.head(5))

        # (ax1, ax2, ax3,ax4, ax5)
        if 'label' in preprocessed_data.columns:
            num_plots = len(preprocessed_data.columns)-1
        else:
            num_plots = len(preprocessed_data.columns)
        fig, axes = plt.subplots(
            num_plots, sharex=False, figsize=(10, 10))  # sharex=False,
        plt.suptitle(ds[i])
        for j in range(num_plots):
            ax = axes[j]
            ax.set_xlim([min_t - datetime.timedelta(0, 3600),
                         max_t + datetime.timedelta(0, 3600)])
            xticks1 = [min_t, min_t + datetime.timedelta(0, 3600 * 6), min_t + datetime.timedelta(0, 3600 * 12),
                       max_t - datetime.timedelta(0, 3600 * 6), max_t]
            xticks2 = ["12PM", "6PM", "12AM", "6AM", "12PM"]
            ax.set_xticks(xticks1)
            ax.set_xticklabels(xticks2)
            # ax.xaxis.set_tick_params(rotation=0)
            if j == num_plots - 1:
                ax.set_xlabel('Time')

            cdict = {'red': ((0.0, 0.0, 0.0),
                             (0.5, 0.0, 0.0),
                             (1.0, 1.0, 1.0)),
                     'blue': ((0.0, 0.0, 0.0),
                              (1.0, 0.0, 0.0)),
                     'green': ((0.0, 0.0, 1.0),
                               (0.5, 0.0, 0.0),
                               (1.0, 0.0, 0.0))}
            cmap = mpl.colors.LinearSegmentedColormap(
                'my_colormap', cdict, 100)

            if j == 0:  # plot BI
                ax.set_ylabel('Biovital Index')
                ax.set_ylim([0.0, 1.0])
                ax.set_yticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
                ax.set_yticklabels([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
                for k in range(len(output_bi_temp)):
                    bi_y = min(output_bi_temp[name_list[-1]].iloc[k], 1)
                    # - datetime.timedelta(0, 60)
                    seg_time_begin = max(output_bi_temp[name_list[0]].iloc[k], min_t)
                    seg_time_end = min(output_bi_temp[name_list[1]].iloc[k], max_t)
                    ax.fill_between([seg_time_begin, seg_time_end], [bi_y, bi_y],
                                    color=cmap(
                                        output_bi_temp[name_list[-1]].iloc[k]),
                                    edgecolor="b",
                                    linewidth=0.0)
                '''draw painlevel
                
                painrecord = pd.read_csv(db_path + '/output_v3/' + each_file + '_painrecord.csv', parse_dates = ['timestamp'], dayfirst = True)
                for k in range(len(painrecord)):
                    if painrecord['PainIntensity'].iloc[k] != 0 and not np.isnan(painrecord['PainIntensity'].iloc[k]):
                        seg_time_begin = painrecord['timestamp'].iloc[k]
                        seg_time_end = seg_time_begin + datetime.timedelta(0, int(60 * max(painrecord['PainLast'].iloc[k], 15)))
                        y1 = painrecord['PainIntensity'].iloc[k]
                        if y1/10 <= 0.5:
                            ax.hlines(y1/10, seg_time_begin, seg_time_end,
                                                   linestyle = 'solid', colors = 'orange')
                        else:
                            ax.hlines(y1/10, seg_time_begin, seg_time_end,
                                                   linestyle = 'solid', colors = 'red')
                end
                '''
            elif (rawname_list[j].lower() == 'spo2'):
                ax.set_ylabel(rawname_list[j].upper())
                ax.set_ylim([70, 100])
                ax.plot(raw_tmp[rawname_list[0]], raw_tmp[rawname_list[j]],
                            linestyle='--', color='grey', linewidth=0.3)



            elif rawname_list[j].lower() == 'pos':  # plot pos
                # continue
                ax.set_ylim([127.5, 131.5])
                yticks1 = [128, 129, 130, 131]
                yticks2 = ["Unknown", "Standing", "Leaning", "lying"]
                ax.set_yticks(yticks1)
                ax.set_yticklabels(yticks2)
                '''
                raw_tmp.loc[raw_tmp[rawname_list[j]] >131, rawname_list[j]] = 128
                raw_tmp.loc[raw_tmp[rawname_list[j]] < 129, rawname_list[j]] = 128

                if len(raw_tmp[rawname_list[j]][raw_tmp[rawname_list[j]] == 129])!=0:
                    raw_tmp[rawname_list[j]][raw_tmp[rawname_list[j]] == 129].plot(style='r.', ax=ax)
                if len(raw_tmp[rawname_list[j]][raw_tmp[rawname_list[j]] == 130])!=0:
                    raw_tmp[rawname_list[j]][raw_tmp[rawname_list[j]] == 130].plot(style='b.', ax=ax)
                if len(raw_tmp[rawname_list[j]][raw_tmp[rawname_list[j]] == 131])!=0:
                    raw_tmp[rawname_list[j]][raw_tmp[rawname_list[j]] == 131].plot(style='g.', ax=ax)
                if len(raw_tmp[rawname_list[j]][raw_tmp[rawname_list[j]] == 128])!=0:
                    raw_tmp[rawname_list[j]][raw_tmp[rawname_list[j]] == 128].plot(style='k.', ax=ax)
                '''

                output_bi_temp.loc[output_bi_temp.pos > 131, 'pos'] = 128
                output_bi_temp.loc[output_bi_temp.pos < 129, 'pos'] = 128

                if len(output_bi_temp['pos'][output_bi_temp['pos'] == 129]) != 0:
                    tmp_129 = output_bi_temp.loc[output_bi_temp['pos'] == 129]
                    for line in range(len(tmp_129)):
                        # - datetime.timedelta(0, 60)
                        seg_time_begin = tmp_129['seg_start'].iloc[line]
                        seg_time_end = tmp_129['seg_end'].iloc[line]
                        pos = tmp_129['pos'].iloc[line]
                        ax.hlines(pos, seg_time_begin, seg_time_end,
                                  linestyle='solid', colors='red', lw=2)

                if len(output_bi_temp['pos'][output_bi_temp['pos'] == 130]) != 0:
                    tmp_130 = output_bi_temp.loc[output_bi_temp['pos'] == 130]
                    for line in range(len(tmp_130)):
                        # - datetime.timedelta(0, 60)
                        seg_time_begin = tmp_130['seg_start'].iloc[line]
                        seg_time_end = tmp_130['seg_end'].iloc[line]
                        pos = tmp_130['pos'].iloc[line]
                        ax.hlines(pos, seg_time_begin, seg_time_end,
                                  linestyle='solid', colors='blue', lw=2)

                if len(output_bi_temp['pos'][output_bi_temp['pos'] == 131]) != 0:
                    tmp_131 = output_bi_temp.loc[output_bi_temp['pos'] == 131]
                    for line in range(len(tmp_131)):
                        # - datetime.timedelta(0, 60)
                        seg_time_begin = tmp_131['seg_start'].iloc[line]
                        seg_time_end = tmp_131['seg_end'].iloc[line]
                        pos = tmp_131['pos'].iloc[line]
                        ax.hlines(pos, seg_time_begin, seg_time_end,
                                  linestyle='solid', colors='green', lw=2)

                if len(output_bi_temp['pos'][output_bi_temp['pos'] == 128]) != 0:
                    tmp_128 = output_bi_temp.loc[output_bi_temp['pos'] == 128]
                    for line in range(len(tmp_128)):
                        # - datetime.timedelta(0, 60)
                        seg_time_begin = tmp_128['seg_start'].iloc[line]
                        seg_time_end = tmp_128['seg_end'].iloc[line]
                        pos = tmp_128['pos'].iloc[line]
                        ax.hlines(pos, seg_time_begin, seg_time_end,
                                  linestyle='solid', colors='black', lw=2)

            else:  # plot 'act','hr','rr','ts'.....
                ax.set_ylabel(rawname_list[j].upper())
                if rawname_list[j].lower() == 'act':
                    ax.set_ylim([0, 50])
                elif rawname_list[j].lower() == 'bpw':
                    ax.set_ylim([0, preprocessed_data[rawname_list[j]].max() + 0.5])
                    #ax.set_ylabel('PP.VOLV'.upper())
                elif rawname_list[j].lower() == 'ts':
                    ax.set_ylabel('hrv'.upper())
                elif rawname_list[j].lower() == 'rr':
                    ax.set_ylim([0, preprocessed_data[rawname_list[j]].max()])
                else:
                    ax.set_ylim([preprocessed_data[rawname_list[j]].min(
                    ), preprocessed_data[rawname_list[j]].max()])
                if 'label' in raw_tmp.columns and mode == 'debug':
                    raw_tmp_nj = raw_tmp.loc[raw_tmp['label'] == 0]
                    raw_tmp_j = raw_tmp.loc[raw_tmp['label'] == 1]
                    raw_tmp_nj = raw_tmp_nj.set_index('timestamp', drop=False)
                    raw_tmp_nj = raw_tmp_nj.resample(Tseg).mean()
                    raw_tmp_nj['timestamp'] = raw_tmp_nj.index
                    raw_tmp_j = raw_tmp_j.set_index('timestamp', drop=False)
                    raw_tmp_j = raw_tmp_j.resample(Tseg).mean()
                    raw_tmp_j['timestamp'] = raw_tmp_j.index

                    ax.plot(raw_tmp_nj[rawname_list[0]], raw_tmp_nj[rawname_list[j]],
                            linestyle='--', color='grey', linewidth=0.3)
                    ax.plot(raw_tmp_j[rawname_list[0]], raw_tmp_j[rawname_list[j]],
                            linestyle='--', color='red', linewidth=1.5)

                else:
                    ax.plot(raw_tmp[rawname_list[0]], raw_tmp[rawname_list[j]],
                            linestyle='--', color='grey', linewidth=0.3)
#                    ax.plot(raw_tmp[rawname_list[0]], raw_tmp[rawname_list[j]],
#                            '.', color='red')
                for k in range(len(output_bi_temp)):
                    # - datetime.timedelta(0, 60)
                    seg_time_begin = max(output_bi_temp[name_list[0]].iloc[k], min_t)
                    seg_time_end = min(output_bi_temp[name_list[1]].iloc[k], max_t)
                    y1 = output_bi_temp[rawname_list[j]].iloc[k]
                    y2 = output_bi_temp[rawname_list[j] + '_e'].iloc[k]
                    if 'label' in output_bi_temp.columns and mode == 'debug':
                        label = output_bi_temp['label'].iloc[k]
                        [xblue, xorange] = plot_seg(
                            seg_time_begin, seg_time_end, y1, y2, ax, label=label)
                    else:
                        [xblue, xorange] = plot_seg(
                            seg_time_begin, seg_time_end, y1, y2, ax)
#                if mode == 'user' and len(output_bi_temp) != 0:
#                    ax.legend([xblue, xorange], ["segment value",
#                                                 "segment prediction value"],loc='center left', bbox_to_anchor=(1, 0.5))        # print(i)
        # pp.savefig(fig)
        figs.append(fig)
        plt.close(fig)
    return figs

# save to pdf


def savepdf(output_file_path, figs):
    pp = PdfPages(output_file_path)
    for fig in figs:
        pp.savefig(fig)
    pp.close()


def main():
    '''
    '''
    args = docopt(__doc__, version=__version__)

    if args['--debug']:
        print(args)
        sys.exit()

    if args['--device-type'] == 'biovotion':
        raw_path = args['<raw_file>']
        segment_path = args['<segment_file>']
        prediction_path = args['<prediction_file>']

        try:
            with open(raw_path) as f:
                raw_data = json.load(f)

            with open(segment_path) as f:
                segment_data = json.load(f)

            with open(prediction_path) as f:
                prediction_data = json.load(f)

            columns = ['timestamp', 'hr', 'act', 'rr', 'bpw', 'label']
            timezone = 'Asia/Singapore'
            mode = 'user'

            figs = prepare_data_and_plot(raw_data, segment_data, prediction_data, columns, timezone, mode=mode)
            output_file_path = '_tmp_report.pdf'
            savepdf(output_file_path, figs)
        except FileNotFoundError as e:
            print(e)

    else:
        print('SORRY, currently, only support biovotion data')
        sys.exit()


if __name__ == '__main__':
    # biovotion data


    timezone='Asia/Singapore'

    # 7984
    db_path = "/Users/daisy/Dropbox/test_different_time_slot/raw_truncated_test/CAP001058/flexible_seg/"
    columns=['timestamp','hr','act','rr']#,'ts','bpw','spo2','bpw'
    # timezone='America/Los_Angeles'
    Tseg =[]
    file_list = []
    for each_file in os.listdir(db_path):#
        if each_file.endswith('.json'):
            c = each_file.split('.')[0]
            # if c.find('_2in5raw') == -1:
            if c.find('_60_BI_output')!= -1:
                file_list.append(each_file)

    file_list = list(set(file_list))



    for each_file in file_list:
        try:
            f = db_path + each_file.split('_')[0]+ 'truncated_' + each_file.split('_')[1]

            with open(f +'_raw_sample.json') as f:
                preprocessed_data = json.load(f)
        except:
            print('file not found', f + '_raw_sample.json')
            continue

        try:
            with open(db_path + each_file) as f:  # _raw_sample
                output_bi = json.load(f)
        #                for i in range(len(preprocessed_data)):
        #                    preprocessed_data[i]['hr'] = preprocessed_data[i].pop('bpm')
        #                    preprocessed_data[i]['rr'] = preprocessed_data[i].pop('brpm')

        except:
            # print('file not found, ', os.path.join(db_path,each_file+'_raw_sample.json'))
            continue
        print("pause")

        # try:
        #     with open(os.path.join(db_path, 'data_output', each_file + '_segments.json')) as f:
        #         segments = json.load(f)
        # except:
        #     #print('file not found, ', os.path.join(db_path,'output',each_file+'_segments.json'))
        #     continue
        # try:
        #     with open(os.path.join(db_path, 'data_output', each_file + '_prediction.json')) as f:
        #         predictions = json.load(f)
        # except:
        #     #print('file not found, ', os.path.join(db_path,'output',each_file+'_prediction.json'))
        #     continue
        preprocessed_data = pd.DataFrame.from_dict(preprocessed_data)
        output_bi = pd.DataFrame.from_dict(output_bi)
        # #preprocessed_data = preprocessed_data.rename(columns={'ts': 'delta', 'bpw': 'pp_volv'})
        # segments = pd.DataFrame.from_dict(segments)
        # predictions = pd.DataFrame.from_dict(predictions)
        #
        # # biovotion data
        # output_bi = pd.DataFrame.merge(segments, predictions,
        #                                on=['seg_start', 'seg_end'],# 'label'],
        #                                how='outer', suffixes=('', '_e'))
        print(each_file)
        if each_file.startswith('truncated_'):
            T = int(each_file.split('_')[1])
            Tseg = str(int(T/60)) + 'T'
        else:
            Tseg = '1T'

        mode = 'user'  # "Please enter report version: 'user' or 'debug'"

        figs = plot_dailyreport(
            preprocessed_data, output_bi, columns, timezone)

        if not os.path.exists(os.path.join(db_path, 'data_output', 'report')):
            os.makedirs(os.path.join(db_path, 'data_output', 'report'))

        output_file_path = db_path + each_file + '_report.pdf'

        savepdf(output_file_path, figs)
