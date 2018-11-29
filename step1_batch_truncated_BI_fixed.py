import sys
sys.path.insert(0, '/Users/daisy/Dropbox/biovital_algorithms')
import os
import json
import datetime
import re
import random
path1 = '/Users/daisy/Dropbox/test_different_time_slot/data_AWS/raw_data/'
path2 = '/Users/daisy/Dropbox/test_different_time_slot/raw_truncated_test/'
# FixFlex = ['fixed_seg','flexible_seg']
FixFlex = ['fixed_seg']

for FixFlex in FixFlex:
    if FixFlex == 'fixed_seg':
        from analyticsengine.vitals.vitals import *
        freq = [1]
        # freq = [2, 3, 5, 10]

    elif FixFlex == 'flexible_seg':
        print(FixFlex)
        from analyticsengine.vitals.vitals_flex import *
        freq = [1]
        # freq = [1, 2, 3, 5, 10]

    files=[]

    for each_file in os.listdir(path1):
        if each_file.endswith('_raw_sample.json'):
            files.append(each_file)

    for each_file in files:
        with open(path1 + '/'+each_file) as f:
            rawData = json.load(f)
        print('read file: ' + each_file)

        rawData = pd.DataFrame.from_dict(rawData)
        rawData = rawData.iloc[:180,:]
        timeseg = [i*60 for i in freq]

        for tiF in timeseg:
            startT = int(rawData[['timestamp']].iloc[0])
            endT = int(rawData[['timestamp']].iloc[-1])
            TimeP = startT
            TimeIn = []

            while TimeP < endT:
                TimeP = TimeP + tiF
                TimeIn.append(TimeP)
                continue

            insideIndex = rawData['timestamp'].isin(TimeIn)
            DatForProc = rawData[insideIndex]
            DatForProc = DatForProc.astype(object).to_dict('records')
            truncated_file = each_file.split('_')[0] + '_truncated_' + str(tiF) + '_raw_sample.json'

            path3 = path2 + each_file.split('_')[0] + '/' + FixFlex + '/'
            if not os.path.exists(os.path.join(path3)):
                os.makedirs(os.path.join(path2, path3))

            truncated_file = each_file.split('_')[0] + 'truncated_' + str(tiF) + '_raw_sample.json'
            pd.DataFrame(DatForProc).to_csv(path3 + truncated_file + '.csv')
            with open(path3 + truncated_file, 'w') as f:
                json.dump(DatForProc, f)

            res_data = []
            baseline_data = []
            timezone = 'UTC+08:00'
            FName = truncated_file.split('_')[1]
            Tseg = str(int(int(FName)/60)) + 'T'
            incr = int(FName)

            new_res_data, output_segments, new_baseline_data, predictions, alarms = process_vitals(DatForProc, res_data,baseline_data, timezone,Tseg,incr)  #, patient_status_test

            for file_to_save in ['new_baseline_data','output_segments','predictions']:
                fileN = re.split('_', each_file)[0] + '_' + FName + '_' + file_to_save
                with open(path3 + fileN +'.json', 'w') as f:
                    json.dump(eval(file_to_save), f)

                vars()[file_to_save] = pd.DataFrame.from_dict(eval(file_to_save))
                eval(file_to_save).sort_values(by='seg_start', inplace=True)

                for sstart_end in ['seg_start','seg_end']:
                    eval(file_to_save)[sstart_end] = pd.to_datetime(eval(file_to_save)[sstart_end], unit='s')
                    eval(file_to_save)[sstart_end] = eval(file_to_save)[sstart_end].dt.tz_localize('UTC').dt.tz_convert(
                        'Asia/Singapore')
                    eval(file_to_save)[sstart_end] = eval(file_to_save)[sstart_end].apply(
                        lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute))

                eval(file_to_save).to_csv(path3+'/'+ fileN +'.csv', index=False)

            bi_output_csv_filename= path3 + re.split('_', each_file)[0] + '_' + FName + '_BI_output.csv'
            bi_output_json_filename = path3 + re.split('_', each_file)[0] + '_' + FName + '_BI_output.json'
            bi_output=pd.merge(output_segments, predictions, how='left', on=['seg_start', 'seg_end'], suffixes=('','_e'))
            bi_output.to_csv(bi_output_csv_filename, index=False)

            bi_output['seg_start'] = pd.DatetimeIndex(bi_output['seg_start']).astype(np.int64) // 10 ** 9 -28800 #tz_localize('UTC').tz_convert('Asia/Singapore').
            bi_output['seg_end'] = pd.DatetimeIndex(bi_output['seg_end']).astype(np.int64) // 10 ** 9 -28800 #.tz_localize('UTC').tz_convert('Asia/Singapore')
            bi_output = bi_output.astype(object).to_dict('records')

            with open(bi_output_json_filename, 'w') as f:
                json.dump(bi_output, f)


