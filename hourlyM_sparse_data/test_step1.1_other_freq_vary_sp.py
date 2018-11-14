import sys

sys.path.insert(0, "/Users/daisy/Dropbox/biovital_algorithms")
import os
import json
import random

path1 = '/Users/daisy/Dropbox/test_different_time_slot/data_AWS/raw_data/'
path2 = '/Users/daisy/Dropbox/test_different_time_slot/raw_truncated/changeSP/'
incrment = 120
fre = 5 * 60
Tseg = '5T'

FixFlex = ['flexible_seg','fixed_seg']
for FixFlex in FixFlex:
    if FixFlex == 'flexible_seg':
        from analyticsengine.vitals.vitals_flex import *
    else:
        from analyticsengine.vitals.vitals import *
    files = []
    for each_file in os.listdir(path1):
        if each_file.endswith('_raw_sample.json'):
            files.append(each_file)
            print('Find input file: ' + each_file)

    for each_file in files:
        with open(path1 + '/' + each_file) as f:
            rawData = json.load(f)
        print('read file: ' + each_file)

        rawData = pd.DataFrame.from_dict(rawData)
        rawData2 = rawData.set_index('timestamp', drop=False)
        TStamp = list(rawData['timestamp'])
        randT = random.randint(1, 10)
        start = TStamp[randT]
        endt = TStamp[-1]
        extract_2in5 = []
        new_Data = pd.DataFrame()

        try:
            i = randT
            while i < len(rawData):
                temp1 = rawData.iloc[i, :]
                new_Data = new_Data.append(temp1)
                temp2 = rawData.iloc[i + 1, :]
                new_Data = new_Data.append(temp2)
                i += 5

        except Exception as e:
            print(e)

        DatForProc = new_Data.astype(object).to_dict('records')
        truncated_file = each_file.split('_')[0] + '_2in5_raw_sample.json'
        path3 = path2 + each_file.split('_')[0] + '/' + FixFlex + '/'
        new_Data.to_csv(path3 + truncated_file + '.csv')
        with open(path3 + truncated_file, 'w') as f:
            json.dump(DatForProc, f)
        '''
        step 2 use the test_function
        '''
        incr = fre
        res_data = []
        baseline_data = []
        timezone = 'UTC+08:00'
        new_res_data, output_segments, new_baseline_data, predictions, alarms = process_vitals(DatForProc, res_data,
                                                                                               baseline_data, timezone,
                                                                                               Tseg, incr)
        fname = truncated_file
        seg_json_filename = fname + '_segments.json'
        baseline_json_filename = fname + '_baseline.json'
        prediction_json_filename = fname + '_prediction.json'
        path3 = path2 + each_file.split('_')[0] + '/' + FixFlex + '/'
        with open(path3 + seg_json_filename, 'w') as f:
            json.dump(output_segments, f)
        with open(path3 + baseline_json_filename, 'w') as f:
            json.dump(new_baseline_data, f)
        with open(path3 + prediction_json_filename, 'w') as f:
            json.dump(predictions, f)

        baseline_csv_filename = fname + '_baseline.csv'
        prediction_csv_filename = fname + '_prediction.csv'
        seg_csv_filename = fname + '_segments.csv'
        bi_output_csv_filename = path3 + fname + '_BI_output.csv'
        bi_output_json_filename = path3 + fname + '_BI_output.json'

        output_segments = pd.DataFrame.from_dict(output_segments)
        output_segments.sort_values(by='seg_start', inplace=True)

        output_segments['seg_start'] = pd.to_datetime(output_segments['seg_start'], unit='s')
        output_segments['seg_start'] = output_segments['seg_start'].dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Singapore')
        output_segments['seg_start'] = output_segments['seg_start'].apply(
            lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute))
        output_segments['seg_end'] = pd.to_datetime(output_segments['seg_end'], unit='s')
        output_segments['seg_end'] = output_segments['seg_end'].dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Singapore')
        output_segments['seg_end'] = output_segments['seg_end'].apply(
            lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute))
        output_segments.to_csv(path3 + seg_csv_filename, index=False)

        predictions = pd.DataFrame.from_dict(predictions)
        predictions.sort_values(by='seg_start', inplace=True)
        # predictions['seg_start']=predictions['seg_start'].apply(lambda dt: datetime.datetime.fromtimestamp(dt, pytz.timezone('UTC')))
        # predictions['seg_end']=predictions['seg_end'].apply(lambda dt: datetime.datetime.fromtimestamp(dt, pytz.timezone('UTC')))
        predictions['seg_start'] = pd.to_datetime(predictions['seg_start'], unit='s')
        predictions['seg_start'] = predictions['seg_start'].dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Singapore')
        predictions['seg_start'] = predictions['seg_start'].apply(
            lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute))
        predictions['seg_end'] = pd.to_datetime(predictions['seg_end'], unit='s')
        predictions['seg_end'] = predictions['seg_end'].dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Singapore')
        predictions['seg_end'] = predictions['seg_end'].apply(
            lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute))
        predictions.to_csv(path3 + prediction_csv_filename, index=False)

        # predictions = predictions.drop(['pos'], axis=1)
        bi_output = pd.merge(output_segments, predictions, how='left', on=['seg_start', 'seg_end'], suffixes=('', '_e'))
        bi_output.to_csv(bi_output_csv_filename, index=False)

        bi_output['seg_start'] = pd.DatetimeIndex(bi_output['seg_start']).astype(
            np.int64) // 10 ** 9 - 28800  # tz_localize('UTC').tz_convert('Asia/Singapore').
        bi_output['seg_end'] = pd.DatetimeIndex(bi_output['seg_end']).astype(
            np.int64) // 10 ** 9 - 28800  # .tz_localize('UTC').tz_convert('Asia/Singapore')
        bi_output = bi_output.astype(object).to_dict('records')
        with open(bi_output_json_filename, 'w') as f:
            json.dump(bi_output, f)

        new_baseline_data = pd.DataFrame.from_dict(new_baseline_data)
        new_baseline_data.sort_values(by='seg_start', inplace=True)

        new_baseline_data['seg_start'] = pd.to_datetime(new_baseline_data['seg_start'], unit='s')
        new_baseline_data['seg_start'] = new_baseline_data['seg_start'].dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Singapore')
        new_baseline_data['seg_start'] = new_baseline_data['seg_start'].apply(
            lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute))
        new_baseline_data['seg_end'] = pd.to_datetime(new_baseline_data['seg_end'], unit='s')
        new_baseline_data['seg_end'] = new_baseline_data['seg_end'].dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Singapore')
        new_baseline_data['seg_end'] = new_baseline_data['seg_end'].apply(
            lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute))
        # new_baseline_data.to_csv(path2+'/'+baseline_csv_filename, index=False)
