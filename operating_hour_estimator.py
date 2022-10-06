import pandas as pd
from collections import Counter
from datetime import datetime


class OperatingHourEstimator:
    '''
    OperatingHourEstimator class is used to estimate business hours for a given NMI.
    Estimation is done at day level, and the most common pattern is used as the final result.

    '''
    def __init__(self, processed_folder, file_name):
        try:
            nmi = file_name.split('_')[0]
            print('\nOperating hour estimation for ' + nmi)
            self.df = pd.read_csv(processed_folder + file_name)
            self.df['AESTTime_dt'] = pd.to_datetime(self.df['AESTTime_dt'],
                                                    format='%Y-%m-%d %H:%M:%S')
            self.df['LocalTime_dt'] = pd.to_datetime(self.df['LocalTime_dt'],
                                                     format='%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print('Error reading data file for NMI : ', nmi)
            print(e)
            return None

    def run_estimation(self, start_date='1990-01-01', end_date='2025-12-31'):
        date_list = self.df['AESTTime_dt'].dt.date.unique()
        start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        date_list = date_list[(date_list >= start_date_dt) &
                              (date_list <= end_date_dt)]
        estimates = []
        for _date in date_list:
            estimate = self.single_day_estimation(_date)
            if estimate is not None:
                estimates.append(estimate)
        est_by_freq = Counter(estimates)
        print(f'Data used for estimation: {len(estimates)} days')
        print(f'Business hours estimated: {est_by_freq.most_common(1)[0][0]}')
        print(f'{est_by_freq.most_common(1)[0][1]} days had same pattern')

    def single_day_estimation(self, date_estimate):
        threshold = 0.35  # Normalized quantity to be used as threshold to determine business start/finish time
        df = self.df[self.df['AESTTime_dt'].dt.date == date_estimate].copy()
        # Days where consumption quantity does not vary much will be ignored
        if df['Quantity'].max() / df['Quantity'].mean() < 1.5:
            return None
        df['AESTDate'] = df['AESTTime_dt'].dt.date
        # Normalize quantity to 1 for each day
        grouper = df.groupby('AESTDate')['Quantity']                                                                             
        maxes = grouper.transform('max')
        mins = grouper.transform('min')
        df = df.assign(Quantity_Normalized=(df.Quantity - mins)/(maxes - mins))
        df_ = df[df['Quantity_Normalized'] > threshold]
        if df_.empty:
            return None
        start_time = df_.iloc[0]['LocalTime_dt']
        df = df[df['LocalTime_dt'] >= start_time]
        df = df[df['Quantity_Normalized'] < threshold]
        if df.empty:
            return None
        end_time = df.iloc[0]['LocalTime_dt']
        if (end_time - start_time).total_seconds()/(60*60) < 5.0:
            return None
        return start_time.strftime('%H:%M:%S') + ' to ' + \
            end_time.strftime('%H:%M:%S')