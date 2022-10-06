import os
import shutil
import pytz
import pandas as pd


class ConsumptionDataProcessor:
    '''
    This class is used to process consumption data into the format
    required by OperatingHourEstimator
    '''
    states = {'NSW', 'VIC', 'QLD', 'WA'}
    nmi_info_col = {'Nmi', 'State', 'Interval'}
    expected_cols = {'AESTTime', 'Quantity', 'Unit'}
    allowed_units = {'KWH', 'MWH'}
    intervals = {15.0, 30.0}
    tz_lookup = {
        'NSW': pytz.timezone('Australia/NSW'),
        'QLD': pytz.timezone('Australia/Queensland'),
        'VIC': pytz.timezone('Australia/Victoria'),
        'WA': pytz.timezone('Australia/West')
    }

    def __init__(self, nmi_info_file, consumption_data_folder,
                 processed_folder):
        print('Consumption data processing started.')
        self.processed_folder = processed_folder
        self.conumption_data_folder = consumption_data_folder
        try:
            self.nmi_info = pd.read_csv(nmi_info_file)
            col_diff = self.nmi_info_col.difference(set(self.nmi_info.columns.values.tolist()))
            if len(col_diff) > 0:
                raise ValueError('Class initialzation failed: NMI info is missing columns', col_diff)
        except ValueError as e:
            raise ValueError(e)
        except FileNotFoundError as e:
            print(f'{e.args[1]}: {e.filename}')

    def process(self):
        try:
            shutil.rmtree(self.processed_folder, ignore_errors=True)
            for row in self.nmi_info.sort_values('Nmi').itertuples():
                nmi = row.Nmi
                state = row.State
                interval = row.Interval
                print('\nProcessing NMI: ' + nmi)
                if state.upper() not in self.states:  #
                    print(' #Not a valid state: ', state, ". NMI skipped")
                    continue
                if interval not in self.intervals:
                    print(' #Not a valid interval: ', interval, ". NMI skipped")
                    continue
                file_name = os.path.join(self.conumption_data_folder,
                                         nmi + '.csv')
                if not os.path.isfile(file_name):
                    print(' #No data for NMI: ', nmi, ". NMI skipped")
                    continue
                df = pd.read_csv(file_name)
                col_diff = self.expected_cols.difference(
                    set(df.columns.values.tolist()))
                if len(col_diff) > 0:
                    print(' -Consumption data does not have columns', col_diff)
                    return None
                df = df.dropna(axis=0, how='any')
                df = df.drop_duplicates().drop_duplicates(
                    subset='AESTTime', keep=False)
                df = df[df['Unit'].str.upper().isin(self.allowed_units)]
                df = self.add_datetime_column(df)
                df = self.remove_incomplete_days(df, interval)
                df = self.standardize_units(df)
                df = self.add_datetimes_with_timezone(df, state)
                if df is None:
                    print(' -Consumption data processing failed')
                    continue
                df = df.drop(['AESTTime_dt_tz', 'LocalTime_dt_tz'], axis=1)
                self.write_df_to_csv(df, nmi, state)
                print(' -File created successfully')
            print('\nConsumption data processing completed.\n')
        except ValueError as e:
            print(e)
        except IndexError:
            print('No data left to continue processing')

    def write_df_to_csv(self, df, nmi, state):
        if not os.path.isdir(self.processed_folder):
            os.mkdir(self.processed_folder)
        df.to_csv(self.processed_folder + nmi + '_' + state + '.csv',
                  index=False)

    def add_datetime_column(self, df):
        try:
            df['AESTTime_dt'] = pd.to_datetime(df['AESTTime'],
                                               format='%Y-%m-%d %H:%M:%S'
                                               )
        except ValueError:
            pass
        else:
            return df
        try:
            df['AESTTime_dt'] = pd.to_datetime(df['AESTTime'],
                                               format='%d/%m/%Y %H:%M:%S'
                                               )
        except ValueError:
            raise ValueError(' -AESTTime is not in the correct format') from None
        return df

    def remove_incomplete_days(self, df, interval):
        freq = str(int(interval)) + 'min'
        datetime_list = pd.date_range(df['AESTTime_dt'].min().date(),
                                      df['AESTTime_dt'].max().date(),
                                      freq=freq)
        missing_dt = set(datetime_list.difference(df['AESTTime_dt']).date)
        df = df[~df['AESTTime_dt'].dt.date.isin(missing_dt)]
        if missing_dt != set():
            print(f' -Removed {len(missing_dt)} incomplete days')
        return df

    def standardize_units(self, df):
        if 'mwh' in [s.lower() for s in df.Unit.unique()]:
            df['Quantity'] = df.apply(lambda x: x.Quantity * 1000.0
                                      if x.Unit.lower() == 'mwh'
                                      else x.Quantity, axis=1)
            df['Unit'] = df['Unit'].map(lambda x: 'kwh'
                                        if x.lower() == 'mwh' else x)
        return df

    def add_datetimes_with_timezone(self, df, state):
        df['AESTTime_dt_tz'] = df['AESTTime_dt'].dt.tz_localize(
            'Australia/Queensland')
        df['LocalTime_dt_tz'] = df['AESTTime_dt_tz'].dt.tz_convert(
            self.tz_lookup[state])
        df['LocalTime_dt'] = df['LocalTime_dt_tz'].dt.strftime(
            '%Y-%m-%d %H:%M:%S') 
        return df
