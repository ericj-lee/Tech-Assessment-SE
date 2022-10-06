import os
import argparse
from consumption_data_processor import ConsumptionDataProcessor
from operating_hour_estimator import OperatingHourEstimator


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('nmi_file', type=str,
                        help='''NMI info file name in csv format.
                                eg. nmi_info.csv'''
                        )
    parser.add_argument('data_folder', type=str,
                        help='''Folder name of consumpton data files.
                                eg. ConsumptionData'''
                        )
    parser.add_argument('processed_folder', type=str,
                        help='''Folder name for processed consumption data.
                                eg. processed_data'''
                        )
    args = parser.parse_args()
    try:
        processed_folder = args.processed_folder + '/'
        cdp = ConsumptionDataProcessor(args.nmi_file,
                                       args.data_folder,
                                       processed_folder)
        cdp.process()
        with os.scandir(processed_folder) as it:
            for data_file in it:
                if data_file.name.endswith(".csv") and data_file.is_file():
                    ohe = OperatingHourEstimator(processed_folder,
                                                 data_file.name)
                    if ohe is not None:
                        ohe.run_estimation()
    except FileNotFoundError as e:
        print(f'{e.args[1]}: {e.filename}')
    except ValueError as e:
        print(e)
    except Exception as e:
        print('Unexpected error:  ', e)
