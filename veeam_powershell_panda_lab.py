import pandas
import logging
from datetime import datetime

MODULE_LOGGER = logging.getLogger()

def last_restore_point_filter(file_name,less_column=True, further_filter=True):
    '''
    Function to filter csv generated from Get-VBRRestorepoint. 
    It will produce new csv that only contain latest restore point of each VM/Workload.
    By default will generate a csv with the same column outputted from Get-VBRRestorepoint command.
    Can also generate csv with only VM Name and Creation Time column if less_column argument passed as True.
    '''
    MODULE_LOGGER.info('Will process file: \n%s',file_name)
    try:
        data_frame = pandas.read_csv(file_name)
        data_frame['CreationTime'] = pandas.to_datetime(data_frame['CreationTime'])
        data_frame.sort_values(by='CreationTime', ascending=False, inplace=True)
        data_frame.drop_duplicates(subset='VmName', keep='first', inplace=True)
        data_frame.sort_values(by='VmName', ascending=True, inplace=True)
        data_frame.reset_index(drop=True, inplace=True)
        if further_filter == True:
            filtered_data = data_frame.loc[(data_frame['CreationTime'] >= '2023-01-01') & (data_frame['CreationTime'] <= '2023-08-01')]
        else:
            filtered_data = data_frame

        filtered_data.sort_values(by='VmName', ascending=True, inplace=True)
        filtered_data.reset_index(drop=True, inplace=True)
        if less_column == True:
            return filtered_data[['VmName', 'CreationTime']]
            # .to_csv(f'{file_name}-filtered-less.csv', index=False)
        else:
            return filtered_data
            # .to_csv(f'{file_name}-filtered-full.csv', index=False)
    except FileNotFoundError as error:
        MODULE_LOGGER.warning('File not found: \n%s', error)
        MODULE_LOGGER.info('Will skip file: \n%s', file_name)
        return None
    except Exception as error:
        MODULE_LOGGER.warning('An unexpected error occurred: \n%s', error)
        MODULE_LOGGER.info('Will skip file: \n%s', file_name)
        return None

def filtered_restore_point_merge(dataframe_list):
    dataframes_list = []
    
    for file in dataframe_list:
        if file != None:
            MODULE_LOGGER.info('Will process file: \n%s',file)
            dataframe = pandas.read_csv(file)
            dataframe['SourceFile'] = file
            dataframes_list.append(dataframe)
    
    combined_dataframe = pandas.concat(dataframes_list, ignore_index=True)
    # .to_csv(f'restore-point-per-{datetime.now().strftime("%H.%M-%d-%m-%Y")}-combined.csv', index=False)
    return combined_dataframe

if __name__ == '__main__':
    current_time = datetime.now()
    MODULE_LOGGER.setLevel(logging.DEBUG)

    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    stdout_handler.setLevel(logging.INFO)
    MODULE_LOGGER.addHandler(stdout_handler)

    try:
        file_handler = logging.FileHandler(f'log/session-{current_time.strftime("%H.%M-%d-%m-%Y")}.log')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s : %(message)s'))
        file_handler.setLevel(logging.DEBUG)
        MODULE_LOGGER.addHandler(file_handler)
    except (FileNotFoundError, PermissionError) as error:
        MODULE_LOGGER.warning('File and path related error occured: \n%s', error)
        MODULE_LOGGER.info('Program will not log to file')
    except Exception as error:
        MODULE_LOGGER.warning('Unexpected error occured: \n%s', error)
        MODULE_LOGGER.info('Program will not log to file')

    MODULE_LOGGER.info('Script called directly started at: %s', current_time.strftime('%H:%M %d-%m-%Y'))

    data_list = [
        last_restore_point_filter('dataAllPointF1Sby.csv'),
        last_restore_point_filter('dataAllPointF2Sby.csv'),
        last_restore_point_filter('dataAllPointF1Stl.csv'),
        last_restore_point_filter('dataAllPointF2Stl.csv')
        ]
    filtered_restore_point_merge(data_list).to_csv(f'restore-point-per-{datetime.now().strftime("%H.%M-%d-%m-%Y")}-combined.csv', index=False)


    MODULE_LOGGER.info('Script finished at : %s', current_time.strftime('%H:%M %d-%m-%Y'))