import pandas
import logging
from datetime import datetime

MODULE_LOGGER = logging.getLogger()

def last_restore_point_filter(file_name,less_column=False, further_filter=True):
    """
    Function to filter csv generated from Get-VBRRestorepoint. 
    It will produce new csv that only contain latest restore point of each VM/Workload.
    By default will generate a csv with the same column outputted from Get-VBRRestorepoint command.
    Can also generate csv with only VM Name and Creation Time column if less_column argument passed as True.
    """
    MODULE_LOGGER.info(f"Will process file: {file_name}")
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
            filtered_data[['VmName', 'CreationTime']].to_csv(f"{file_name}-filtered-less.csv", index=False)
        filtered_data.to_csv(f"{file_name}-filtered-full.csv", index=False)

    except FileNotFoundError as error:
        MODULE_LOGGER.warning("File not found:", error)
        MODULE_LOGGER.info(f"Will skip file: {file_name}")
    except Exception as error:
        MODULE_LOGGER.warning("An unexpected error occurred:", error)
        MODULE_LOGGER.info(f"Will skip file: {file_name}")


if __name__ == "__main__":
    current_time = datetime.now()
    MODULE_LOGGER.setLevel(logging.DEBUG)

    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    stdout_handler.setLevel(logging.INFO)
    MODULE_LOGGER.addHandler(stdout_handler)

    try:
        file_handler = logging.FileHandler(f"log/session-{current_time.strftime('%H.%M-%d-%m-%Y')}.log")
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s : %(message)s'))
        file_handler.setLevel(logging.DEBUG)
        MODULE_LOGGER.addHandler(file_handler)
    except (FileNotFoundError, PermissionError) as error:
        MODULE_LOGGER.warning("File and path related error occured:", error)
        MODULE_LOGGER.info("Program will not log to file")

    MODULE_LOGGER.info(f"Script called directly started at : {current_time.strftime('%H:%M %d-%m-%Y')}")

    file_list = [
        "dataAllPointF1Sby.csv",
        "dataAllPointF2Sby.csv",
        "dataAllPointF1Stl.csv",
        "dataAllPointF2Stl.csv"
        ]
    for file_name in file_list:
        last_restore_point_filter(file_name,True)

    MODULE_LOGGER.info(f"Script finished at : {current_time.strftime('%H:%M %d-%m-%Y')}")