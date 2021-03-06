# DO ERROR HANDLING OF CONNECTIONS
# find a way to reduce the computation time on the rainfall colour conversion
from urllib.request import urlopen, Request
from urllib.error import URLError
import cv2
import os
import numpy as np
import pandas as pd
import datetime
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import re
from time import sleep
import io


def get_latest_radar_time():

    # connect to the BOM ftp radar address
    input_url = "ftp://ftp.bom.gov.au/anon/gen/radar//"
    input_url = input_url.replace(" ", "%20")
    retry_connect = True
    wait_time = 5
    attempts = 1
    while retry_connect:

        # Error handling for
        try:
            req = Request(input_url)
            req_html = urlopen(req).read()
            retry_connect = False
        except URLError:
            wait_time = min(wait_time * attempts, 60)
            sleep(wait_time)
            print('attempt {}, waiting to retry connection...'.format(attempts))
            attempts += 1

    # parse ftp response string
    soup = str(BeautifulSoup(req_html, 'html.parser').contents[0])
    soup = soup.splitlines()

    # for each line in ftp response, check a transparency image exists
    file_list = []
    # return text which begins with IDR and ends with png
    match_string = 'IDR(.*)png'

    # get the latest radar time
    for strings in soup:
        # use regex and apply match_string
        file_match = re.search(match_string, strings, re.M | re.I)
        if file_match:

            # check that the radar file is the either of the types we want
            components = file_match.group().split('.')
            radar_type = components[0][-1:]

            if radar_type.isdigit() or radar_type == 'I':  # if it is a wind or rain intensity radar
                file_list.append(file_match.group())

    # check each file name and see if it already exists in database
    append_list = []
    for radar_file in file_list:

        components = radar_file.split('.')
        radar_timestamp = int(components[2])  # get the datatime of file

        append_list.append([radar_timestamp])

    # sort the files by date, so that the oldest are downloaded first
    append_list = sorted(append_list)
    latest_time = append_list[-1:][0][0]

    return latest_time


# some fast way to write a pandas DataFrame to PostgreSQL db
def load_to_postgresql(df_to_db, engine, db_name):

    output = io.StringIO()
    df_to_db.to_csv(output, sep='\t', header=False, index=False)
    output.getvalue()
    output.seek(0)

    connection = engine.raw_connection()
    cursor = connection.cursor()
    columns = df_to_db.columns.tolist()
    cursor.copy_from(output, db_name, null="", columns=columns)
    connection.commit()
    cursor.close()


# connect to ftp server and get a list of all available files
def check_radar_updates(start_time_radar, radar_ids):

    # connect to the BOM ftp radar address
    input_url = "ftp://ftp.bom.gov.au/anon/gen/radar//"
    input_url = input_url.replace(" ", "%20")
    retry_connect = True
    wait_time = 5
    attempts = 1
    while retry_connect:

        # Error handling for
        try:
            req = Request(input_url)
            req_html = urlopen(req).read()
            retry_connect = False
        except URLError:
            wait_time = min(wait_time * attempts, 60)
            sleep(wait_time)
            print('attempt {}, waiting to retry connection...'.format(attempts))
            attempts += 1

    # parse ftp response string
    soup = str(BeautifulSoup(req_html, 'html.parser').contents[0])
    soup = soup.splitlines()

    # for each line in ftp response, check a transparency image exists
    file_list = []
    # return text which begins with IDR and ends with png
    match_string = 'IDR(.*)png'

    # get the latest radar time
    for strings in soup:
        # use regex and apply match_string
        file_match = re.search(match_string, strings, re.M | re.I)
        if file_match:

            # check that the radar file is the either of the types we want
            components = file_match.group().split('.')
            radar_type = components[0][-1:]
            radar = components[0][3:5]

            # get the time
            radar_time = int(components[2])

            # check if the file was created after the start_time
            if radar_time >= start_time_radar:

                # check if radar is in list of allowed radars
                if radar in radar_ids:

                    if radar_type.isdigit() or radar_type == 'I':  # if it is a wind or rain intensity radar
                        file_list.append(file_match.group())

    print('Getting new list of {} files at {}'.format(len(file_list), datetime.datetime.time(
            datetime.datetime.now().replace(second=0, microsecond=0))))
    print()
    return file_list


# check all available radar files, return list of files that have not already been downloaded
def check_for_new_radar(radar_file_list, engine):

    # check each file name and see if it already exists in database
    append_list = []
    for radar_file in radar_file_list:
        components = radar_file.split('.')
        radar_timestamp = int(components[2])  # get the datatime of file
        file_df = pd.read_sql_query(r'select * from file_list where file_name = ' + f"'{radar_file}'", con=engine)

        # if file doesn't exist in database, then add it to list
        if file_df.empty:
            append_list.append([radar_file, radar_timestamp])

    # sort the files by date, so that the oldest are downloaded first
    append_list = sorted(append_list, key=lambda x: x[1])
    append_list = [x[0] for x in append_list]

    print('{} to be downloaded...'.format(len(append_list)))
    print()

    return append_list


# download files and save them to the database
def update_radar_db(file_name, rainfall_intensity_df, engine, indx_value, indx_length):

    # get connection to file
    input_url = "ftp://ftp.bom.gov.au/anon/gen/radar//" + file_name
    req = Request(input_url)

    # wrap ftp connection process with error handler
    try:
        req_html = urlopen(req).read()
    except IOError:
        print('Some files no longer exists')
        return ['reset file list']

    # read file
    radar_image = np.fromstring(req_html, np.uint8)             # read byte image
    radar_image = cv2.imdecode(radar_image, cv2.IMREAD_COLOR)   # convert to numpy array

    # switch colours with rain intensity
    radar_df = pd.DataFrame(rainfall_intensity_df.loc[list(map(tuple, pin))].rainfall.values for pin in radar_image)
    radar_df.columns = ['pixel_col_' + str(col) for col in radar_df.columns]

    # test image size
    if radar_df.shape != (512, 512):
        raise ValueError('The shape of the radar image is not 512x512')

    # make adjustments to image
    radar_df = radar_df[:][16:497]          # trim off text
    radar_df['pixel_row'] = radar_df.index  # get pixel row position

    # get radar metadata
    components = file_name.split('.')
    radar_id = components[0]
    radar_year = int(components[2][:4])
    radar_month = int(components[2][4:6])
    radar_day = int(components[2][6:8])
    radar_hour = int(components[2][8:10])
    radar_minute = int(components[2][10:12])
    radar_data_time = datetime.datetime(radar_year, radar_month, radar_day, radar_hour, radar_minute)
    radar_week = ((radar_data_time - datetime.datetime(radar_data_time.year, 1, 1)).days // 7) + 1
    radar_day_of_week = radar_data_time.weekday()

    # load radar metadata
    radar_df['radar_id'] = radar_id
    radar_df['radar_year'] = radar_year
    radar_df['radar_month'] = radar_month
    radar_df['radar_day'] = radar_day
    radar_df['radar_hour'] = radar_hour
    radar_df['radar_minute'] = radar_minute
    radar_df['radar_data_time'] = radar_data_time
    radar_df['radar_week'] = radar_week
    radar_df['radar_day_of_week'] = radar_day_of_week

    # remove rows with no radar data
    remove_index = radar_df.iloc[:, 0:512]
    remove_index = remove_index.dropna(axis=0, how='all')
    remove_index = pd.DataFrame(index=remove_index.index.copy())
    efficient_radar_df = pd.merge(radar_df, remove_index, left_index=True, right_index=True)

    # connect to database and append radar data and file name
    print('{} remaining: Appending {} rows of radar {} captured at {:02}:{:02} on {:02}/{:02}/{}'.format(
        (indx_length-indx_value-1), len(efficient_radar_df), radar_id, radar_hour, radar_minute,
        radar_day, radar_month, radar_year))

    load_to_postgresql(efficient_radar_df, engine, 'radar_reflectivity')  # append radar data

    file_list_df = pd.DataFrame(columns=["file_name"], data=[[file_name]])
    load_to_postgresql(file_list_df, engine, 'file_list')  # append file name

    return None


def main():

    start_time = get_latest_radar_time()

    # connect to database and query if image already exists
    db_engine = create_engine('postgresql://postgres:Postpassword1@localhost:5432/postgres')

    # radar list
    radar_list = ['17', '77', '63', '78', '23', '08', '40', '03', '71', '04', '69', '28', '50', '66']

    # OS agnostic relative file path
    # get the current directory path
    base_dir = os.path.dirname(__file__)

    # OS agnostic relative file path
    # load colour to mm/hr concurrency table
    rainfall_colour_table = os.path.join(os.sep, base_dir, 'sample_data', 'radar_colours.csv')
    rainfall_colour_df = pd.read_csv(rainfall_colour_table)
    rainfall_colour_df.set_index(['B', 'G', 'R'], inplace=True)

    while True:

        # get the list of radar files for rain intensity and wind
        list_of_files = check_radar_updates(start_time, radar_list)

        # get a list of the radar files to download
        update_list = check_for_new_radar(list_of_files, db_engine)
        update = None

        indx_len = len(update_list)

        # load any files that haven't been loaded already
        for indx, file in enumerate(update_list):
            update = update_radar_db(file, rainfall_colour_df, db_engine, indx, indx_len)

            # error handle for missing files
            if update is not None:
                break

        # wait before updating
        if update is None:
            print('waiting to update...')
            print()
            sleep(10)


if __name__ == '__main__':

    main()
