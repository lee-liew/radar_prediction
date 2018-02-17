from urllib.request import urlopen, Request
import cv2
import os
import numpy as np
import pandas as pd
import datetime
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import re

def check_radar_updates():

    # connect to the BOM ftp radar address
    input_url = "ftp://ftp.bom.gov.au/anon/gen/radar//"
    input_url = input_url.replace(" ", "%20")
    req = Request(input_url)
    req_html = urlopen(req).read()

    # parse ftp response string
    soup = str(BeautifulSoup(req_html, 'html.parser').contents[0])
    soup = soup.splitlines()

    # for each line in ftp response, check a transparency image exists
    file_list = []
    # return text which begins with IDR and ends with png
    match_string = 'IDR(.*)png'
    for strings in soup:
        # use regex and apply match_string
        file_match = re.search(match_string, strings, re.M|re.I)
        if file_match:
            file_list.append(file_match.group())

    return file_list

# do the check first in a seperate function then run all radars which need updating
def update_radar_db(file_name, rainfall_intensity_df):

    # connect to database and query if image already exists
    engine = create_engine('postgresql://postgres:Postpassword1@localhost:5432/postgres')
    file_df = pd.read_sql_query(r'select * from file_list where file_name = ' + f"'{file_name}'", con=engine)

    # if image does not exist, load it inside database
    if file_df.empty:

        input_url = "ftp://ftp.bom.gov.au/anon/gen/radar//" + file_name
        req = Request(input_url)

        # wrap ftp connection process with error handler
        try:
            req_html = urlopen(req).read()
        except IOError:
            print('Connection lost to file')
            return

        radar_image = np.fromstring(req_html, np.uint8)             # read byte image
        radar_image = cv2.imdecode(radar_image, cv2.IMREAD_COLOR)   # convert to numppy array

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
        radar_data_time = datetime.datetime(radar_year, radar_month, radar_day, radar_hour,
                                                 radar_minute)
        radar_week = ((radar_data_time - datetime.datetime(radar_data_time.year, 1,
                                                                     1)).days // 7) + 1
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
        print('Appending image for radar {} at {:02}:{:02} on {:02}/{:02}/{}'.format(
            radar_id, radar_hour, radar_minute, radar_day, radar_month, radar_year))
        engine = create_engine('postgresql://postgres:Postpassword1@localhost:5432/postgres')
        efficient_radar_df.to_sql('radar_reflectivity', engine, if_exists='append', index=False)  # append radar data
        file_list_df = pd.DataFrame(columns=["file_name"], data=[[file_name]])
        file_list_df.to_sql('file_list', engine, if_exists='append', index=False)  # append file name


# OS agnostic relative file path
# get the current directory path
dir = os.path.dirname(__file__)

# OS agnostic relative file path
# load colour to mm/hr concurrency table
rainfall_colour_table = os.path.join(os.sep, dir, 'sample_data', 'radar_colours.csv')
rainfall_colour_df = pd.read_csv(rainfall_colour_table)
rainfall_colour_df.set_index(['B', 'G', 'R'], inplace=True)

# get the list of radar files
list_of_files = check_radar_updates()


for file in list_of_files:
    update_radar_db(file, rainfall_colour_df)

tester = 1
