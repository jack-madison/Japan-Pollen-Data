import pandas as pd
import json
import requests
from datetime import datetime
import time

# Specify the list of prefectures and their codes
prefectures = {'Hokkaido': '01', 'Aomori': '02', 'Iwate': '03', 'Miyagi': '04', 'Akita': '05', 'Yamagata': '06',
'Fukushima': '07', 'Ibaraki': '08', 'Tochigi': '09', 'Gunma': '10', 'Saitama': '11', 'Chiba': '12', 'Tokyo': '13',
'Kanagawa': '14', 'Niigata': '15', 'Toyama': '16', 'Ishikawa': '17', 'Fukui': '18', 'Yamanashi': '19', 'Nagano': '20',
'Gifu': '21', 'Shizuoka': '22', 'Aichi': '23', 'Mie': '24', 'Shiga': '25', 'Kyoto': '26', 'Osaka': '27', 'Hyogo': '28',
'Nara': '29', 'Wakayama': '30', 'Tottori': '31', 'Shimane': '32', 'Okayama': '33', 'Hiroshima': '34', 'Yamaguchi': '35',
'Tokushima': '36', 'Kagawa': '37', 'Ehime': '38', 'Kochi': '39', 'Fukuoka': '40', 'Saga': '41', 'Nagasaki': '42', 
'Kumamoto': '43', 'Oita': '44', 'Miyazaki': '45', 'Kagoshima': '46'}

# Create an empty dataframe to write the pollen data to
pollen_data_2021 = pd.DataFrame()

# Create a for loop to pull the 2021 pollen data for each prefecture
for x in prefectures:
    # Specify the url with the code assoicated with x for the TDFKN_CD 
    url = 'https://kafun.env.go.jp/hanako/api/data_search?Start_YM=202101&End_YM=202107&TDFKN_CD=' + str(prefectures[x])
    
    # Request the data
    r = requests.get(url)

    # Format the data in json 
    json = r.json()

    # Write the json data to a dataframe
    df = pd.DataFrame(json)

    # Rename the columns of the new dataframe. The column header names came from https://kafun.env.go.jp/apiManual/apiPage2/api-2-2
    df.columns = ['station code', 'AMeDAS measurement station code', 'Measurement date', 'hour', 'Measuring station name', 
    'Measurement station type', 'Prefecture code', 'Name of prefectures', 'City code', 'City name', 'pollen', 'wind_direction',
    'wind_speed', 'temperature', 'rainfall', 'snowfall_dummy']

    df['prefecture'] = x

    # Write the data to the pollen_data_2021 dataframe
    pollen_data_2021 = pollen_data_2021.append(df)

    # Pause for 5 seconds to avoid overloading the server
    time.sleep(5)

    # Tell the user that the data has been collected for the prefecture
    print(str(x) + ' is done!')

# Tell the user that the for loop is complete
print("For loop complete!")

# Create a dataframe with the unique station names
stations = pd.DataFrame(pollen_data_2021['Measuring station name'].unique())
stations['s_point'] = stations.index + 1
stations.columns = ['Measuring station name', 's_point']

pollen_data_2021 = pd.merge(pollen_data_2021, stations, how = 'left', on = 'Measuring station name')

pollen_data_2021['date'] = pd.to_datetime(pollen_data_2021['Measurement date'])
pollen_data_2021['year'] = pollen_data_2021['date'].dt.year
pollen_data_2021['month'] = pollen_data_2021['date'].dt.month
pollen_data_2021['day'] = pollen_data_2021['date'].dt.day

pollen_data_2021['hour'] = pollen_data_2021['hour'].astype(int)

pollen_data_2021 = pollen_data_2021[['s_point', 'date', 'year', 'month', 'day', 'hour', 'pollen', 'rainfall', 'temperature', 
'wind_direction', 'wind_speed', 'prefecture']]

pollen_data_2021.to_csv('./pollen_data2021.csv')