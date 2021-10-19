import pandas as pd
import numpy as np
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

# Create the list of stations and their s_point values that were used in the 2019 and 2020 data
stations_list = [['北海道立衛生研究所', 1], ['北海道渡島総合振興局', 2], ['北海道上川総合振興局', 3],
['北海道十勝総合振興局', 4], ['青森市中央卸売市場', 5], ['国立大学法人弘前大学', 6],
['岩手県環境保健研究センター', 7], ['大船渡地区合同庁舎', 8], ['東北大学医学部', 9],
['宮城県東部地方振興事務所', 10], ['秋田県健康環境センター千秋庁舎', 11], ['秋田県平鹿地域振興局福祉環境部', 12],
['山形県衛生研究所', 13], ['置賜保健所', 14], ['庄内総合支庁分庁舎', 15],
['福島県衛生研究所', 16], ['株式会社江東微生物研究所 東北中央研究所', 17], ['水戸石川一般環境大気測定局', 18],
['日立市消防本部', 19], ['国立研究開発法人 国立環境研究所', 20], ['宇都宮市中央生涯学習センター', 21],
['栃木県那須庁舎', 22], ['日光市役所第４庁舎', 23], ['群馬県衛生環境研究所', 24],
['館林保健福祉事務所', 25], ['さいたま市役所', 26], ['熊谷市保健センター', 27],
['飯能市役所', 28], ['東邦大学', 29], ['千葉県環境研究センター', 30],
['印旛健康福祉センター成田支所', 31], ['君津市糠田測定局', 32], ['東京都多摩小平保健所', 33],
['新宿区役所第二分庁舎', 34], ['神奈川県庁第二分庁舎', 35], ['川崎生命科学・環境研究センター', 36],
['神奈川県環境科学センター', 37], ['新潟県保健環境科学研究所', 38], ['長岡環境センター', 39],
['上越環境センター', 40], ['富山県庁舎', 41], ['富山県魚津総合庁舎', 42],
['金沢大学医学部附属病院', 43], ['石川県能登中部保健福祉センター', 44], ['福井市福井大気汚染測定局', 45],
['二州健康福祉センター', 46], ['山梨県衛生環境研究所', 47], ['山梨県身延合同庁舎', 48],
['長野県環境保全研究所安茂里庁舎', 49], ['長野県飯田合同庁舎', 50], ['長野県松本合同庁舎', 51],
['大垣市民病院', 52], ['岐阜県飛騨総合庁舎', 53], ['静岡県庁本庁舎屋上', 54],
['静岡県東部総合庁舎（沼津財務事務所)', 55], ['伊東市役所', 56], ['愛知県環境調査センター', 57],
['愛知県東三河総合庁舎', 58], ['三重県立総合医療センター', 59], ['三重県庁', 60],
['彦根地方気象台', 61], ['滋賀県琵琶湖・環境科学研究センター', 62], ['滋賀県高島合同庁舎', 63],
['京都府立医科大学', 64], ['舞鶴市西コミュニティセンター', 65], ['京都市右京区役所京北合同庁舎', 66],
['大阪合同庁舎第2号館別館', 67], ['豊中市役所第一庁舎', 68], ['泉大津市役所', 69],
['兵庫県立健康科学研究所', 70], ['北山緑化植物園（西宮市都市整備公社）', 71], ['太子町役場', 72],
['兵庫県環境研究センター', 73], ['奈良県産業振興総合センター', 74], ['奈良県吉野保健所', 75],
['橿原総合庁舎', 76], ['和歌山地方気象台', 77], ['和歌山県西牟婁振興局庁舎', 78],
['和歌山県東牟婁振興局庁舎', 79], ['鳥取県庁西町分庁舎', 80], ['鳥取県中部総合事務所', 81],
['島根県保健環境科学研究所', 82], ['浜田保健所', 83], ['岡山県備中県民局井笠地域事務所', 84],
['岡山県美作県民局真庭地域事務所', 85], ['岡山大学医学部', 86], ['広島県立総合技術研究所保健環境センター', 87],
['広島県東部建設事務所 三原支所', 88], ['光市立大和総合病院', 89], ['山口大学医学部附属病院', 90],
['山口県環境保健センター（葵庁舎）', 91], ['徳島県立保健製薬環境センター', 92], ['南部総合県民局', 93],
['香川県庁舎本館', 94], ['香川県中讃保健福祉事務所', 95], ['新居浜市役所', 96],
['愛媛大学農学部', 97], ['宇和島市役所庁舎', 98], ['高知県保健衛生総合庁舎', 99],
['幡多福祉保健所', 100], ['小倉医師会北九州中央臨床検査センター', 101], ['福岡県久留米市中央浄化センター', 102],
['田川市立病院', 103], ['佐賀県環境センター', 104], ['唐津市役所', 105],
['佐賀県武雄総合庁舎', 106], ['長崎大学病院', 107], ['健康保険諫早総合病院検査部', 108],
['長崎県県北振興局', 109], ['熊本市医師会ヘルスケアセンター', 110], ['国立水俣病総合研究センター', 111],
['休暇村 南阿蘇', 112], ['大分県南部振興局', 113], ['大分大学医学部', 114],
['大分県農林水産研究指導センター林業研究部', 115], ['延岡保健所', 116], ['宮崎県庁', 117],
['鹿児島県環境保健センター', 118], ['鹿児島県姶良・伊佐地域振興局伊佐庁舎', 119], ['鹿児島県大隅地域振興局本庁舎', 120]
]

# Create a dataframe from the list of station names and s_point values
station_df = pd.DataFrame(stations_list, columns = ['Measuring station name', 's_point'])

# Replace the string '\u3000' in one of the station names as it causes errors
pollen_data_2021['Measuring station name'] = pollen_data_2021['Measuring station name'].str.replace('\u3000', ' ')

# Merge the pollen data for 2021 with the station numbers following the assigned values used when collecting the 2019 and 2020 data
pollen_data_2021 = pd.merge(pollen_data_2021, station_df, how = 'left', on = 'Measuring station name')

# Set the conditions to create the region variable
conditions = [
    (pollen_data_2021['prefecture'] == 'Hokkaido'),
    (pollen_data_2021['prefecture'] == 'Aomori') | (pollen_data_2021['prefecture'] == 'Iwate') | (pollen_data_2021['prefecture'] == 'Miyagi') | (pollen_data_2021['prefecture'] == 'Akita') | (pollen_data_2021['prefecture'] == 'Yamagata') | (pollen_data_2021['prefecture'] == 'Fukushima'),
    (pollen_data_2021['prefecture'] == 'Ibaraki') | (pollen_data_2021['prefecture'] == 'Tochigi') | (pollen_data_2021['prefecture'] == 'Gunma') | (pollen_data_2021['prefecture'] == 'Saitama') | (pollen_data_2021['prefecture'] == 'Chiba') | (pollen_data_2021['prefecture'] == 'Tokyo') | (pollen_data_2021['prefecture'] == 'Kanagawa'),
    (pollen_data_2021['prefecture'] == 'Niigata') | (pollen_data_2021['prefecture'] == 'Toyama') | (pollen_data_2021['prefecture'] == 'Ishikawa') | (pollen_data_2021['prefecture'] == 'Fukui') | (pollen_data_2021['prefecture'] == 'Yamanashi') | (pollen_data_2021['prefecture'] == 'Nagano') | (pollen_data_2021['prefecture'] == 'Gifu') | (pollen_data_2021['prefecture'] == 'Shizuoka') | (pollen_data_2021['prefecture'] == 'Aichi'),
    (pollen_data_2021['prefecture'] == 'Mie') | (pollen_data_2021['prefecture'] == 'Shiga') | (pollen_data_2021['prefecture'] == 'Kyoto') | (pollen_data_2021['prefecture'] ==  'Osaka') | (pollen_data_2021['prefecture'] == 'Hyogo') | (pollen_data_2021['prefecture'] == 'Nara') | (pollen_data_2021['prefecture'] == 'Wakayama'),
    (pollen_data_2021['prefecture'] == 'Tottori') | (pollen_data_2021['prefecture'] == 'Shimane') | (pollen_data_2021['prefecture'] == 'Okayama') | (pollen_data_2021['prefecture'] == 'Hiroshima') | (pollen_data_2021['prefecture'] == 'Yamaguchi'),
    (pollen_data_2021['prefecture'] == 'Tokushima') | (pollen_data_2021['prefecture'] == 'Kagawa') | (pollen_data_2021['prefecture'] == 'Ehime') | (pollen_data_2021['prefecture'] == 'Kochi'),
    (pollen_data_2021['prefecture'] == 'Fukuoka') | (pollen_data_2021['prefecture'] == 'Saga') | (pollen_data_2021['prefecture'] == 'Nagasaki') | (pollen_data_2021['prefecture'] == 'Kumamoto') | (pollen_data_2021['prefecture'] == 'Oita') | (pollen_data_2021['prefecture'] == 'Miyazaki') | (pollen_data_2021['prefecture'] == 'Kagoshima')
    ]

# Set the values for the region variable
values = ['Hokkaido', 'Tohoku', 'Kanto', 'Chubu', 'Kansai', 'Chugoku', 'Shikoku', 'Kyushu']

# Create the region variable based on the conditions and values
pollen_data_2021['region'] = np.select(conditions, values)    

# Format the date to be in line with the 2019 and 2020 data that has already been collected
pollen_data_2021['date'] = pd.to_datetime(pollen_data_2021['Measurement date'])
pollen_data_2021['year'] = pollen_data_2021['date'].dt.year
pollen_data_2021['month'] = pollen_data_2021['date'].dt.month
pollen_data_2021['day'] = pollen_data_2021['date'].dt.day

# Change the data type of the hour and s_point variables
pollen_data_2021['hour'] = pollen_data_2021['hour'].astype(int)
pollen_data_2021['s_point'] = pollen_data_2021['s_point'].astype(int)

# Remove unneeded columns
pollen_data_2021 = pollen_data_2021[['s_point', 'date', 'year', 'month', 'day', 'hour', 'pollen', 'rainfall', 'temperature', 
'wind_direction', 'wind_speed', 'region']]

# Subset the data to just Hokkaido so that the data for Feb to June can be extracted
hokkaido = pollen_data_2021[pollen_data_2021['region'] == 'Hokkaido']
hokkaido = hokkaido[(hokkaido['date'] >= '2021-02-01') & (hokkaido['date'] <= '2021-06-30')]

# Subset the data to exclude Hokkaido so that the data for Feb to May can be extracted
rest_of_japan = pollen_data_2021[pollen_data_2021['region'] != 'Hokkaido']
rest_of_japan = rest_of_japan[(rest_of_japan['date'] >= '2021-02-01') & (rest_of_japan['date'] <= '2021-05-31')]

# Append the two subsets together again
pollen_data_2021 = hokkaido.append(rest_of_japan)

# Save the data as a csv to be used later
pollen_data_2021.to_csv('./pollen_data2021.csv', index=False)