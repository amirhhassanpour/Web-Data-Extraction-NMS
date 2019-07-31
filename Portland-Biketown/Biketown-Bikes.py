import sqlite3
import urllib.request
import json
import datetime
import pandas as pd
import pathlib
from apscheduler.schedulers.background import BackgroundScheduler
import telegram 

bot = telegram.Bot(token='token')
data_dock_add = 'http://biketownpdx.socialbicycles.com/opendata/station_status.json'
data_free_add = 'http://biketownpdx.socialbicycles.com/opendata/free_bike_status.json'
def extract():
    conn = sqlite3.connect('biketown_data.db')
    #getting time and date
    date_time = str(datetime.datetime.now())
    year = date_time[0:4]
    month = date_time[5:7]
    year_month = year + '_' + month
    day_no = datetime.datetime.now().day
    hour = datetime.datetime.now().hour
    minute = datetime.datetime.now().minute
    #requesting urls
    try:
        data_dock = json.loads(urllib.request.urlopen(data_dock_add).read().decode())
        data_free = json.loads(urllib.request.urlopen(data_free_add).read().decode())
    except Exception:
        bot.send_message(chat_id = 'chat-id', text = '*** error in portland_biketown.py ***')
    else:
        #data cleaning
        data_dock = data_dock['data']['stations']
        data_free = data_free['data']['bikes']
        #converting list to dataframe
        df_data_dock = pd.DataFrame.from_dict(data_dock, orient='columns', dtype=None)
        df_data_free = pd.DataFrame.from_dict(data_free, orient='columns', dtype=None)
        #adding day and time of the day data
        df_data_dock['day'] = day_no
        df_data_dock['hour'] = hour
        df_data_dock['minute'] = minute    
        df_data_free['day'] = day_no
        df_data_free['hour'] = hour
        df_data_free['minute'] = minute
        #sorting the columns
        df_data_dock = df_data_dock[['day','hour','minute','station_id','num_bikes_available','num_docks_available']]
        df_data_free = df_data_free[['day','hour','minute','bike_id','lat','lon']]
        #storing the data
        df_data_dock.to_sql('stations_' + year_month, conn, if_exists = 'append', index = False)
        df_data_free.to_sql('freebikes_' + year_month, conn, if_exists = 'append', index = False)
        conn.close()
        print(str(hour) + ':' + str(minute))
        if minute == 0:
            bot.send_message(chat_id = 'chat-id', text = str(hour) + ': portland_biketown')

scheduler = BackgroundScheduler()
scheduler.add_job(extract, 'cron', minute="*/15")
scheduler.start()
