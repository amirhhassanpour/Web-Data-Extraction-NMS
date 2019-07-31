import requests
import pandas as pd
import datetime
import sqlite3
from apscheduler.schedulers.background import BackgroundScheduler
import telegram 

bot = telegram.Bot(token='token')
url = "https://api.citybik.es/v2/networks/mobibikes"

def extract():
    conn = sqlite3.connect('vancouver_mobi_data.db')
    #datetime
    date_time = datetime.datetime.now()
    year_month = str(date_time.year) + '-' + str(date_time.month)
    day = date_time.day; hour = date_time.hour; minute = date_time.minute
    #requesting data
    try:
        response = requests.get(url).json()
    except Exception:
        bot.send_message(chat_id = 'chat-id', text = '*** error in vancouver_mobi.py ***')
    else:
        response = response['network']['stations']
        #data cleaning
        df = pd.DataFrame(response)
        df = pd.concat([df.drop(['extra'], axis=1), df['extra'].apply(pd.Series)], axis=1)
        df['day'], df['hour'], df['minute'] = [day, hour, minute]
        output = df[['day','hour','minute','uid','latitude','longitude','free_bikes','slots']]
        output.rename({'free_bikes': 'num_available_bikes', 'slots': 'number_of_docks'})
        #storing the data
        output.to_sql(year_month, conn, if_exists = 'append', index = False)
        conn.close()
        print(str(hour) + ':' + str(minute))
        if minute == 0:
            bot.send_message(chat_id = 'chat-id', text = str(hour) + ': vancouver_mobi')

scheduler = BackgroundScheduler()
scheduler.add_job(extract, 'cron', minute="*/15")
scheduler.start()
