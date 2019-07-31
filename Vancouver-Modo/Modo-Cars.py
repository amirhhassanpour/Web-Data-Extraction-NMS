import requests
import pandas as pd
import datetime
import sqlite3
from apscheduler.schedulers.background import BackgroundScheduler
import telegram 

bot = telegram.Bot(token='token')

def extract():
    conn = sqlite3.connect('vancouver_modo_data.db')
    #datetime
    date_time = datetime.datetime.now()
    year_month = str(date_time.year) + '-' + str(date_time.month)
    day = date_time.day; hour = date_time.hour
    #requesting the data
    try:
        response = requests.get("https://bookit.modo.coop/api/v2/availability")
    except Exception:
        bot.send_message(chat_id = 'chat-id', text = '*** error in vancouver_modo_res.py ***')
    else:
        response = response.json()
        response = response['Response']['Availability']
        data = []
        for x in response:
            a = response[str(x)]['Availability']
            if len(a) > 1:
                data = data + [{'car_id':x,
                                'from':int(a[0]['EndTime']),
                                'to':int(a[1]['StartTime'])}]

        #data cleaning
        reserve = pd.DataFrame(data)

        reserve['day'], reserve['hour'] = [day, hour]
        #storing the data
        reserve.to_sql(year_month + '_reserved_cars', conn, if_exists = 'append', index = False)
        conn.close()
        print(hour)
        bot.send_message(chat_id = 'chat-id', text = str(hour) + ': vancouver_mobi_res')

scheduler = BackgroundScheduler()
scheduler.add_job(extract, 'cron', hour="*")
scheduler.start()
