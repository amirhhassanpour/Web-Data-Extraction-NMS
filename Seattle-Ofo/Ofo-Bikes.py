import pyofo
import ast
import pandas as pd
import datetime
import sqlite3
from apscheduler.schedulers.background import BackgroundScheduler
import telegram 

bot = telegram.Bot(token='token')

#credentials for the ofo website
pyofo.set_token('ofo-token')
ofo = pyofo.Ofo()
#reading the points on the city
points = pd.read_csv('measurment_points_seattle.csv')
def extract():
    conn = sqlite3.connect('data.db')
    #date_time
    date_time = datetime.datetime.now()
    year_month = str(date_time.year) + '-' + str(date_time.month)
    day = date_time.day; hour = date_time.hour
    #creating an empty dataframe for our output
    output = pd.DataFrame({'carno': [],'bomNum': [],'userIdLast': [], 'lng': [],'lat': [],
                           'o_lng': [],'o_lat': [], 'point_id': []}, index=[])
    number_of_bikes = []
    try:
        #iterating through points and gathering data from ofo website
        for index, row in points.iterrows():
            #getting the data
            r = ofo.nearby_ofo_car(lat=row[1], lng=row[2])
            print(index)
            r = ast.literal_eval(r.text)
            r = r['values']['cars']
            df = pd.DataFrame.from_dict(r, orient='columns', dtype=None)
            df['point_id'] = row[0]
            output = pd.concat([output, df], sort=False)
    except Exception:
        bot.send_message(chat_id = 'chat-id', text = '*** error in seattle_ofo.py ***')
        print('error!')
    else:
        #data cleaning
        output['day'] = day; output['hour'] = hour
        bikes = pd.DataFrame()
        bikes = output[['day', 'hour', 'carno', 'lat', 'lng', 'point_id']]
        #writing out the output all bikes with their coordinates
        bikes = bikes.drop_duplicates(subset=['carno'], keep='first')
        bikes.to_sql(year_month, conn, if_exists = 'append', index = False)
        conn.close()
        print(hour)
        bot.send_message(chat_id = 'chat-id', text = str(hour) + ': seattle_ofo')

scheduler = BackgroundScheduler()
scheduler.add_job(extract, 'cron', hour='*/2')
scheduler.start()
