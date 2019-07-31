from lyft_rides.auth import ClientCredentialGrant
from lyft_rides.client import LyftRidesClient

from uber_rides.session import Session
from uber_rides.client import UberRidesClient

import geopy.distance
import pandas as pd
import numpy as np
import datetime
import sqlite3
import random
from apscheduler.schedulers.background import BackgroundScheduler

#opening necessary files
points = pd.read_csv('measurment_points_seattle.csv')
lat = points['latitude'].values; lon = points['longitude'].values; point_id = points['point_id'].values; 
#number of points
nop = count = len(points)

#authentication lyft
auth_flow = ClientCredentialGrant('token','token','public')
session_lyft = auth_flow.get_session();
client_lyft = LyftRidesClient(session_lyft)

#authentication uber
session_uber = Session(server_token='token')
client_uber = UberRidesClient(session_uber)

#function for getting data on lyft
def extract_lyft(point_id_o, o_lat, o_lon, d_lat, d_lon, point_id_d):
    global count, pd_timestamp, rand_seq
    #getting number of nearby cars
    locations = []
    response = client_lyft.get_drivers(o_lat, o_lon)
    nearby_drivers = response.json.get('nearby_drivers')
    for i in range(0, len(nearby_drivers)):
        ride_type = nearby_drivers[i]['ride_type']
        drivers = nearby_drivers[i]['drivers']
        for j in range(0, len(drivers)):
            to_add_ride_type = drivers[j]['locations']
            for k in to_add_ride_type:
                k.update({'ride_type': ride_type})
                k.update({'distance': geopy.distance.distance((k['lat'],k['lng']), (o_lat, o_lon)).km})
            locations = locations + to_add_ride_type
    #number of cars and distances (min, mean, and max)
    drivers = pd.DataFrame(locations)
    list_ride_types = list(drivers['ride_type'].unique())
    output = pd.DataFrame({'ride_type': list_ride_types})
    output[['ride_type','dist_min']] = drivers[['ride_type','distance']].groupby(by = 'ride_type', as_index = False).min()
    output[['ride_type','dist_mean']] = drivers[['ride_type','distance']].groupby(by = 'ride_type', as_index = False).mean()
    output[['ride_type','dist_max']] = drivers[['ride_type','distance']].groupby(by = 'ride_type', as_index = False).max()
    output[['ride_type','noc']] = drivers[['ride_type','distance']].groupby(by = 'ride_type', as_index = False).count()
    #getting cost from trips
    response = client_lyft.get_cost_estimates(o_lat, o_lon, d_lat, d_lon)
    estimate = response.json.get('cost_estimates')
    estimate = pd.DataFrame(estimate)
    estimate['usd_high'] = estimate['estimated_cost_cents_max'] / 100
    estimate['usd_low'] = estimate['estimated_cost_cents_min'] / 100
    estimate['ptp'] = estimate['primetime_percentage'].str.rstrip('%').astype('float') / 100.0
    estimate = estimate.rename(columns = {'estimated_distance_miles':'miles',
                                          'estimated_duration_seconds':'duration'})
    output = pd.merge(output, estimate[['ride_type','miles','duration','usd_high','usd_low','ptp']], on = 'ride_type')
    #getting eta
    response = client_lyft.get_pickup_time_estimates(o_lat, o_lon)
    eta_estimates = response.json.get('eta_estimates')
    eta_estimates = pd.DataFrame(eta_estimates)
    output = pd.merge(output, eta_estimates[['ride_type','eta_seconds']], on = 'ride_type')
    #adding point id and time stamp
    output['point_id_o'] = point_id_o
    output['point_id_d'] = point_id_d
    output['time_stamp'] = pd_timestamp
    return output


def extract_uber(point_id_o, o_lat, o_lon, d_lat, d_lon, point_id_d):
    global count, pd_timestamp, rand_seq

    response = client_uber.get_price_estimates(
        start_latitude = o_lat,
        start_longitude = o_lon,
        end_latitude = d_lat,
        end_longitude = d_lon)
    estimate = response.json.get('prices')
    estimate = pd.DataFrame(estimate)
    estimate = estimate.rename(columns = {'distance':'miles',
                                          'high_estimate':'usd_high',
                                          'low_estimate':'usd_low'})
    output = estimate[['display_name','miles','duration','usd_high','usd_low','surge_multiplier']]
    #getting pick-up time estimates
    response = client_uber.get_pickup_time_estimates(o_lat, o_lon)
    times = response.json.get('times')
    times = pd.DataFrame(times)
    #data cleaning
    times['eta'] = times['estimate']/60
    output = pd.merge(output, times[['display_name','eta']], on = 'display_name')
    #adding point id and time stamp
    output['point_id_o'] = point_id_o
    output['point_id_d'] = point_id_d
    output['time_stamp'] = pd_timestamp
    return output



def run():
    global count, pd_timestamp, rand_seq
    pd_timestamp = pd.Timestamp.now()
    if count == nop:
        rand_seq = np.random.permutation(nop)
        count = 0
    conn = sqlite3.connect('seattle_uber_lyft_data.db')
    rand = random.randint(0, nop - 1)
    while rand == rand_seq[count]:
        rand = random.randint(0, nop - 1)
        
    try:
        output_lyft = extract_lyft(point_id[rand_seq[count]],
                                   lat[rand_seq[count]],
                                   lon[rand_seq[count]],
                                   lat[rand],
                                   lon[rand],
                                   point_id[rand])

        output_uber = extract_uber(point_id[rand_seq[count]],
                                   lat[rand_seq[count]],
                                   lon[rand_seq[count]],
                                   lat[rand],
                                   lon[rand],
                                   point_id[rand])

    except Exception:
        print('error')
    else:
        #return output_lyft, output_uber
        output_lyft.to_sql('lyft', conn, if_exists = 'append', index = False)
        output_uber.to_sql('uber', conn, if_exists = 'append', index = False)
        conn.close()
        count += 1
        print(count)

scheduler = BackgroundScheduler()
scheduler.add_job(run, 'cron', second="*/30")
scheduler.start()
