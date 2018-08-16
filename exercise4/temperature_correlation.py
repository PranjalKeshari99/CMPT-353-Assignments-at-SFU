
import numpy as np
import pandas as pd
import sys
import matplotlib.pyplot as plt
import gzip
import math as m

stations_file = sys.argv[1]
cities_file = sys.argv[2]
output_file = sys.argv[3]

station_fh = gzip.open(stations_file, 'rt', encoding='utf-8')
stations_df = pd.read_json(station_fh, lines=True)
cities_df = pd.read_csv(cities_file)

stations_df['avg_tmax'] = stations_df['avg_tmax'] / 10 #divide by ten weather data is °C×10 (because that's what GHCN distributes)
#the average daily-high temperature for the year

cities_df = cities_df[np.isfinite(cities_df.population)]
cities_df = cities_df[np.isfinite(cities_df.area)].reset_index(drop=True) #get ride of unusable data
cities_df['area'] = cities_df['area'] / 1000000 #convert from m^2 to km^2

cities_df = cities_df[cities_df.area <= 10000] #exclude unreasonable area

cities_df.reset_index(drop=True)

cities_df['density'] = cities_df['population'] / cities_df['area'] #calculate density

def distance(city, stations): #takes the current row of the cities df and the stations df as argument
    #returns a df of distances from the city to all the stations
    p = float(m.pi/180)
    city_lat = city['latitude']
    city_long = city['longitude']

    #row is the city columns are stations
    d = 0.5 - np.cos((stations['latitude']-city_lat)*p)/2 + np.cos(city_lat*p) * np.cos(stations['latitude']*p) * (1- np.cos((stations['longitude']-city_long)*p))/2
    
    return 12742*np.arcsin(np.sqrt(d))

def best_tmax(city, stations): 
    stations['distance'] = distance(city, stations)
    station = stations_df[stations_df['distance'] == stations_df['distance'].min()]
    
    station = station.reset_index(drop=True)
    
    return station.loc[0, 'avg_tmax']

cities_df['avg_tmax'] = cities_df.apply(best_tmax, axis=1, stations=stations_df)
cities_df

'''city = cities_df.loc[0]
p = float(m.pi/180)
city_lat = city['latitude']
city_long = city['longitude']
    
#row is the city columns are stations
d = 0.5 - np.cos((stations_df['latitude']-city_lat)*p)/2 + np.cos(city_lat*p) * np.cos(stations_df['latitude']*p) * (1- np.cos((stations_df['longitude']-city_long)*p))/2
stations_df['distance'] = 12742*np.arcsin(np.sqrt(d))
station = stations_df[stations_df['distance'] == stations_df['distance'].min()]
station = station.reset_index(drop=True)
station.loc[0, 'avg_tmax']'''

#code to test functionality



plt.scatter(cities_df['avg_tmax'], cities_df['population'])
#plt.show()

plt.savefig(output_file)
