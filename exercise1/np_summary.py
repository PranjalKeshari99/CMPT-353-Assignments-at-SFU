# Created May 14, 2018 for CMPT 353 at Simon Fraser University

import numpy as np

data = np.load('monthdata.npz')
totals = data['totals']
counts = data['counts']

# number of rows
stations_num = totals.shape[0]

# data for calculating average precipitation per cities 
city_precip_total = np.sum(totals, axis=1)
city_total_observ = np.sum(counts, axis=1)
# data for calculating average precipitation per month
month_precip_total = np.sum(totals, axis=0)
month_total_observ = np.sum(counts, axis=0)

average_month = month_precip_total / month_total_observ
average_city = city_precip_total / city_total_observ

# data manipulation for calculating quarterly totals in each city
quarterly_reshape = totals.reshape((4*stations_num,3))
quarterly_totals = np.sum(quarterly_reshape,axis=1)
quarterly = quarterly_totals.reshape(stations_num,4)

# calculate city with lowest total precipitation
lowest_city = np.argmin(city_precip_total)


# print results
print ("Row with lowest total precipitation:\n",lowest_city)

print("Average precipitation in each month:\n",average_month)

print("Average precipitation in each city:\n",average_city)

print("Quarterly precipitation totals:\n",quarterly)