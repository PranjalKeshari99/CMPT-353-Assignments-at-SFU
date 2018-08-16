
import pandas as pd

totals = pd.read_csv('totals.csv').set_index(keys=['name'])
counts = pd.read_csv('counts.csv').set_index(keys=['name'])

# data for calculating average precipitation per cities 
city_precip_total = totals.sum(axis=1)
city_total_observ = counts.sum(axis=1)
# data for calculating average precipitation per month
month_precip_total = totals.sum(axis=0)
month_total_observ = counts.sum(axis=0)

average_month = month_precip_total / month_total_observ
average_city = city_precip_total / city_total_observ

# calculate city with lowest total precipitation
lowest_city = city_precip_total.idxmin(axis=1)


# print results
print ("City with lowest total precipitation:\n",lowest_city)

print("Average precipitation in each month:\n",average_month)

print("Average precipitation in each city:\n",average_city)
