
# coding: utf-8

# In[176]:

import pandas as pd
import numpy as np
import re
import datetime
import matplotlib.pyplot as plt
from scipy import stats


# In[135]:

tweet_data = pd.read_csv('dog_rates_tweets.csv', parse_dates=True)


# In[136]:

pattern_match = tweet_data.text.str.extract(r'(\d+(\.\d+)?)/10', expand=False)


# In[141]:

rating_data = pattern_match[pattern_match[0] < 'NaN'][0]


# In[145]:

rating_data = pd.to_numeric(rating_data)
rating_data = rating_data[rating_data <= 25.0]


# In[146]:

tweet_data['rating'] = rating_data


# In[149]:

final_data = tweet_data.loc[rating_data.index]


# In[156]:

final_data['created_at'] = pd.to_datetime(final_data['created_at'], format='%Y-%m-%d %H:%M:%S')
final_data


# In[185]:

def to_timestamp(d):
    return d.timestamp()
final_data['timestamp'] = final_data['created_at'].apply(to_timestamp)


# In[183]:

fit = stats.linregress(final_data['timestamp'], final_data['rating'])
fit.slope, fit.intercept


# In[184]:

plt.xticks(rotation=25)
plt.plot(final_data['created_at'].values, final_data['rating'], 'b.', alpha=0.5)
plt.plot(final_data['created_at'].values, final_data['timestamp']*fit.slope + fit.intercept, 'r-', linewidth=3)
plt.show()

