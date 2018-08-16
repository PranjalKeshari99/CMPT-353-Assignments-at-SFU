import pandas as pd
import numpy as np
import sys
import matplotlib.pyplot as plt
from statsmodels.nonparametric.smoothers_lowess import lowess
from datetime import timedelta, datetime
from pykalman import KalmanFilter

file = sys.argv[1]
cpu_data = pd.read_csv(file)


#Convert timestamp to a float instead of a datetime
cpu_data['datetime_timestamp'] = pd.to_datetime(cpu_data['timestamp'])
cpu_data['timedelta_timestamp'] = pd.to_timedelta(cpu_data['datetime_timestamp'])
cpu_data['plottable_timestamp'] = cpu_data['timedelta_timestamp'].apply(timedelta.total_seconds)

#LOESS Smoothing
LOESS_smoothed = lowess(cpu_data['temperature'], cpu_data['plottable_timestamp'] , frac=0.01)

#Plot the output of LOESS smoothing
plt.figure(figsize=(12, 4))
plt.plot(cpu_data['plottable_timestamp'], cpu_data['temperature'], 'b.', alpha=0.5)
plt.plot(cpu_data['plottable_timestamp'], LOESS_smoothed[:, 1], 'r-', alpha=0.5)


#Kalman Smoothing 
kalman_data = cpu_data[['temperature', 'cpu_percent', 'sys_load_1']]

initial_state = kalman_data.iloc[0]
observation_covariance = np.diag([0.9, 0.9, 0.9]) ** 2
transition_covariance = np.diag([0.1, 0.1, 0.1]) ** 2
transition = [[1, -1, 0.7], [0, 0.6, 0.03], [0, 1.3, 0.8]]

#do the necessary transformations with the created matrices
kf = KalmanFilter(initial_state_mean = initial_state, initial_state_covariance = observation_covariance, observation_covariance = observation_covariance, transition_covariance = transition_covariance, transition_matrices = transition)
kalman_smoothed, _ = kf.smooth(kalman_data)
plt.plot(cpu_data['plottable_timestamp'], kalman_smoothed[:, 0], 'g-', alpha=0.5)
plt.legend(['CPU Data', 'LOESS Smoothing', 'Kalman Smoothing'])


plt.show()
plt.savefig('cpu.svg')
