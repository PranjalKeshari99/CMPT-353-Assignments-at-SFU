import pandas as pd
import sys 
import matplotlib.pyplot as plt

#Obtain the filenames from the command line input
filename1 = sys.argv[1]
filename2 = sys.argv[2]

#Plot 1: Distribution of Views
#Only use the first data set, should be a Pareto distribution

#Sort the data by the number of views (decreasing)
data1 = pd.read_table(filename1, sep=' ', header=None, index_col=1, names=['lang', 'page', 'views', 'bytes'])
data1_sorted = data1.sort_values(by='views', ascending=False)

#Plot 2: Daily Views
#A scatterplot of views from the firsts data file (x-coordinate) and the corresponding values from the second data file (y-coordinate)
data2 = pd.read_table(filename2, sep=' ', header=None, index_col=1, names=['lang', 'page', 'views', 'bytes'])
data1['views2'] = data2['views']

#Plot the single plot wth multiple subplots
plt.figure(figsize=(14, 7))
plt.subplot(1, 2, 1)
plt.plot(data1_sorted['views'].values)
plt.title('Popularity Distribution')
plt.ylabel('Views')
plt.xlabel('Rank')

plt.subplot(1, 2, 2)
plt.xscale('log')
plt.yscale('log')
plt.scatter(data1['views'], data1['views2'])
plt.title('Daily Correlation')
plt.ylabel('Day 2 Views')
plt.xlabel('Day 1 Views')

plt.savefig('wikipedia.png')